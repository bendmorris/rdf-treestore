import Bio.Phylo as bp
import sys


class Prunable:
    def get_subtree(self, contains=[], contains_ids=[], tree_uri=None,
                    format='newick', prune=True, filter=None, taxonomy=None):
        # TODO: filter is not being used. Use cql.py to parse the query, then convert the
        # requirements into SPARQL.
        
        if not contains or contains_ids: raise Exception('A list of taxa or ids is required.')
        if tree_uri:
            tree_uri = self.uri_from_id(tree_uri)
        else:
            trees = self.list_trees_containing_taxa(contains=contains,
                                                    show_counts=False,
                                                    filter=filter)
        
            try:
                tree_uri = trees.next()
            except StopIteration:
                raise Exception("An appropriate tree for this query couldn't be found.")
        
        tree = self.subtree(list(contains), tree_uri, 
                            taxonomy=taxonomy, prune=prune)
        
        return self.serialize_trees(trees=[tree], format=format)
        
        
    def find_mrca(self, taxa, graph, taxonomy=None):
        assert len(taxa) > 0
        
        cursor = self.get_cursor()
        
        mrca = None
        
        for n, taxon in enumerate(taxa[:]):
            try:
                result = self.find_name(graph, taxon, taxonomy)
                if len(result) == 2: node_id, taxon, synonym = result + (None,)
                else: node_id, taxon, synonym = result
                ancestors = self.get_ancestors(graph, node_id)
                if synonym: taxa[n] = synonym
            except:
                taxa[n] = None
                continue

            if not mrca:
                mrca_ancestors = []
                for (ancestor,) in ancestors:
                    mrca_ancestors.append(ancestor)
                if not mrca_ancestors:
                    taxa[n] = None
                    continue
                mrca = mrca_ancestors[0]
                continue
                
            for (ancestor,) in ancestors:
                if ancestor in mrca_ancestors:
                    mrca = ancestor
                    mrca_ancestors = mrca_ancestors[mrca_ancestors.index(ancestor):]
                    break
        
        if not mrca: raise Exception('None of these taxa are members of this tree.')
        return mrca
        
        
    def subtree(self, taxa, graph, taxonomy=None, prune=False):
        '''Get a subtree containing a given set of taxa.'''
        
        if taxa:
            old_taxa = taxa[:]
            mrca = self.find_mrca(taxa, graph, taxonomy)
            
            # these taxa were changed by the MRCA query; they're either None (couldn't
            # be found) or the name of a synonym
            replace = {new:old for (new, old) in zip(taxa, old_taxa) 
                       if new and old != new}
        else:
            mrca, replace = None, None
        
        cursor = self.get_cursor()
        
        query = '''sparql
    PREFIX obo: <http://purl.obolibrary.org/obo/>
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

    SELECT DISTINCT ?n ?length ?parent ?label
    WHERE {
        GRAPH <''' + graph + '''> {
            ?n obo:CDAO_0000200 ?tree .
            ?n a ?type .
            ''' + ((
    "?n obo:CDAO_0000179 <%s> option(transitive, t_min(0), t_step('step_no') as ?steps) ." % mrca
    ) if mrca else '') + '''
            OPTIONAL { ?n obo:CDAO_0000187 [ rdfs:label ?label ] . }
            OPTIONAL { ?n obo:CDAO_0000143 [ obo:CDAO_0000193 [ obo:CDAO_0000215 ?length ] ] . }
            OPTIONAL { ?n obo:CDAO_0000179 ?parent . }
            FILTER (?type = obo:CDAO_0000108 || ?type = obo:CDAO_0000026)
        }
    }''' +  ('ORDER BY ?steps ?n' if mrca else 'ORDER BY ?n')
        #print query
        cursor.execute(query)
        root = None
        nodes = {}
        stmts = cursor
        
        for _ in range(2):
            redo = []
            for stmt in stmts:
                node_id, edge_length, parent, label = stmt
                
                # replace synonymous names from the phylogeny with names from the query
                if replace and label in replace: label = replace[label]
                
                # create a clade for each node, and store in a dictionary by URI
                if not node_id in nodes:
                    clade = bp.CDAO.Clade(name=label, branch_length=float(edge_length) if edge_length else 1)
                    nodes[node_id] = clade
                
                # this is the root node if it has no parent or if it's the MRCA
                if root is None and ((node_id == mrca) if mrca else (parent is None)):
                    root = nodes[node_id]
                elif parent and (parent in nodes):
                    nodes[parent].clades.append(clade)
                else:
                    redo.append(stmt)
                    
            stmts = redo
        
        tree = bp.CDAO.Tree(root=root, rooted=True)
        
        if prune: result = pruned_tree(tree, old_taxa)
        else: result = tree
        
        return result
    
    
    def get_ancestors(self, graph, node_id):
        '''Query to get all ancestors of a node, starting with the most recent.'''
        
        cursor = self.get_cursor()
        
        query = '''sparql
    PREFIX obo: <http://purl.obolibrary.org/obo/>
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

    SELECT DISTINCT ?ancestor
    WHERE {
        GRAPH <%s> { 
            <%s> obo:CDAO_0000179 ?ancestor 
            option(transitive, t_direction 1, t_step('step_no') as ?steps, 
                   t_min 0, t_max 10000)
        }
    }
    ORDER BY ?steps
    ''' % (graph, node_id)
        #print query
        cursor.execute(query)
        results = cursor
        
        return results
        
        
    def find_name(self, graph, taxon, taxonomy=None):
        '''If taxon is the name of a node in this graph, return it; otherwise,
        return a synonym from `taxonomy` that matches a name in this graph.'''
        
        cursor = self.get_cursor()
        
        if taxon.split()[-1] == 'sp.':
            # when species is unidentified, fall back to searching for the genus
            taxon = ' '.join(taxon.split()[:-1])
        
        query = '''sparql
    PREFIX obo: <http://purl.obolibrary.org/obo/>
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
    PREFIX skos: <http://www.w3.org/2004/02/skos/core#>

    SELECT ?t ?label ''' + ('?synonym' if taxonomy else '') + '''
    WHERE { 
    {
        GRAPH <%s> { 
            ?t obo:CDAO_0000187 [ rdfs:label ?label ] 
            FILTER (?label = "%s") 
        }
    }''' % (graph, taxon)
        if taxonomy: query += ''' UNION {
        GRAPH <%s> { ?t obo:CDAO_0000187 [ rdfs:label ?synonym ] }
        GRAPH <%s> { 
            ?x obo:CDAO_0000187 [ ?l1 ?synonym ; ?l2 ?label ]
            FILTER (?label = "%s" && 
                    ?l1 in (rdfs:label, skos:altLabel) &&
                    ?l2 in (rdfs:label, skos:altLabel))
        }
    }''' % (graph, taxonomy, taxon)
        query += '\n}'
        #print query
        cursor.execute(query)
        results = cursor
        
        return results.next()
    
    
    
def pruned_tree(tree, contains):
    def prune_clade(tree, clade, root=False):
        keep_pruning = True
        while keep_pruning:
            keep_pruning = False
            for child in clade.clades:
                if prune_clade(tree, child):
                    keep_pruning = True
                    break
        
        if not root and not (clade.name in contains) and len(clade.clades) <= 1:
            tree.collapse(clade)
            return True
        else:
            return False
    
    prune_clade(tree, tree.root, True)
    
    return tree
