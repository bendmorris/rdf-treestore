import Bio.Phylo as bp
import sys


def find_mrca(taxa, treestore, graph, taxonomy=None):
    assert len(taxa) > 0
    
    cursor = treestore.get_cursor()
    
    mrca = None
    
    for n, taxon in enumerate(taxa[:]):
        try:
            result = find_name(graph, cursor, taxon, taxonomy)
            if len(result) == 2: node_id, taxon, synonym = result + (None,)
            else: node_id, taxon, synonym = result
            ancestors = get_ancestors(graph, cursor, node_id)
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
    
    
def subtree(taxa, treestore, graph, taxonomy=None, prune=False):
    '''Get a subtree containing a given set of taxa.'''

    if taxa:
        old_taxa = taxa[:]
        mrca = find_mrca(taxa, treestore, graph, taxonomy)
        
        # these taxa were changed by the MRCA query; they're either None (couldn't
        # be found) or the name of a synonym
        replace = {new:old for (new, old) in zip(taxa, old_taxa) if old != new}
    else:
        mrca, replace = None, None
    
    cursor = treestore.get_cursor()
    
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
            
            if not node_id in nodes:
                clade = bp.CDAO.Clade(name=label, branch_length=float(edge_length) if edge_length else 1)
                nodes[node_id] = clade
            
            if root is None and ((node_id == mrca) if mrca else (parent is None)):
                root = nodes[node_id]
            elif parent and (parent in nodes):
                nodes[parent].clades.append(clade)
            else:
                redo.append(stmt)
                
        stmts = redo
    
    tree = bp.CDAO.Tree(root=root, rooted=True)
    
    if prune: result = pruned_tree(tree, taxa)
    else: result = tree
    
    # replace synonymous names from the phylogeny with names from the query
    if replace:
        for x in tree.find_elements():
            if x.name in replace:
                old_name = x.name
                x.name = replace[x.name]
                del replace[old_name]
                if not replace: break
    
    return result


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


def get_ancestors(graph, cursor, node_id):
    '''Query to get all ancestors of a node, starting with the most recent.'''
    
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
    
    
def find_name(graph, cursor, taxon, taxonomy=None):
    '''If taxon is the name of a node in this graph, return it; otherwise,
    return a synonym from `taxonomy` that matches a name in this graph.'''
    
    query = '''sparql
PREFIX obo: <http://purl.obolibrary.org/obo/>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

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
        ?x obo:CDAO_0000187 [ rdfs:label ?synonym ; rdfs:label ?label ]
        FILTER (?label = "%s")
    }
}''' % (graph, taxonomy, taxon)
    query += '\n}'
    #print query
    cursor.execute(query)
    results = cursor
    
    return results.next()
