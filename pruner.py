import RDF
import Redland_python
import Bio.Phylo as bp


def mrca(taxa, treestore, graph):
    assert len(taxa) > 0
    
    connection = treestore.odbc_connection
    cursor = connection.cursor()
    
    mrca = None
    
    for taxon in taxa:
        try: node_info = get_node_info(graph, cursor, taxon)
        except:
            taxa.remove(taxon)
            continue

        if not mrca:
            mrca_ancestors = []
            for (ancestor,) in node_info:
                mrca_ancestors.append(ancestor)
            mrca = mrca_ancestors[0]
            continue
            
        for (ancestor,) in node_info:
            if ancestor in mrca_ancestors:
                mrca = ancestor
                mrca_ancestors = mrca_ancestors[mrca_ancestors.index(ancestor):]
                break
        
    return mrca
    
    
def subtree(mrca, treestore, graph, prune=False):
    connection = treestore.odbc_connection
    cursor = connection.cursor()
    
    query = '''sparql
PREFIX obo: <http://purl.obolibrary.org/obo/>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>

SELECT DISTINCT ?n ?length ?parent ?label
WHERE {
    GRAPH <%s> {
        %s
        OPTIONAL { ?n obo:CDAO_0000187 [ rdf:label ?label ] . }
        OPTIONAL { ?n obo:CDAO_0000143 [ obo:CDAO_0000193 [ obo:CDAO_0000215 ?length ] ] . }
        OPTIONAL { ?n obo:CDAO_0000179 ?parent . }
    }
}''' % (graph, ('''?n obo:CDAO_0000179 <%s> 
option(transitive, t_min(0)) .''' % mrca) if mrca 
else 
'')
    cursor.execute(query)
    
    root = bp.CDAO.Clade()
    nodes = {}
    nodes[mrca] = root
    stmts = cursor
    
    for i in range(2):
        redo = []
        for stmt in stmts:
            node_id, edge_length, parent, label = stmt
            
            if not node_id in nodes:
                clade = bp.CDAO.Clade(name=label, branch_length=float(edge_length) if edge_length else None)
                nodes[node_id] = clade
            
            if parent and (parent in nodes):
                nodes[parent].clades.append(clade)
            else:
                root = nodes[node_id]
                redo.append(stmt)
                
        stmts = redo

    tree = bp.CDAO.Tree(root=root, rooted=True)

    if prune: return pruned_tree(tree, prune)

    return tree


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


def get_node_info(graph, cursor, taxon):
    query = '''sparql
PREFIX obo: <http://purl.obolibrary.org/obo/>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>

SELECT DISTINCT ?ancestor
WHERE {
    GRAPH <%s> {
        ?t obo:CDAO_0000187 [ rdf:label "%s" ] .
        OPTIONAL { ?t obo:CDAO_0000179 ?ancestor 
                   option(transitive, t_direction 1, t_step('step_no') as ?steps, 
                          t_min 0, t_max 10000) }
    }
}
ORDER BY ?steps
''' % (graph, taxon)
    cursor.execute(query)
    results = cursor
    
    return results
