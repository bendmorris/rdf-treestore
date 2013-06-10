import Bio.Phylo as bp


def mrca(taxa, treestore, graph):
    assert len(taxa) > 0
    
    cursor = treestore.get_cursor()
    
    mrca = None
    
    for taxon in taxa:
        try: ancestors = get_ancestors(graph, cursor, taxon)
        except:
            taxa.remove(taxon)
            continue

        if not mrca:
            mrca_ancestors = []
            for (ancestor,) in ancestors:
                mrca_ancestors.append(ancestor)
            if not mrca_ancestors:
                taxa.remove(taxon)
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
    
    
def subtree(mrca, treestore, graph, prune=False):
    cursor = treestore.get_cursor()
    
    query = '''sparql
PREFIX obo: <http://purl.obolibrary.org/obo/>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

SELECT DISTINCT ?n ?length ?parent ?label
WHERE {
    GRAPH <''' + graph + '''> {
        {?n a obo:CDAO_0000026} UNION {?n a obo:CDAO_0000108}
        ''' + ((
'?n obo:CDAO0000179 <%s> . \noption(transitive, t_min(0)) .' % mrca
) if mrca else '') + '''
        OPTIONAL { ?n obo:CDAO_0000187 [ rdfs:label ?label ] . }
        OPTIONAL { ?n obo:CDAO_0000143 [ obo:CDAO_0000193 [ obo:CDAO_0000215 ?length ] ] . }
        OPTIONAL { ?n obo:CDAO_0000179 ?parent . }
    }
}'''
    cursor.execute(query)
    
    root = None
    nodes = {}
    stmts = cursor
    
    for i in range(2):
        redo = []
        for stmt in stmts:
            node_id, edge_length, parent, label = stmt
            
            if not node_id in nodes:
                clade = bp.CDAO.Clade(name=label, branch_length=float(edge_length) if edge_length else None)
                nodes[node_id] = clade
            
            if root is None and ((node_id == mrca) if mrca else (parent is None)):
                root = nodes[node_id]
            elif parent and (parent in nodes):
                nodes[parent].clades.append(clade)
            else:
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


def get_ancestors(graph, cursor, taxon):
    query = '''sparql
PREFIX obo: <http://purl.obolibrary.org/obo/>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

SELECT DISTINCT ?ancestor
WHERE {
    GRAPH <%s> {
        ?t obo:CDAO_0000187 [ rdfs:label "%s" ] .
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
