import RDF
import Redland_python
import Bio.Phylo as bp


def mrca(taxa, treestore, graph):
    assert len(taxa) > 0
    
    connection = treestore.odbc_connection
    cursor = connection.cursor()
    
    mrca = None
    
    for taxon in taxa:
        try: node_id = get_node_id(graph, cursor, taxon)
        except:
            taxa.remove(taxon)
            continue

        if not mrca:
            mrca = node_id
            continue

        if same_lineage(graph, cursor, node_id, mrca): continue
        if same_lineage(graph, cursor, mrca, node_id):
            mrca = node_id
            continue

        query = '''sparql
PREFIX obo: <http://purl.obolibrary.org/obo/>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>

SELECT ?mrca ?steps
WHERE {
    GRAPH <%s> {
        ?t1 obo:CDAO_0000179 ?mrca option(transitive, t_step('step_no') as ?steps) .
        ?t2 obo:CDAO_0000179 ?mrca2 option(transitive) .

        FILTER ( ?t1 = <%s> )
        FILTER ( ?t2 = <%s> )
        FILTER ( ?mrca = ?mrca2 )
    }
}
ORDER BY ?steps
''' % (graph, node_id, mrca)

        cursor.execute(query)
        results = cursor
        
        try:
            new_mrca = str(results.next()[0])
            if new_mrca: mrca = new_mrca
        except StopIteration:
            raise Exception('MRCA of (%s, %s) not found.' % (mrca, taxon))
    
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
option(transitive, t_min 0) .''' % mrca) if mrca 
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
        
        if not root and not (clade.name in contains):
            tree.collapse(clade)
            return True
        else:
            return False

    prune_clade(tree, tree.root, True)
    
    return tree



def same_lineage(graph, cursor, taxon, mrca):
    '''Returns True if the taxon is a descendent of the MRCA.'''

    if taxon == mrca: return True

    query = '''sparql
PREFIX obo: <http://purl.obolibrary.org/obo/>

ASK {
    GRAPH <%s> {
        <%s> obo:CDAO_0000179 <%s> option(transitive, t_min(0)) .
    }
}
''' % (graph, taxon, mrca)
    
    cursor.execute(query)
    try:
        if cursor.next()[0]: return True
    except StopIteration: pass

    return False


def get_node_id(graph, cursor, taxon):
    query = '''sparql
PREFIX obo: <http://purl.obolibrary.org/obo/>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>

SELECT ?t
WHERE {
    GRAPH <%s> {
        ?t obo:CDAO_0000187 [ rdf:label "%s" ] .
    }
}
GROUP BY ?mrca
ORDER BY ?steps
LIMIT 1
''' % (graph, taxon)
    cursor.execute(query)
    results = cursor
    
    try:
        return str(results.next()[0])
    except StopIteration:
        raise Exception('%s not found.' % (taxon))
