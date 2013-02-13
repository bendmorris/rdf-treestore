import RDF
import Redland_python
import Bio.Phylo as bp


def mrca(taxa, treestore, graph):
    assert len(taxa) > 1
    
    connection = treestore.odbc_connection
    cursor = connection.cursor()
    
    query = '''sparql
PREFIX obo: <http://purl.obolibrary.org/obo/>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>

SELECT ?mrca (count(?mrca_ancestor) as ?mrca_ancestors)
WHERE {
    GRAPH <%s> {

        ?t1 obo:CDAO_0000179 ?mrca option(transitive) ;
           obo:CDAO_0000187 [ rdf:label "%s" ] .
        ?t2 obo:CDAO_0000179 ?mrca option(transitive) ;
           obo:CDAO_0000187 [ rdf:label "%s" ] .
        OPTIONAL { ?mrca obo:CDAO_0000179 ?mrca_ancestor option(transitive) }
    }
}
GROUP BY ?mrca
ORDER BY desc(?mrca_ancestors)
''' % (graph, taxa[0], taxa[1],)
    
    cursor.execute(query)
    results = cursor
    
    try:
        mrca = str(results.next()[0])
    except StopIteration:
        raise Exception('MRCA of (%s, %s) not found.' % (taxa[0], taxa[1]))
    
    for taxon in taxa[2:]:
        query = '''sparql
PREFIX obo: <http://purl.obolibrary.org/obo/>

ASK {
    GRAPH <%s> {
        ?n obo:CDAO_0000187 [ rdf:label "%s" ] ;
           obo:CDAO_0000179 <%s> option(transitive) .
    }
}
''' % (graph, taxon, mrca)
        
        cursor.execute(query)
        try:
            if cursor.next()[0]: continue
        except StopIteration: pass
    
        query = '''sparql
PREFIX obo: <http://purl.obolibrary.org/obo/>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX owl: <http://www.w3.org/2002/07/owl#>

SELECT ?mrca (count(?mrca_ancestor) as ?mrca_ancestors)
WHERE {
    GRAPH <%s> {
        
        ?n obo:CDAO_0000187 [ rdf:label "%s" ] ;
           obo:CDAO_0000179 ?mrca option(transitive) .
        <%s> obo:CDAO_0000179 ?mrca option(transitive) .
        OPTIONAL { ?mrca obo:CDAO_0000179 ?mrca_ancestor option(transitive) }
    }
}
GROUP BY ?mrca
ORDER BY desc(?mrca_ancestors)
''' % (graph, taxon, mrca)
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
        OPTIONAL { ?n obo:CDAO_0000187 ?tu . ?tu rdf:label ?label . }
        ?n obo:CDAO_0000143 ?edge . 
        OPTIONAL { ?edge obo:CDAO_0000193 [ obo:CDAO_0000215 ?length ] . }
        OPTIONAL { ?n obo:CDAO_0000179 ?parent . }
    }
}''' % (graph, ('?n obo:CDAO_0000179 <%s> option(transitive) .' % mrca) if mrca else '')
    cursor.execute(query)
    
    root = bp.CDAO.Clade()
    nodes = {}
    nodes[mrca] = root
    stmts = cursor
    redo = True
    
    while redo:
        redo = []
        for stmt in stmts:
            node_id, edge_length, parent, label = stmt
            if parent in nodes:
                clade = bp.CDAO.Clade(name=label, branch_length=float(edge_length) if edge_length else None)
                nodes[node_id] = clade
                nodes[parent].clades.append(clade)
            else:
                redo.append(stmt)
        stmts = redo
        
    tree = bp.CDAO.Tree(root=root, rooted=True)
    
    if prune:
        contains = prune
    
        def prune_extra_clades(tree, clade, root=True):
            result = 0
            for child in clade.clades:
                result += prune_extra_clades(tree, child, False)
            if not root and len(clade) < 2 and not clade.name:
                try: 
                    tree.collapse(clade)
                    return 1
                except: return 0
            return result

        terms = [c.name for c in tree.get_terminals()]
        for term in terms:
            if not term in contains:
                tree.prune(term)
        while (prune_extra_clades(tree, tree.clade) or 
               any([tree.prune(term) for term in tree.get_terminals() if not term.name])):
            pass

    return tree
