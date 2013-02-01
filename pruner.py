import RDF
import Redland_python


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

        [] obo:CDAO_0000144 ?mrca ;
           obo:CDAO_0000187 [ rdf:label "%s" ] .
        [] obo:CDAO_0000144 ?mrca ;
           obo:CDAO_0000187 [ rdf:label "%s" ] .
        OPTIONAL { ?mrca obo:CDAO_0000144 ?mrca_ancestor }
    }
}
GROUP BY ?mrca
ORDER BY desc(?mrca_ancestors)
''' % (graph, taxa[0], taxa[1],)
    
    cursor.execute(query)
    results = cursor
    
    mrca = str(results.next()[0])
    
    for taxon in taxa[2:]:
        query = '''sparql
PREFIX obo: <http://purl.obolibrary.org/obo/>

ASK {
    GRAPH <%s> {
        ?n obo:CDAO_0000187 [ rdf:label "%s" ] ;
           obo:CDAO_0000144 <%s> .
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
           obo:CDAO_0000144 ?mrca .
        <%s> obo:CDAO_0000144 ?mrca .
        OPTIONAL { ?mrca obo:CDAO_0000144 ?mrca_ancestor }
    }
}
GROUP BY ?mrca
ORDER BY desc(?mrca_ancestors)
''' % (graph, taxon, mrca)
        cursor.execute(query)
        results = cursor
        
        new_mrca = str(results.next()[0])
        if new_mrca: mrca = new_mrca
    
    return mrca
    
    
def subtree(mrca, treestore, graph):
    connection = treestore.odbc_connection
    cursor = connection.cursor()
    
    query = '''sparql
PREFIX obo: <http://purl.obolibrary.org/obo/>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>

SELECT ?n ?length ?parent ?label
WHERE {
    GRAPH <bird2000> {
        ?n obo:CDAO_0000144 <%s> .
        OPTIONAL { ?n obo:CDAO_0000187 ?tu . ?tu rdf:label ?label . }
        OPTIONAL { ?n obo:CDAO_0000143 ?edge . OPTIONAL { ?edge obo:CDAO_0000193 [ obo:CDAO_0000215 ?length ] . } }
        OPTIONAL { ?n obo:CDAO_0000179 ?parent . }
    }
}''' % mrca

    cursor.execute(query)
    
    return cursor
