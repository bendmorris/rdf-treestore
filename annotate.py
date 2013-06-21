class Annotatable:
    def annotate(self, tree_uri, annotations=None, annotation_file=None):
        '''Annotate tree with annotations from RDF file.'''
        cursor = self.get_cursor()
        
        if annotation_file and not annotations:
            with open(annotation_file) as input_file:
                annotations = input_file.read()
        
        insert_stmt = '''sparql
PREFIX obo: <http://purl.obolibrary.org/obo/>

WITH <%s>
INSERT {
    %s
}
WHERE {
    ?tree obo:CDAO_0000148 [] .
}
        ''' % (tree_uri, annotations)
        print insert_stmt
        
        cursor.execute(insert_stmt)
