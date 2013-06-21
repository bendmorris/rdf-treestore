import urllib2
import re


class Annotatable:
    def annotate(self, tree_uri, annotations=None, annotation_file=None, doi=None):
        '''Annotate tree with annotations from RDF file.'''
        cursor = self.get_cursor()
        
        if annotations:
            pass
        elif annotation_file:
            with open(annotation_file) as input_file:
                annotations = input_file.read()
        elif doi:
            # TODO: lookup citation info from DOI
            annotations = doi_lookup(doi)
        else:
            raise Exception('No annotation source (string, file, or DOI)  was specified.')
            
        
        insert_stmt = self.build_query('''
WITH <%s>
INSERT {
    %s
}
WHERE {
    ?tree obo:CDAO_0000148 [] .
}
        ''' % (tree_uri, annotations))
        print insert_stmt
        
        cursor.execute(insert_stmt)


def doi_lookup(doi):
    if not doi.startswith('http://dx.doi.org/'):
        doi = 'http://dx.doi.org/' + doi
    r = urllib2.Request(doi, headers={'Accept': 'text/turtle'})
    data = urllib2.urlopen(r).read()
    
    # strip out prefixes, which will already be present in the insert statement
    data = re.sub('@prefix [^\:]*\: \<[^\>]*\> .', '', data)
    
    return '?tree bibo:cites <%s> .\n\n%s' % (doi, data)
