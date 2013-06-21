class Annotatable:
    def annotate(self, tree_uri, annotation_file):
        '''Annotate tree with annotations from RDF file.'''
        cursor = self.get_cursor()
        
        # TODO: rewrite without redland
