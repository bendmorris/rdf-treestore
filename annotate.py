import RDF


def annotate(tree_uri, annotation_file, treestore, format='ntriples'):
    '''Annotate tree with annotations from RDF file.'''
    model = RDF.Model(treestore.store)
    file_model = RDF.Model()
    RDF.Parser(name = format).parse_into_model(file_model, 'file://%s' % os.path.abspath(annotation_file))
    for triple in file_model:
        model.add_statement(triple, RDF.Node(RDF.Uri(tree_uri)))
    model.sync()
