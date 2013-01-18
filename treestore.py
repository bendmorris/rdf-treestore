import Bio.Phylo as bp
import RDF


class Treestore:
    def __init__(self, dsn='Virtuoso', user='dba', password='dba'):
        '''Create a treestore object from a Virtuoso ODBC connection with given
        DSN, username and password.'''

        self.store = RDF.Storage(storage_name='virtuoso', name='db',
                                 options_string="dsn='%s',user='%s',password='%s'"
                                 % (dsn, user, password))


    def add_trees(self, tree_file, format, tree_name=None):
        '''Convert trees residing in a text file into RDF, and add them to the
        underlying RDF store with a context node for retrieval.

        Example:
        >>> treestore.add_trees('test.newick', 'newick', 'test')
        '''
        
        if tree_name is None: tree_name = tree.__name__

        bp.convert(tree_file, format, None, 'cdao', storage=self.store, base_uri=tree_name)


    def get_trees(self, tree_name):
        '''Retrieve trees that were previously added to the underlying RDF 
        store. Returns a generator of Biopython trees.

        Example:
        >>> trees = treestore.get_trees('test')
        >>> trees.next()
        Tree(weight=1.0, rooted=False)
        '''
        
        parser = bp.CDAOIO.Parser()
        return parser.parse_model(RDF.Model(self.store), tree_name)
        