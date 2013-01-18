import Bio.Phylo as bp
import RDF
import os
from cStringIO import StringIO


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
        
        if tree_name is None: tree_name = os.path.basename(tree_file)

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
        

    def serialize_trees(self, tree_name, format='newick'):
        '''Retrieve trees serialized to any format supported by Biopython.
        
        Current options include 'newick', 'nexus', 'phyloxml', 'nexml', and 'cdao'

        Example:
        >>> treestore.serialize_trees('test')
        '''

        s = StringIO()
        bp.write(self.get_trees(tree_name), s, format)

        return s.getvalue()


if __name__ == '__main__':
    import argparse

    formats = ' | '.join(bp._io.supported_formats)

    parser = argparse.ArgumentParser()
    parser.add_argument('--dsn', help='DSN for Virtuoso')
    parser.add_argument('-u', '--user', help='user for Virtuoso')
    parser.add_argument('-p', '--password', help='password for Virtuoso')

    subparsers = parser.add_subparsers(help='sub-command help', dest='command')

    add_parser = subparsers.add_parser('add', help='add trees to Virtuoso')
    add_parser.add_argument('file', help='tree file')
    add_parser.add_argument('format', help='file format (%s)' % formats)
    add_parser.add_argument('name', help='tree name (defaults to file name)', nargs='?', default=None)

    get_parser = subparsers.add_parser('get', help='retrieve trees from Virtuoso')
    get_parser.add_argument('name', help='tree name')
    get_parser.add_argument('format', help='serialization format (%s) (defaults to newick)' % formats, 
nargs='?', default='newick')


    args = parser.parse_args()

    kwargs = {}
    if args.dsn: kwargs['dsn'] = args.dsn
    if args.user: kwargs['user'] = args.user
    if args.password: kwargs['password'] = args.password
    treestore = Treestore(**kwargs)

    if args.command == 'add':
        if args.file is None: raise Exception('No tree file specified.')
        treestore.add_trees(args.file, args.format, args.name)
    elif args.command == 'get':
        print treestore.serialize_trees(args.name, args.format)
