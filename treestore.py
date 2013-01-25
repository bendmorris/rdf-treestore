import Bio.Phylo as bp
import RDF
import os
from cStringIO import StringIO
from __init__ import __version__


class Treestore:
    def __init__(self, storage_name='virtuoso', dsn='Virtuoso', 
                 user='dba', password='dba', options_string=None, storage=None):
        '''Create a treestore object from an ODBC connection with given DSN,
        username and password.'''

        if storage:
            self.store = storage
        else:
            self.store = RDF.Storage(storage_name=storage_name, name='db',
                                     options_string="dsn='%s',user='%s',password='%s'" 
                                     % (dsn, user, password)
                                     if not options_string else options_string
                                     )

    def add_trees(self, tree_file, format, tree_name=None):
        '''Convert trees residing in a text file into RDF, and add them to the
        underlying RDF store with a context node for retrieval.

        Example:
        >>> treestore.add_trees('test.newick', 'newick', 'test')
        '''
        
        if tree_name is None: tree_name = os.path.basename(tree_file)

        bp.convert(tree_file, format, None, 'cdao', storage=self.store, context=tree_name)


    def get_trees(self, tree_name):
        '''Retrieve trees that were previously added to the underlying RDF 
        store. Returns a generator of Biopython trees.

        Example:
        >>> trees = treestore.get_trees('test')
        >>> trees.next()
        Tree(weight=1.0, rooted=False)
        '''
        
        parser = bp.CDAOIO.Parser()
        return parser.parse_model(RDF.Model(self.store), context=tree_name)
        

    def serialize_trees(self, tree_name, format='newick'):
        '''Retrieve trees serialized to any format supported by Biopython.
        
        Current options include 'newick', 'nexus', 'phyloxml', 'nexml', and 'cdao'

        Example:
        >>> treestore.serialize_trees('test')
        '''

        s = StringIO()
        bp.write(self.get_trees(tree_name), s, format)

        return s.getvalue()


    def remove_trees(self, tree_name):
        context = RDF.Node(RDF.Uri(tree_name))
        model = RDF.Model(self.store)
        model.remove_statements_with_context(context=context)
        model.sync()


def main():
    import argparse

    formats = ' | '.join(bp._io.supported_formats)

    parser = argparse.ArgumentParser()
    parser.add_argument('-v', '--version', action='version', version=__version__)
    parser.add_argument('-s', '--store', help='name of Redland store (default=virtuoso)')
    parser.add_argument('-d', '--dsn', help='ODBC DSN (default=Virtuoso)')
    parser.add_argument('-u', '--user', help='ODBC user (default=dba)')
    parser.add_argument('-p', '--password', help='ODBC password (default=dba)')
    parser.add_argument('-o', '--option', help='options_string for Redland store; ignores dsn/user/password')

    subparsers = parser.add_subparsers(help='sub-command help', dest='command')

    add_parser = subparsers.add_parser('add', help='add trees to treestore')
    add_parser.add_argument('file', help='tree file')
    add_parser.add_argument('format', help='file format (%s)' % formats)
    add_parser.add_argument('name', help='tree name (default=file name)', nargs='?', default=None)

    get_parser = subparsers.add_parser('get', help='retrieve trees from treestore')
    get_parser.add_argument('name', help='tree name')
    get_parser.add_argument('format', help='serialization format (%s) (default=newick)' % formats, 
                            nargs='?', default='newick')

    rm_parser = subparsers.add_parser('rm', help='remove trees from treestore')
    rm_parser.add_argument('name', help='tree name')


    args = parser.parse_args()

    kwargs = {}
    if args.store: kwargs['storage_name'] = args.store
    if args.dsn: kwargs['dsn'] = args.dsn
    if args.user: kwargs['user'] = args.user
    if args.password: kwargs['password'] = args.password
    if args.option: kwargs['options_string'] = args.option
    treestore = Treestore(**kwargs)

    if args.command == 'add':
        treestore.add_trees(args.file, args.format, args.name)
    elif args.command == 'get':
        print treestore.serialize_trees(args.name, args.format),
    elif args.command == 'rm':
        treestore.remove_trees(args.name)


if __name__ == '__main__':
    main()
