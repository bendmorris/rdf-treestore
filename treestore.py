import Bio.Phylo as bp
import RDF
import Redland_python
import os
import sys
from cStringIO import StringIO


__version__ = '0.1.0'

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

        bp.convert(tree_file, format, None, 'cdao', storage=self.store, tree_name=tree_name, context=tree_name)


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


    def list_trees(self, contains=[], match_all=False):
        model = RDF.Model(self.store)

        query = '''
PREFIX obo: <http://purl.obolibrary.org/obo/>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>

SELECT DISTINCT ?graph (count(?match) as ?matches)
WHERE {
    GRAPH ?graph {
        [] obo:CDAO_0000148 [] .
        %s
    }
} 
GROUP BY ?graph
ORDER BY DESC(?matches)
''' % (
' UNION\n        '.join(['{ ?match rdf:label "%s" }' % contain for contain in contains]))
        
        #print query
        query = RDF.SPARQLQuery(query)
        
        def handler(*args): pass
        Redland_python.set_callback(handler)
        results = query.execute(model)
        Redland_python.reset_callback()

            
        for result in results:
            if (not match_all) or int(str(result['matches']))==len(contains):
                yield str(result['graph']) + (' (%s)' % result['matches'] 
                                              if (contains and not match_all) else '') 


    def get_names(self, tree_name=None, format='json'):
        model = RDF.Model(self.store)

        query = '''
PREFIX obo: <http://purl.obolibrary.org/obo/>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>

SELECT DISTINCT ?uri, ?label
WHERE {
    GRAPH %s {
        [] obo:CDAO_0000148 [] .
        ?uri rdf:label ?label .
    }
}
ORDER BY ?label
''' % (('<%s>' % tree_name) if tree_name else '?graph')
        
        #print query
        query = RDF.SPARQLQuery(query)
        def handler(*args): pass
        Redland_python.set_callback(handler)
        results = query.execute(model)
        Redland_python.reset_callback()
        
        if format == 'json':
            return '[%s]' % ','.join([repr({'name': str(result['label']), 
                                            'uri': str(result['uri'])}) for result in results])
        elif format =='csv':
            return ','.join([str(result['label']) for result in results])
        else: 
            return results

    def get_subtree(self, contains=[], match_all=False):
        trees = self.list_trees(contains=contains, match_all=match_all)
        tree = trees.next()
        if not tree: raise Exception("An appropriate tree for this query couldn't be found.")
        
        


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

    ls_parser = subparsers.add_parser('ls', help='list all trees in treestore')
    ls_parser.add_argument('contains', 
        help='comma-delimited list of species that must be contained in each returned tree (default=none)',
        nargs='?', default='')
    ls_parser.add_argument('--all', help='only return trees that contain all given species', 
                           action='store_true')

    names_parser = subparsers.add_parser('names', 
                                         help='return a comma-separated list of all taxa names')
    names_parser.add_argument('tree', help='name of tree (default=all trees)', 
                              nargs='?', default=None)
    names_parser.add_argument('-f', '--format', help='file format (json, csv, xml) (default=csv)', 
                              default='csv')


    prune_parser = subparsers.add_parser('prune', 
                                         help='retrieve the best subtree containing a given set of taxa')
    prune_parser.add_argument('contains', 
        help='comma-delimited list of species that must be contained in each returned tree',
        nargs='?')
    prune_parser.add_argument('format', help='serialization format (%s) (default=newick)' % formats, 
                              nargs='?', default='newick')
    prune_parser.add_argument('--all', help='only return trees that contain all given species', 
                              action='store_true')

    args = parser.parse_args()

    kwargs = {}
    if args.store: kwargs['storage_name'] = args.store
    if args.dsn: kwargs['dsn'] = args.dsn
    if args.user: kwargs['user'] = args.user
    if args.password: kwargs['password'] = args.password
    if args.option: kwargs['options_string'] = args.option
    treestore = Treestore(**kwargs)

    if args.command == 'add':
        # parse a tree and add it to the treestore
        treestore.add_trees(args.file, args.format, args.name)
        
    elif args.command == 'get':
        # get a tree, serialize in specified format, and output to stdout
        print treestore.serialize_trees(args.name, args.format),
        
    elif args.command == 'rm':
        # remove a certain tree from the treestore
        treestore.remove_trees(args.name)
        
    elif args.command == 'ls':
        # list all trees in the treestore
        contains = args.contains
        if contains: contains = set([s.strip() for s in contains.split(',')])
        trees = [r for r in treestore.list_trees(contains=contains, match_all=args.all)]
        if not args.contains: trees = sorted(trees)
        if not trees: exit()
        
        if sys.stdout.isatty():
            # if output to terminal, use column output
            
            #width, height = console.getTerminalSize()
            from term_size import get_terminal_size
            cols,lines = get_terminal_size()
            max_width = cols
            
            def tree_columns(trees, cols):
                columns = []
                col_size = len(trees) / cols
                extra = len(trees) % cols
                n = 0
                for i in range(cols):
                    s = col_size
                    if i+1 <= extra: s += 1         
                    this_column = trees[n:n+s]
                    columns.append(this_column)
                    n += s
                return columns
                
            for cols in [int(len(trees) / float(i) + 0.5) for i in range(1, len(trees) + 1)]:
                columns = tree_columns(trees, cols)
                widths = [max([len(c) for c in column])+2 for column in columns]
                if sum(widths) < max_width:
                    break
                
            for pos in range(len(columns[0])):
                for column, width in zip(columns, widths):
                    if len(column) > pos:
                        print column[pos].ljust(width-1),
                print
                
        else:
            # otherwise, just output each tree, one per line
            for tree in trees: print tree


    elif args.command == 'names':
        print treestore.get_names(tree_name=args.tree, format=args.format)


    elif args.command == 'prune':
        contains = set([s.strip() for s in args.contains.split(',')])
        print treestore.get_subtree(contains=contains, match_all=args.all)


if __name__ == '__main__':
    main()
