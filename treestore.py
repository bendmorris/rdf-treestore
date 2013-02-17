import Bio.Phylo as bp
import RDF
import os
import re
import sha
import sys
import pyodbc
import pruner
import annotate
from cStringIO import StringIO


__version__ = '0.1.2'
treestore_dir = '/home/ben/Dev/treestore/'

class Treestore:
    def __init__(self, storage_name='virtuoso', dsn='Virtuoso', 
                 user='dba', password='dba', storage=None):
        '''Create a treestore object from an ODBC connection with given DSN,
        username and password.'''

        try:
            if storage:
                self.store = storage
            else:
                self.store = RDF.Storage(storage_name=storage_name, name='db',
                                         options_string="dsn='%s',user='%s',password='%s'" 
                                         % (dsn, user, password)
                                         )
        except:
            # if Redland's Virtuoso storage fails, don't create this store now
            pass

        try:
            self.odbc_connection = pyodbc.connect('DSN=%s;UID=%s;PWD=%s' % (dsn, user, password),
                                                  autocommit=True)
        except NameError:
            # Handled later.
            pass


    def add_trees(self, tree_file, format, tree_uri=None, bulk_loader=None, puid=False, rooted=False):
        '''Convert trees residing in a text file into RDF, and add them to the
        underlying RDF store with a context node for retrieval.

        Example:
        >>> treestore.add_trees('test.newick', 'newick', 'http://www.example.org/test/')
        '''
        
        if tree_uri is None: tree_uri = os.path.basename(tree_file)

        if puid:
            # Create a pseudo-unique URI for trees, if the tree name is not a URI already:
            if not re.match(r'\w+://', tree_uri):
                puid = sha.new(open(tree_file).read()).hexdigest()
                tree_uri = 'http://phylotastic.org/hack2/%s/%s' % (puid, tree_uri)

        if bulk_loader:
            if self.odbc_connection == None:
                print 'Woops. \'pyodbc\' is not available on this platform.'
                return

            bp.convert(tree_file, format, os.path.join(treestore_dir, 'temp.cdao'), 'cdao', 
                       tree_uri=tree_uri, rooted=rooted)
        
            cursor = self.odbc_connection.cursor()
        
            update_stmt = 'sparql load <file://%s> into <%s>' % (
                os.path.abspath(os.path.join(treestore_dir, 'temp.cdao')), tree_uri)
        
            load_stmt = "ld_dir ('%s', 'temp.cdao', '%s')" % (
                os.path.abspath(treestore_dir), tree_uri)
            print load_stmt
            cursor.execute(load_stmt)
        
            update_stmt = "rdf_loader_run()"
            print update_stmt
            cursor.execute(update_stmt)

            cursor.execute('DELETE FROM DB.DBA.load_list')
        
        else:
            bp.convert(tree_file, format, RDF.Model(self.store), 'cdao', 
                       tree_uri=tree_uri, context=tree_uri, rooted=rooted)
        
        
    def get_trees(self, tree_uri):
        '''Retrieve trees that were previously added to the underlying RDF 
        store. Returns a generator of Biopython trees.

        Example:
        >>> trees = treestore.get_trees('http://www.example.org/test/')
        >>> trees.next()
        Tree(weight=1.0, rooted=False)
        '''
        
        #return pruner.subtree(None, self, tree_uri).next()
        parser = bp.CDAOIO.Parser()
        return parser.parse_model(RDF.Model(self.store), context=tree_uri)
        

    def serialize_trees(self, tree_uri='', format='newick', trees=None):
        '''Retrieve trees serialized to any format supported by Biopython.
        
        Current options include 'newick', 'nexus', 'phyloxml', 'nexml', and 'cdao'

        Example:
        >>> treestore.serialize_trees('http://www.example.org/test/')
        '''

        if trees is None: 
            trees = [i for i in self.get_trees(tree_uri)]
        if not trees:
            raise Exception('Tree to be serialized not found.')

        s = StringIO()
        if format == 'cdao':
            bp.write(trees, s, format, tree_uri=tree_uri)
        elif format == 'ascii':
            bp._utils.draw_ascii((i for i in trees).next())
        else:
            bp.write(trees, s, format)

        return s.getvalue()


    def remove_trees(self, tree_uri):
        '''Remove trees from treestore. Be careful with this; it really just
        removes a named graph, so if Virtuoso contains named graphs other than
        trees, those can be deleted too.

        Example:
        >>> treestore.remove_trees('http://www.example.org/test/')
        '''

        cursor = self.odbc_connection.cursor()
        cursor.execute('sparql clear graph <%s>' % tree_uri)


    def list_trees(self, contains=[], match_all=False, show_match_counts=False):
        '''List all trees in the treestore. Use `contains` to find trees that
        contain one or more specified taxa; `match_all` to find only trees that
        contain all of the listed taxa.

        Example:
        >>> treestore.remove_trees('http://www.example.org/test/')
        '''
        # TODO: use the smaller tree in the event of a match tie for efficiency?

        query = '''sparql
PREFIX obo: <http://purl.obolibrary.org/obo/>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>

SELECT ?graph (count(?match) as ?matches)
WHERE {
    GRAPH ?graph {
        [] obo:CDAO_0000148 [] .
        %s
    }
} 
GROUP BY ?graph
ORDER BY DESC(?matches)
''' % (' UNION\n        '.join(['{ ?match rdf:label "%s" }' % contain for contain in contains]))
        
        #print query
        cursor = self.odbc_connection.cursor()
        cursor.execute(query)
        results = cursor
        
        for result in results:
            if (not match_all) or int(str(result[1]))==len(contains):
                yield str(result[0]) + (' (%s)' % result[1] 
                                         if (contains 
                                         and not match_all 
                                         and show_match_counts) 
                                         else '') 


    def get_names(self, tree_uri=None, format=None):
        query = '''sparql
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
''' % (('<%s>' % tree_uri) if tree_uri else '?graph')
        
        #print query
        cursor = self.odbc_connection.cursor()
        cursor.execute(query)

        results = cursor
        
        if format == 'json':
            metadata = {
                "version":"",
                "treestoreMetadata":{
                    "treestoreShortName":"rdf_treestore",
                    "treestoreLongName":"Phylotastic RDF Treestore",
                    "domain":"",
                    "urlPrefix":"",
                }
            }

            json_dict = {'metadata': {}, 'externalSources': {},
                         'names': [{
                                    'name': str(result[1]),
                                    'treestoreId': str(result[0]),
                                    'sourceIds': {},
                                    }
                                   for result in results
                                   ]
                        }
            return repr(json_dict)
        elif format == 'csv':
            return ','.join(sorted(list(set([str(result[1]) for result in results]))))
        else: 
            return results


    def get_subtree(self, contains=[], contains_ids=[], match_all=False, format='newick', prune=True):
        if not contains or contains_ids: raise Exception('A list of taxa or ids is required.')
        trees = self.list_trees(contains=contains, match_all=match_all)
        try:
            tree_uri = trees.next()
        except StopIteration:
            raise Exception("An appropriate tree for this query couldn't be found.")
        
        mrca = pruner.mrca(list(contains), self, tree_uri)
        tree = pruner.subtree(mrca, self, tree_uri, prune=contains if prune else False)

        return self.serialize_trees(trees=[tree], format=format)



def main():
    import argparse

    bp_formats = ' | '.join(bp._io.supported_formats)
    input_formats = bp_formats
    output_formats = '%s | ascii' % bp_formats

    parser = argparse.ArgumentParser()
    parser.add_argument('-v', '--version', action='version', version=__version__)
    parser.add_argument('-s', '--store', help='name of Redland store (default=virtuoso)')
    parser.add_argument('-d', '--dsn', help='ODBC DSN (default=Virtuoso)')
    parser.add_argument('-u', '--user', help='ODBC user (default=dba)')
    parser.add_argument('-p', '--password', help='ODBC password (default=dba)')

    subparsers = parser.add_subparsers(help='sub-command help', dest='command')

    add_parser = subparsers.add_parser('add', help='add trees to treestore')
    add_parser.add_argument('file', help='tree file')
    add_parser.add_argument('format', help='file format (%s)' % input_formats)
    add_parser.add_argument('uri', help='tree uri (default=file name)', nargs='?', default=None)
    add_parser.add_argument('--bulk', help='use the virtuoso bulk loader', action='store_true')
    add_parser.add_argument('--puid', help='create a pseudo-unique ID for the tree', action='store_true')
    add_parser.add_argument('--rooted', help='this is a rooted tree', action='store_true')

    get_parser = subparsers.add_parser('get', help='retrieve trees from treestore')
    get_parser.add_argument('uri', help='tree uri')
    get_parser.add_argument('format', help='serialization format (%s) (default=newick)' % output_formats, 
                            nargs='?', default='newick')

    rm_parser = subparsers.add_parser('rm', help='remove trees from treestore')
    rm_parser.add_argument('uri', help='tree uri')

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

    count_parser = subparsers.add_parser('count', 
                                         help='returns the number of labelled nodes in a tree')
    count_parser.add_argument('tree', help='name of tree (default=all trees)', 
                              nargs='?', default=None)

    query_parser = subparsers.add_parser('query', 
                                         help='retrieve the best subtree containing a given set of taxa')
    query_parser.add_argument('contains', 
        help='comma-delimited list of species that must be contained in each returned tree',
        nargs='?')
    query_parser.add_argument('format', help='serialization format (%s) (default=newick)' % output_formats, 
                              nargs='?', default='newick')
    query_parser.add_argument('--all', help='only return trees that contain all given species', 
                              action='store_true')
    query_parser.add_argument('--complete', help="return complete subtree from MRCA; don't prune other taxa from the resulting tree",
                              action='store_true')

    ann_parser = subparsers.add_parser('annotate', help='annotate tree with triples from RDF file')
    ann_parser.add_argument('file', help='annotation file')
    ann_parser.add_argument('format', help='annotation file format (default=ntriples)')
    ann_parser.add_argument('uri', help='tree uri', default=None)

    args = parser.parse_args()

    kwargs = {}
    if args.store: kwargs['storage_name'] = args.store
    if args.dsn: kwargs['dsn'] = args.dsn
    if args.user: kwargs['user'] = args.user
    if args.password: kwargs['password'] = args.password
    treestore = Treestore(**kwargs)

    if args.command == 'add':
        # parse a tree and add it to the treestore
        treestore.add_trees(args.file, args.format, args.uri, bulk_loader=args.bulk, puid=args.puid,
                            rooted=args.rooted)
        
    elif args.command == 'get':
        # get a tree, serialize in specified format, and output to stdout
        print treestore.serialize_trees(args.uri, args.format),
        
    elif args.command == 'rm':
        # remove a certain tree from the treestore
        treestore.remove_trees(args.uri)
        
    elif args.command == 'ls':
        # list all trees in the treestore
        contains = args.contains
        if contains: contains = set([s.strip() for s in contains.split(',')])
        trees = [r for r in treestore.list_trees(contains=contains, match_all=args.all, 
                                                 show_match_counts=True)]
        if not contains: trees = sorted(trees)
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
        print treestore.get_names(tree_uri=args.tree, format=args.format)

    elif args.command == 'count':
        print len([r for r in treestore.get_names(tree_uri=args.tree, format=None)])

    elif args.command == 'query':
        contains = set([s.strip() for s in args.contains.split(',')])
        print treestore.get_subtree(contains=contains, match_all=args.all, format=args.format, prune=not args.complete),

    elif args.command == 'annotate':
        annotate(args.uri, args.file, treestore, format=args.format)


if __name__ == '__main__':
    main()
