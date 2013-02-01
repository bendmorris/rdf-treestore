import Bio.Phylo as bp
import RDF
import Redland_python
import dendropy
import os
import re
import sha
import sys
import urlparse
import tempfile
import pyodbc
import pruner
from cStringIO import StringIO


__version__ = '0.1.2'
treestore_dir = '/home/ben/Dev/treestore/'

class Treestore:
    def __init__(self, storage_name='virtuoso', dsn='Virtuoso', 
                 user='dba', password='dba', options_string=None, storage=None):
        '''Create a treestore object from an ODBC connection with given DSN,
        username and password.'''

        try:
            if storage:
                self.store = storage
            else:
                self.store = RDF.Storage(storage_name=storage_name, name='db',
                                         options_string="dsn='%s',user='%s',password='%s'" 
                                         % (dsn, user, password)
                                         if not options_string else options_string
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


    def add_trees(self, tree_file, format, tree_name=None, bulk_loader=None, puid=False):
        '''Convert trees residing in a text file into RDF, and add them to the
        underlying RDF store with a context node for retrieval.

        Example:
        >>> treestore.add_trees('test.newick', 'newick', 'test')
        '''
        
        if tree_name is None: tree_name = os.path.basename(tree_file)

        # If the source is an N-Triples file, then import it "as is":
        if format == 'ntriples':
            model = RDF.Model(self.store)
            file_model = RDF.Model()
            RDF.Parser(name = 'ntriples').parse_into_model(file_model, 'file://%s' % os.path.abspath(tree_file))
            for triple in file_model:
                model.add_statement(triple, RDF.Node(RDF.Uri(tree_name)))
            model.sync()
            return

        # All other formats are processed:

        # NEXUS files are not properly handled by BioPython (Jan 2013), so convert them
        # to Newick format with DendroPy:
        tmp_file = None
        if format == 'nexus':
            tree = dendropy.Tree(stream=open(tree_file), schema=format)
            format = 'newick' 
            tmp_file = tempfile.NamedTemporaryFile()
            tmp_file.write(re.sub(r'\[.*\]\s*', '', tree.as_string(format)))
            tmp_file.flush()
            tree_file = tmp_file.name
        
        if puid:
            # Create a pseudo-unique URI for trees, if the tree name is not a URI already:
            if not re.match(r'\w+://', tree_name):
                puid = sha.new(open(tree_file).read()).hexdigest()
                tree_name = 'http://phylotastic.org/hack2/%s/%s' % (puid, tree_name)

        if bulk_loader:
            if self.odbc_connection == None:
                print 'Woops. \'pyodbc\' is not available on this platform.'
                return

            bp.convert(tree_file, format, os.path.join(treestore_dir, 'temp.cdao'), 'cdao', 
                       tree_name=tree_name)
        
            cursor = self.odbc_connection.cursor()
        
            update_stmt = 'sparql load <file://%s> into <%s>' % (
                os.path.abspath(os.path.join(treestore_dir, 'temp.cdao')), tree_name)
        
            load_stmt = "ld_dir ('%s', 'temp.cdao', '%s')" % (
                os.path.abspath(treestore_dir), tree_name)
            print load_stmt
            cursor.execute(load_stmt)
        
            update_stmt = "rdf_loader_run()"
            print update_stmt
            cursor.execute(update_stmt)

            cursor.execute('DELETE FROM DB.DBA.load_list')
        

        else:
            bp.convert(tree_file, format, None, 'cdao', 
                       storage=self.store, tree_name=tree_name, context=tree_name)
        
        if tmp_file != None: tmp_file.close()

        
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
        

    def serialize_trees(self, tree_name='', format='newick', trees=None):
        '''Retrieve trees serialized to any format supported by Biopython.
        
        Current options include 'newick', 'nexus', 'phyloxml', 'nexml', and 'cdao'

        Example:
        >>> treestore.serialize_trees('test')
        '''

        if trees is None: trees = self.get_trees(tree_name)

        s = StringIO()
        if format == 'cdao':
            bp.write(trees, s, format, tree_name=tree_name)
        elif format == 'ascii':
            bp._utils.draw_ascii((i for i in trees).next())
        else:
            bp.write(trees, s, format)

        return s.getvalue()


    def remove_trees(self, tree_name):
        context = RDF.Node(RDF.Uri(tree_name))
        cursor = self.odbc_connection.cursor()
        cursor.execute('sparql clear graph <%s>' % tree_name)


    def list_trees(self, contains=[], match_all=False, show_match_counts=False):
        # TODO: use the smaller tree in the event of a match tie for efficiency

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
            if (not match_all) or int(str(result['matches']))==len(contains):
                yield str(result[0]) + (' (%s)' % result[1] 
                                         if (contains 
                                         and not match_all 
                                         and show_match_counts) 
                                         else '') 

    def list_uris(self):
        model = RDF.Model(self.store)

        query = '''
PREFIX obo: <http://purl.obolibrary.org/obo/>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>

SELECT DISTINCT ?graph ?uri
WHERE {
    GRAPH ?graph {
        { ?s obo:CDAO_0000200 ?uri . }
    }
} 
'''

        query = RDF.SPARQLQuery(query)
        
        def handler(*args): pass
        Redland_python.set_callback(handler)
        results = query.execute(model)
        Redland_python.reset_callback()

        for result in results:
            name, separator, identifier = str(result['uri']).partition('#')
            whitespace = ' '
            if len(name) < 24: whitespace = ' ' * (24 - len(name))
            yield '%s%s%s' % (name, whitespace, '%s#%s' % (result['graph'], identifier))


    def get_names(self, tree_name=None, format=None):
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
''' % (('<%s>' % tree_name) if tree_name else '?graph')
        
        #print query
        cursor = self.odbc_connection.cursor()
        cursor.execute(query)

        results = cursor
        
        if format == 'json':
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
        elif format =='csv':
            return ','.join([str(result[1]) for result in results])
        else: 
            return results


    def get_subtree(self, contains=[], contains_ids=[], match_all=False, format='newick'):
        if not contains or contains_ids: raise Exception('A list of taxa or ids is required.')
        trees = self.list_trees(contains=contains, match_all=match_all)
        try:
            tree = trees.next()
        except StopIteration:
            raise Exception("An appropriate tree for this query couldn't be found.")
        
        mrca = pruner.mrca(list(contains), self, tree)
        
        root = bp.CDAO.Clade()
        nodes = {}
        nodes[mrca] = root
        stmts = pruner.subtree(mrca, self, tree)
        redo = True
        
        while redo:
            redo = []
            for stmt in stmts:
                node_id, edge_length, parent, label = stmt
                if parent in nodes:
                    clade = bp.CDAO.Clade(name=label, branch_length=float(edge_length) if edge_length else None)
                    nodes[node_id] = clade
                    nodes[parent].clades.append(clade)
                else:
                    redo.append(stmt)
            stmts = redo
            
        tree = bp.CDAO.Tree(root=root, rooted=True)
        
        def prune_extra_clades(tree, clade, root=True):
            result = 0
            for child in clade.clades:
                result += prune_extra_clades(tree, child, False)
            if not root and len(clade) < 2 and not clade.name:
                try: 
                    tree.collapse(clade)
                    return 1
                except: return 0
            return result

        terms = [c.name for c in tree.get_terminals()]
        for term in terms:
            if not term in contains:
                tree.prune(term)
        while (prune_extra_clades(tree, tree.clade) or 
               any([tree.prune(term) for term in tree.get_terminals() if not term.name])):
            pass

        return self.serialize_trees(trees=[tree], format=format)



def main():
    import argparse

    bp_formats = ' | '.join(bp._io.supported_formats)
    input_formats = '%s | ntriples' % bp_formats
    output_formats = '%s | ascii' % bp_formats

    parser = argparse.ArgumentParser()
    parser.add_argument('-v', '--version', action='version', version=__version__)
    parser.add_argument('-s', '--store', help='name of Redland store (default=virtuoso)')
    parser.add_argument('-d', '--dsn', help='ODBC DSN (default=Virtuoso)')
    parser.add_argument('-u', '--user', help='ODBC user (default=dba)')
    parser.add_argument('-p', '--password', help='ODBC password (default=dba)')
    parser.add_argument('-o', '--option', 
        help='options_string for Redland store; if this option is provided, dsn/user/password options are ignored')

    subparsers = parser.add_subparsers(help='sub-command help', dest='command')

    add_parser = subparsers.add_parser('add', help='add trees to treestore')
    add_parser.add_argument('file', help='tree file')
    add_parser.add_argument('format', help='file format (%s)' % input_formats)
    add_parser.add_argument('name', help='tree uri (default=file name)', nargs='?', default=None)
    add_parser.add_argument('--bulk', help='use the virtuoso bulk loader', action='store_true')
    add_parser.add_argument('--puid', help='create a pseudo-unique ID for the tree', action='store_true')

    get_parser = subparsers.add_parser('get', help='retrieve trees from treestore')
    get_parser.add_argument('name', help='tree name')
    get_parser.add_argument('format', help='serialization format (%s) (default=newick)' % output_formats, 
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

    uri_parser = subparsers.add_parser('uri', help='returns URIs of stored trees')

    count_parser = subparsers.add_parser('count', 
                                         help='returns the number of labelled nodes in a tree')
    count_parser.add_argument('tree', help='name of tree (default=all trees)', 
                              nargs='?', default=None)

    prune_parser = subparsers.add_parser('prune', 
                                         help='retrieve the best subtree containing a given set of taxa')
    prune_parser.add_argument('contains', 
        help='comma-delimited list of species that must be contained in each returned tree',
        nargs='?')
    prune_parser.add_argument('format', help='serialization format (%s) (default=newick)' % output_formats, 
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
        treestore.add_trees(args.file, args.format, args.name, bulk_loader=args.bulk, puid=args.puid)
        
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
        print treestore.get_names(tree_name=args.tree, format=args.format)

    elif args.command == 'count':
        print len([r for r in treestore.get_names(tree_name=args.tree, format=None)])

    elif args.command == 'prune':
        contains = set([s.strip() for s in args.contains.split(',')])
        print treestore.get_subtree(contains=contains, match_all=args.all, format=args.format),


    elif args.command == 'uri':
        uris = treestore.list_uris()
        for uri in uris: print uri

if __name__ == '__main__':
    main()
