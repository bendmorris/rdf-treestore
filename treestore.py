#!/usr/bin/env python
import Bio.Phylo as bp
import os
import re
import sha
import shutil
import sys
import pypyodbc as pyodbc
from pruner import Prunable
from annotate import Annotatable
import tempfile
import phylolabel
import time
from cStringIO import StringIO


__version__ = '0.1.2'
treestore_dir = os.path.join(tempfile.gettempdir(), 'treestore')
if not os.path.exists(treestore_dir): os.makedirs(treestore_dir)

class Treestore(Prunable, Annotatable):
    def __init__(self, storage_name='virtuoso', dsn='Virtuoso', 
                 user='dba', password='dba', storage=None):
        '''Create a treestore object from an ODBC connection with given DSN,
        username and password.'''

        self.dsn = dsn
        self.user = user
        self.password = password


    def get_connection(self):
        return pyodbc.connect('DSN=%s;UID=%s;PWD=%s' % (self.dsn, self.user, self.password),
                              autocommit=True)

    odbc_connection = property(get_connection)

    def get_cursor(self):
        connection = self.odbc_connection
        return connection.cursor()

    def add_trees(self, tree_file, format, tree_uri=None, rooted=False, 
        taxonomy=None, tax_root=None):
        '''Convert trees residing in a text file into RDF, and add them to the
        underlying RDF store with a context node for retrieval.
        
        Example:
        >>> treestore.add_trees('test.newick', 'newick', 'http://www.example.org/test/')
        '''
        
        if tree_uri is None: tree_uri = os.path.basename(tree_file)
        
        hash = sha.sha()
        hash.update(str(time.time()))
        tempfile_name = '%s.cdao' % hash.hexdigest()

        if taxonomy:
            # label higher-order taxa before adding
            phylogeny = bp.read(tree_file, format)
            if isinstance(taxonomy, basestring):
                taxonomy = self.get_trees(taxonomy)[0]
            phylolabel.label_tree(phylogeny, taxonomy, tax_root=tax_root)
            with open(os.path.join(treestore_dir, tempfile_name), 'w') as output_file:
                bp._io.write([phylogeny], output_file, 'cdao')
            
        else:
            if format == 'cdao':
                # if it's already in CDAO format, just copy it
                f1, f2 = tree_file, os.path.join(treestore_dir, tempfile_name)
                if not os.path.abspath(f1) == os.path.abspath(f2):
                    shutil.copy(f1, f2)
            else:
                # otherwise, convert to CDAO
                bp.convert(tree_file, format, os.path.join(treestore_dir, tempfile_name), 'cdao', 
                           tree_uri=tree_uri, rooted=rooted)
        
        # run the bulk loader to load the CDAO tree into Virtuoso
        cursor = self.get_cursor()
        
        update_stmt = 'sparql load <file://%s> into <%s>' % (
            os.path.abspath(os.path.join(treestore_dir, tempfile_name)), tree_uri)
        
        load_stmt = "ld_dir ('%s', '%s', '%s')" % (
            os.path.abspath(treestore_dir), tempfile_name, tree_uri)
        print load_stmt
        cursor.execute(load_stmt)
        
        update_stmt = "rdf_loader_run()"
        print update_stmt
        cursor.execute(update_stmt)
        
        # the next treestore add may not work if you don't explicitly delete 
        # the bulk load list from the Virtuoso db after it's done
        cursor.execute('DELETE FROM DB.DBA.load_list')
        
        os.remove(os.path.join(treestore_dir, tempfile_name))
        
        
    def get_trees(self, tree_uri):
        '''Retrieve trees that were previously added to the underlying RDF 
        store. Returns a generator of Biopython trees.
        
        Example:
        >>> trees = treestore.get_trees('http://www.example.org/test/')
        >>> trees.next()
        Tree(weight=1.0, rooted=False)
        '''
        
        return [self.subtree(None, tree_uri)]

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
            bp._utils.draw_ascii((i for i in trees).next(), file=s)
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

        cursor = self.get_cursor()
        cursor.execute('sparql clear graph <%s>' % tree_uri)


    def list_trees(self):
        '''List all trees in the treestore.
        '''

        query = '''sparql
PREFIX obo: <http://purl.obolibrary.org/obo/>

SELECT DISTINCT ?graph
WHERE {
    GRAPH ?graph {
        [] obo:CDAO_0000148 [] . 
    }
}
ORDER BY ?graph
'''
        cursor = self.get_cursor()
        cursor.execute(query)
        
        return [str(result[0]) for result in cursor]


    def list_trees_containing_taxa(self, contains=[], show_counts=False, filter=None):
        '''List all trees that contain the specified taxa.'''

        query = self.build_query('''
SELECT DISTINCT ?graph (count(DISTINCT ?label) as ?matches)
WHERE {
    GRAPH ?graph {
        ?tree obo:CDAO_0000148 [] .
        { ?match rdfs:label ?label . FILTER (?label in (%s)) }
        %s
    }
} 
GROUP BY ?graph ?tree
ORDER BY DESC(?matches)
''' % (', '.join(['"%s"' % contain for contain in contains]), filter if filter else ''))
        cursor = self.get_cursor()
        cursor.execute(query)
        
        for result in cursor:
            if show_counts: yield '%s (%s)' % (result[0], result[1])
            else: yield str(result[0])


    def get_names(self, tree_uri=None, format=None):
        query = '''sparql
PREFIX obo: <http://purl.obolibrary.org/obo/>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

SELECT DISTINCT ?uri, ?label
WHERE {
    GRAPH %s {
        [] obo:CDAO_0000148 [] .
        ?uri rdfs:label ?label .
    }
}
ORDER BY ?label
''' % (('<%s>' % tree_uri) if tree_uri else '?graph')
        
        cursor = self.get_cursor()
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
            return [str(result[1]) for result in results]
        
        
    def build_query(self, query):
        return '''sparql
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX owl: <http://www.w3.org/2002/07/owl#>
PREFIX dc: <http://purl.org/dc/elements/1.1/>
PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
PREFIX bibo: <http://purl.org/ontology/bibo/>
PREFIX foaf: <http://xmlns.com/foaf/0.1/>
PREFIX prism: <http://prismstandard.org/namespaces/basic/2.0/>
PREFIX obo: <http://purl.obolibrary.org/obo/>
PREFIX doi: <http://dx.doi.org/>''' + query
        
        
    def get_tree_info(self, tree_uri=None):
        query = '''sparql
PREFIX obo: <http://purl.obolibrary.org/obo/>

SELECT ?graph ?tree (count(?otu) as ?taxa)
WHERE {
    GRAPH ?graph {
        ?tree obo:CDAO_0000148 [] .
        ?otu obo:CDAO_0000187 [] .
    }
    %s
} 
GROUP BY ?graph ?tree
ORDER BY ?graph
''' % ('' if tree_uri is None else ('FILTER(?graph = <%s>)' % tree_uri))
        cursor = self.get_cursor()
        cursor.execute(query)
        
        return [{k:v for k, v in zip(('uri', 'tree', 'taxa'), result) } for result in cursor]


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
    
    # treestore add: add trees to the database
    add_parser = subparsers.add_parser('add', help='add trees to treestore')
    add_parser.add_argument('file', help='tree file')
    add_parser.add_argument('uri', help='tree uri (default=file name)', nargs='?', default=None)
    add_parser.add_argument('-f', '--format', help='file format (%s)' % input_formats,
                            nargs='?', default='newick')
    add_parser.add_argument('--rooted', help='this is a rooted tree', action='store_true')
    add_parser.add_argument('--taxonomy', help="the URI of a taxonomy graph to label higher-order taxa",
                            nargs='?', default=None)
    add_parser.add_argument('--tax-root', help="the name of the top-most taxonomic group in the tree, used to subset the taxonomy and avoid homonymy issues",
                            nargs='?', default=None)
    
    # treestore get: download an entire tree
    get_parser = subparsers.add_parser('get', help='retrieve trees from treestore')
    get_parser.add_argument('uri', help='tree uri')
    get_parser.add_argument('-f', '--format', help='serialization format (%s) (default=newick)' % output_formats, 
                            nargs='?', default='newick')
    
    # treestore rm: delete trees from the database
    rm_parser = subparsers.add_parser('rm', help='remove trees from treestore')
    rm_parser.add_argument('uri', help='tree uri')

    # treestore ls: list trees
    ls_parser = subparsers.add_parser('ls', help='list all trees in treestore')
    ls_parser.add_argument('contains', help='comma-delimited list of desired taxa',
                           nargs='?', default='')
    ls_parser.add_argument('--counts', help="display the number of matched taxa next to each tree URI",
                           action='store_true')
    
    # treestore names: get list of taxa contained in a tree
    names_parser = subparsers.add_parser('names', 
                                         help='return a comma-separated list of all taxa names')
    names_parser.add_argument('uri', help='tree uri (default=all trees)', 
                              nargs='?', default=None)
    names_parser.add_argument('-f', '--format', help='file format (json, csv, xml) (default=csv)', 
                              default='csv')
    
    # treestore count: count the number of labeled nodes
    count_parser = subparsers.add_parser('count', 
                                         help='returns the number of labeled nodes in a tree')
    count_parser.add_argument('uri', help='tree uri (default=all trees)', 
                              nargs='?', default=None)
    
    # treestore query: create a subtree from a list of taxa
    query_parser = subparsers.add_parser('query', 
                                         help='retrieve the best subtree containing a given set of taxa')
    query_parser.add_argument('contains', help='comma-delimited list of desired taxa',
                              nargs='?')
    query_parser.add_argument('uri', help='tree uri (default=select automatically)', 
                              nargs='?', default=None)
    query_parser.add_argument('-f', '--format', help='serialization format (%s) (default=newick)' % output_formats, 
                              nargs='?', default='newick')
    query_parser.add_argument('--complete', help="return complete subtree from MRCA; don't prune other taxa from the resulting tree",
                              action='store_true')
    query_parser.add_argument('--taxonomy', help="the URI of a taxonomy graph to enable synonymy lookup",
                              nargs='?', default=None)
    query_parser.add_argument('--filter', help="SPARQL graph pattern that returned trees must match",
                              nargs='?', default=None)
    
    # treestore annotate: add metadata annotations to tree
    ann_parser = subparsers.add_parser('annotate', help='annotate tree with triples from RDF file')
    ann_parser.add_argument('uri', help='tree uri', default=None)
    ann_parser.add_argument('--file', help='annotation file')
    ann_parser.add_argument('--text', help='annotation, in turtle format', default=None)
    ann_parser.add_argument('--doi', help='tree source DOI', default=None)

    args = parser.parse_args()

    kwargs = {}
    if args.dsn: kwargs['dsn'] = args.dsn
    if args.user: kwargs['user'] = args.user
    if args.password: kwargs['password'] = args.password
    treestore = Treestore(**kwargs)

    if args.command == 'add':
        # parse a tree and add it to the treestore
        treestore.add_trees(args.file, args.format, args.uri, rooted=args.rooted,
                            taxonomy=args.taxonomy, tax_root=args.tax_root)
        
    elif args.command == 'get':
        # get a tree, serialize in specified format, and output to stdout
        print treestore.serialize_trees(args.uri, args.format),
        
    elif args.command == 'rm':
        # remove a certain tree from the treestore
        treestore.remove_trees(args.uri)
        
    elif args.command == 'ls':
        # list all trees in the treestore or trees containing a list of taxa
        contains = args.contains
        if contains: 
            contains = set([s.strip() for s in contains.split(',')])
            trees = list(treestore.list_trees_containing_taxa(contains=contains, show_counts=args.counts))
        else:
            trees = treestore.list_trees()

        if not trees: exit()
        
        import lscolumns
        lscolumns.printls(trees)


    elif args.command == 'names':
        print treestore.get_names(tree_uri=args.uri, format=args.format)

    elif args.command == 'count':
        print len([r for r in treestore.get_names(tree_uri=args.uri, format=None)])

    elif args.command == 'query':
        contains = set([s.strip() for s in args.contains.split(',')])
        print treestore.get_subtree(contains=contains, tree_uri=args.uri,
                                    format=args.format, 
                                    prune=not args.complete,
                                    taxonomy=args.taxonomy,
                                    filter=args.filter
                                    )

    elif args.command == 'annotate':
        treestore.annotate(args.uri, annotations=args.text, annotation_file=args.file, doi=args.doi)


if __name__ == '__main__':
    main()
