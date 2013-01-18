This is an implementation of a phylogenetic tree store with an RDF backend.

treestore requires the librdf Python bindings (http://librdf.org/docs/python.html)
and the author's fork of Biopython (included as a Git submodule.) You can install
this fork using the following commands:

    git clone https://github.com/bendmorris/biopython.git
    cd biopython
    git checkout cdao
    python setup.py install

Try it out (you'll need an ODBC connection to Virtuoso; you can specify connection
parameters when initializing the Treestore object):

    >>> from treestore import Treestore
    >>> t = Treestore()
    >>> t.add_trees('test.newick', 'test')
    >>> trees = t.get_trees('test')
    >>> trees.next()
    Tree(weight=1.0, rooted=False)
    >>> print t.serialize_trees('test', 'nexml')
    ...
    
Or from the command line:

    $ python treestore.py add test.newick newick
    $ python treestore.py get test nexml