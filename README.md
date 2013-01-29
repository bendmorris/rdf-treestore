This is an implementation of a phylogenetic tree store with an RDF backend.

treestore requires the librdf Python bindings (http://librdf.org/docs/python.html)
and the author's fork of Biopython. You can install this fork using the following 
commands:

    git clone https://github.com/bendmorris/biopython.git
    cd biopython
    git checkout cdao
    python setup.py install

You'll also need an ODBC connection to an RDF store such as Virtuoso; help on 
setting this up for Virtuoso is here: 
http://docs.openlinksw.com/virtuoso/odbcimplementation.html

Try it out:

    >>> from treestore import Treestore
    >>> t = Treestore()
    >>> t.add_trees('test.newick', 'newick', 'test')
    >>> trees = t.get_trees('test')
    >>> trees.next()
    Tree(weight=1.0, rooted=False)
    >>> print t.serialize_trees('test', 'nexml')
    ...
    
Or from the command line:

    treestore add test.newick newick test
    treestore get test nexml
    treestore rm test

If you're not using Virtuoso, or you need to change connection parameters,
refer to the command-line help menu:

    treestore -h