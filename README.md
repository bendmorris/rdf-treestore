Phylotastic RDF Treestore
=========================

This is an implementation of a phylogenetic tree store with an RDF backend.

### BioPython

treestore requires the librdf Python bindings (http://librdf.org/docs/python.html)
and the author's fork of BioPython. You can install this fork using the following 
commands:

    git clone https://github.com/bendmorris/biopython.git
    cd biopython
    git checkout cdao
    sudo python setup.py install

### DendroPy

BioPython currently has issues with Nexus file parsing. Until this can be resolved,
we are circumventing the issue by using DendroPy, so you'll need to install that 
as well:

    sudo pip install dendropy

or

    sudo easy_install -U dendropy

### Virtuoso ODBC connection

You'll also need an ODBC connection to an RDF store such as Virtuoso. 
Help on setting this up for Virtuoso is here: 

http://docs.openlinksw.com/virtuoso/odbcimplementation.html

And then, install pyodbc:

    sudo pip install pyodbc

or

    sudo easy_install pyodbc

Examples
--------

### Python

Try it out:

    >>> from treestore import Treestore
    >>> t = Treestore()
    >>> t.add_trees('test.newick', 'newick', 'http://www.example.org/test/')
    >>> trees = t.get_trees('http://www.example.org/test/')
    >>> trees.next()
    Tree(weight=1.0, rooted=False)
    >>> print t.serialize_trees('test', 'nexml')
    ...

### Command line tool
    
Or from the command line:

    treestore add test.newick newick http://www.example.org/test/
    treestore get http://www.example.org/test/ nexml
    treestore rm http://www.example.org/test/

If you're not using Virtuoso, or you need to change connection parameters,
refer to the command-line help menu:

    treestore -h
