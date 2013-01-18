This is an implementation of a phylogenetic tree store with an RDF backend.

treestore requires the librdf Python bindings (http://librdf.org/docs/python.html)
and the author's fork of Biopython (included as a Git submodule.) You can install
this fork using the following commands:

    git clone https://github.com/bendmorris/biopython.git
    cd biopython
    git checkout cdao
    python setup.py install

