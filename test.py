from treestore import Treestore

t = Treestore()
t.add_trees('test.newick', 'newick', 'test')

trees = t.get_trees('test')
print repr(trees.next())