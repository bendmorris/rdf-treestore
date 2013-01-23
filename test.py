from treestore import Treestore
import Bio.Phylo as bp
import time
import datetime
from cStringIO import StringIO
import sys
import cPickle as pkl

t = Treestore()
sizes = [50, 100, 200, 500, 1000, 2000, 5000]
ti = lambda x: str(datetime.timedelta(seconds=round(x, 3)))[:-3]

add_times = {}
retrieve_times = {}
write_times = {}
parse_times = {}

print 'size\tadd\tretrieve\twrite\tparse'
for n in sizes:
    s = str(n).zfill(4)

    print s,
    sys.stdout.flush()
    
    start_time = time.time()
    t.add_trees('tests/bird%s.new' % s, 'newick', 'test%s' % s)
    add_times[n] = time.time() - start_time
    print '\t', ti(add_times[n]),
    sys.stdout.flush()

    start_time = time.time()
    tree = t.serialize_trees('test%s' % s)
    retrieve_times[n] = time.time() - start_time
    print '\t', ti(retrieve_times[n]),
    sys.stdout.flush()

    start_time = time.time()
    bp.convert('tests/bird%s.new' % s, 'newick', 'tests/bird%s.cdao' % s, 'cdao')
    write_times[n] = time.time() - start_time
    print '\t', ti(write_times[n]),
    sys.stdout.flush()

    stringio = StringIO()
    start_time = time.time()
    bp.write(bp.read('tests/bird%s.cdao' % s, 'cdao'), stringio, 'newick')
    parse_times[n] = time.time() - start_time
    print '\t', ti(parse_times[n])
    sys.stdout.flush()

data = {}
for term in ('add', 'retrieve', 'write', 'parse'):
    data[term] = eval('%s_times' % term)

pkl.dump(data, open('tests/benchmarks.pkl', 'w'), -1)