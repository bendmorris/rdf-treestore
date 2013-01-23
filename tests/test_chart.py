from pylab import *
import cPickle as pkl

data = pkl.load(open('benchmarks.pkl'))

for key in data:
    xs = []
    ys = []
    for x, y in sorted(data[key].items()):
        xs.append(x)
        ys.append(y)
    plot(xs, ys, label=key)

xscale('log')
yscale('log')
legend()
show()