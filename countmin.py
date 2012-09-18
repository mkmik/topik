# -*- Mode: Python -*-

# estimate top k from a stream using a 'count-min' sketch and heap.
# based on https://github.com/ezyang/ocaml-cminsketch

# also, compare to the 'space saving' algorithm.
# see: http://dimacs.rutgers.edu/~graham/pubs/papers/freqvldbj.pdf

import math
import random
import sys

int_size = len (bin (sys.maxint)) - 1
int_mask = (1 << int_size) - 1

# strange that although C has a log2 in libm, neither python or ocaml does.
def log2 (x):
    return math.log (x) / math.log (2.0)

# hah, in ocaml this *needs* to overflow the int
def multiply_shift (m, a, x):
    return ((a * x)&int_mask) >> (int_size - m)

def random_odd_int():
    n = int (random.getrandbits (int_size-2))
    return n<<1|1

import heapq

class sketch:
    def __init__ (self, k, depth, width):
        # round up the width to a power of 2
        m = int (math.ceil (log2 (float (width))))
        rounded_width = 1 << m
        self.k = k
        self.lg_width = m
        self.count = [ [0] * rounded_width for x in range (depth) ]
        self.hash_functions = [ random_odd_int() for x in range (depth) ]
        self.heap = []
        self.map = {}

    def update (self, key, c=1):
        ix = abs (hash (key))
        est = sys.maxint
        for i in range (len (self.hash_functions)):
            hf = self.hash_functions[i]
            j = multiply_shift (self.lg_width, hf, ix)
            x = self.count[i][j]
            est = min (est, x)
            self.count[i][j] = (x + c)
        self.update_heap (key, est)

    def update_heap (self, key, est):
        if not self.heap or self.heap[0][0] < est:
            probe = self.map.get (key, None)
            if probe is None:
                if len(self.map) < self.k:
                    # still growing...
                    entry = [est, key]
                    heapq.heappush (self.heap, entry)
                    self.map[key] = entry
                else:
                    # push this guy out
                    entry = [est, key]
                    [oest, okey] = heapq.heappushpop (self.heap, entry)
                    del self.map[okey]
                    self.map[key] = entry
            else:
                probe[0] = est
                heapq.heapify (self.heap)
        else:
            pass

    def get (self, key):
        ix = abs (hash (key))
        r = sys.maxint
        for i in range (len (self.hash_functions)):
            hf = self.hash_functions[i]
            j = multiply_shift (self.lg_width, hf, ix)
            r = min (r, self.count[i][j])
        return r

    def get_ranking (self):
        vals = self.map.values()
        vals.sort()
        vals.reverse()
        r = {}
        for i in range (len (vals)):
            r[vals[i][1]] = i
        return r

if __name__ == '__main__':
    s = sketch (200, 20, 500)

    def peek(n):
        re = []
        for i in s.heap:
            re.append((s.get(i[1]), i[1]))

        for i in sorted(re, reverse=True)[0:10]:
            print i[0], i[1]
        

    import string
    n = 0
    for i in open('itwiki-latest-abstract.txt'):
        exclude = set(string.punctuation)
        st = ''.join(ch for ch in i if ch not in exclude)
        for w in st.strip().split():
            s.update(w)

        n += 1
        if n % 10000 == 0:
            print "------"
            print "TOP 10"
            peek(10)


    print "TOP 10"
    peek(10)
