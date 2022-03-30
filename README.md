# dmds_lab01

Lab #1 of Renato Rao and Dominik Briner for the course Data Management & Data Structures of the University of Bern.

## Properties of a B+-Tree

1. All leaves are at the bottom level and have a pointer to the next leaf.
2. A B-Tree is defined by the term minimum degree 'BRANCHING_FACTOR'. The value of BRANCHING_FACTOR depends upon disk block size
3. Every node except root must contain at least (ceiling)(L = [BRANCHING_FACTOR - 1]/2) keys.
4. All keys of a node are sorted in increasing order. The child between two keys k1 and k2 contains no keys outside the range from k1 and k2.
5. B-Tree grows and shrinks from the root.
6. Time complexity for Get and Put is O(log n).

## Possible improvements

key prefix truncation:
https://benjamincongdon.me/blog/2021/08/17/B-Trees-More-Than-I-Thought-Id-Want-to-Know/

sibling pointers

## Buffer Pool Manager

The structures in the "infrastructure" package are based on the reference implementation from here:
https://brunocalza.me/how-buffer-pool-works-an-implementation-in-go/


## Other KV implementations in Go (for reference)

https://github.com/akrylysov/pogreb/blob/master/db.go


## Some benchmark test examples (for reference)
https://github.com/recoilme/pogreb-bench

