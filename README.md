# stress test of scrollable cursors

Compares results between two instances (master and patched).


## b-tree

* `scroll-stress-test-incremental.py` - scrolling to random positions and back
* `scroll-stress-test-random.py` - random scrolling through a cursor

## hash

* `scroll-stress-test-hash.py` - scrolling to random positions and back

## misc

* `init.sh` - creates two cluster (has hardcoded paths to binaries etc.)
