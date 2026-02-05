#!/usr/bin/env python3

import argparse
import logging
import os
from tests.btree_random import BTreeRandomTest
from tests.btree_incremental import BTreeIncrementalTest
from tests.btree_mergejoin import BTreeMergeJoinTest
from tests.btree_mergejoin_anti import BTreeMergeAntiJoinTest
from tests.btree_mergejoin_semi import BTreeMergeSemiJoinTest
from tests.btree_nestloop import BTreeNestLoopTest
from tests.hash import HashIncrementalTest
import time


if __name__ == '__main__':

	FORMAT = '%(asctime)s\t%(levelname)s\t%(module)s\t%(name)s\t%(message)s'
	logging.basicConfig(level=logging.INFO, format=FORMAT)

	parser = argparse.ArgumentParser(prog='stress-test')

	parser.add_argument('--btree-random', action='store_true')
	parser.add_argument('--btree-incremental', action='store_true')
	parser.add_argument('--btree-mergejoin', action='store_true')
	parser.add_argument('--btree-mergejoin-semi', action='store_true')
	parser.add_argument('--btree-mergejoin-anti', action='store_true')
	parser.add_argument('--btree-nestloop', action='store_true')
	parser.add_argument('--hash', action='store_true')

	args = parser.parse_args()

	# which tests to run? by default all, unless some are selected explicitly

	tests = []

	if args.btree_random:
		tests.append(BTreeRandomTest)

	if args.btree_incremental:
		tests.append(BTreeIncrementalTest)

	if args.btree_mergejoin:
		tests.append(BTreeMergeJoinTest)

	if args.btree_mergejoin_semi:
		tests.append(BTreeMergeSemiJoinTest)

	if args.btree_mergejoin_anti:
		tests.append(BTreeMergeAntiJoinTest)

	if args.btree_nestloop:
		tests.append(BTreeNestLoopTest)

	if args.hash:
		tests.append(HashIncrementalTest)

	# nothing selected, run all tests
	if len(tests) == 0:
		tests.append(BTreeRandomTest)
		tests.append(BTreeIncrementalTest)
		tests.append(BTreeMergeJoinTest)
		tests.append(BTreeMergeSemiJoinTest)
		tests.append(BTreeMergeAntiJoinTest)
		tests.append(BTreeNestLoopTest)
		tests.append(HashIncrementalTest)

	idx = 0
	workers = []
	while True:

		for t in tests:
			idx += 1
			workers.append(t(idx))

		if len(workers) >= os.cpu_count():
			break

	[w.start() for w in workers]
	[w.join() for w in workers]
