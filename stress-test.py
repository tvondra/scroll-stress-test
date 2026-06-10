#!/usr/bin/env python3

import argparse
import logging

FORMAT = '%(asctime)s\t%(levelname)s\t%(module)s\t%(name)s\t%(message)s'
logging.basicConfig(level=logging.INFO, format=FORMAT)

import os
from tests.btree_random import BTreeRandomTest
from tests.btree_incremental import BTreeIncrementalTest
from tests.btree_parallel import BTreeParallelTest
from tests.btree_mergejoin import BTreeMergeJoinTest
from tests.btree_mergejoin_parallel import BTreeMergeJoinParallelTest
from tests.btree_mergejoin_anti import BTreeMergeAntiJoinTest
from tests.btree_mergejoin_semi import BTreeMergeSemiJoinTest
from tests.btree_nestloop import BTreeNestLoopTest
from tests.btree_nestloop_parallel import BTreeNestLoopParallelTest
from tests.btree_nestloop_anti import BTreeNestLoopAntiTest
from tests.btree_nestloop_semi import BTreeNestLoopSemiTest
from tests.btree_nestloop_limit import BTreeNestLoopLimitTest
from tests.btree_nestloop_lateral import BTreeNestLoopLateralTest
from tests.gist_random import GistRandomTest
from tests.gist_incremental import GistIncrementalTest
from tests.gist_ordered_random import GistOrderedRandomTest
from tests.gist_ordered_incremental import GistOrderedIncrementalTest
from tests.gist_ordered_points_incremental import GistOrderedPointsIncrementalTest
from tests.gist_ordered_points_random import GistOrderedPointsRandomTest
from tests.hash_incremental import HashIncrementalTest
from tests.hash_random import HashRandomTest
from tests.hash_nestloop import HashNestLoopTest
from tests.hash_nestloop_anti import HashNestLoopAntiTest
from tests.hash_nestloop_semi import HashNestLoopSemiTest

from utils.vacuum import VacuumWorker

import time


if __name__ == '__main__':

	parser = argparse.ArgumentParser(prog='stress-test')

	parser.add_argument('--workers', type=int, default=0, action='store')
	parser.add_argument('--btree-random', action='store_true')
	parser.add_argument('--btree-incremental', action='store_true')
	parser.add_argument('--btree-parallel', action='store_true')
	parser.add_argument('--btree-mergejoin', action='store_true')
	parser.add_argument('--btree-mergejoin-parallel', action='store_true')
	parser.add_argument('--btree-mergejoin-semi', action='store_true')
	parser.add_argument('--btree-mergejoin-anti', action='store_true')
	parser.add_argument('--btree-nestloop', action='store_true')
	parser.add_argument('--btree-nestloop-parallel', action='store_true')
	parser.add_argument('--btree-nestloop-semi', action='store_true')
	parser.add_argument('--btree-nestloop-anti', action='store_true')
	parser.add_argument('--btree-nestloop-limit', action='store_true')
	parser.add_argument('--btree-nestloop-lateral', action='store_true')
	parser.add_argument('--gist-random', action='store_true')
	parser.add_argument('--gist-incremental', action='store_true')
	parser.add_argument('--gist-ordered-random', action='store_true')
	parser.add_argument('--gist-ordered-incremental', action='store_true')
	parser.add_argument('--gist-ordered-points-random', action='store_true')
	parser.add_argument('--gist-ordered-points-incremental', action='store_true')
	parser.add_argument('--hash-random', action='store_true')
	parser.add_argument('--hash-incremental', action='store_true')
	parser.add_argument('--hash-nestloop', action='store_true')
	parser.add_argument('--hash-nestloop-anti', action='store_true')
	parser.add_argument('--hash-nestloop-semi', action='store_true')

	args = parser.parse_args()

	# which tests to run? by default all, unless some are selected explicitly

	tests = []

	if args.btree_random:
		tests.append(BTreeRandomTest)

	if args.btree_incremental:
		tests.append(BTreeIncrementalTest)

	if args.btree_parallel:
		tests.append(BTreeParallelTest)

	if args.btree_mergejoin:
		tests.append(BTreeMergeJoinTest)

	if args.btree_mergejoin_parallel:
		tests.append(BTreeMergeJoinParallelTest)

	if args.btree_mergejoin_semi:
		tests.append(BTreeMergeSemiJoinTest)

	if args.btree_mergejoin_anti:
		tests.append(BTreeMergeAntiJoinTest)

	if args.btree_nestloop:
		tests.append(BTreeNestLoopTest)

	if args.btree_nestloop_parallel:
		tests.append(BTreeNestLoopParallelTest)

	if args.btree_nestloop_anti:
		tests.append(BTreeNestLoopAntiTest)

	if args.btree_nestloop_semi:
		tests.append(BTreeNestLoopSemiTest)

	if args.btree_nestloop_limit:
		tests.append(BTreeNestLoopLimitTest)

	if args.btree_nestloop_lateral:
		tests.append(BTreeNestLoopLateralTest)

	if args.gist_random:
		tests.append(GistRandomTest)

	if args.gist_incremental:
		tests.append(GistIncrementalTest)

	if args.gist_ordered_random:
		tests.append(GistOrderedRandomTest)

	if args.gist_ordered_incremental:
		tests.append(GistOrderedIncrementalTest)

	if args.gist_ordered_points_random:
		tests.append(GistOrderedPointsRandomTest)

	if args.gist_ordered_points_incremental:
		tests.append(GistOrderedPointsIncrementalTest)

	if args.hash_random:
		tests.append(HashRandomTest)

	if args.hash_incremental:
		tests.append(HashIncrementalTest)

	if args.hash_nestloop:
		tests.append(HashNestLoopTest)

	if args.hash_nestloop_anti:
		tests.append(HashNestLoopAntiTest)

	if args.hash_nestloop_semi:
		tests.append(HashNestLoopSemiTest)


	# nothing selected, run all tests
	if len(tests) == 0:
		tests.append(BTreeRandomTest)
		tests.append(BTreeIncrementalTest)
		tests.append(BTreeParallelTest)
		tests.append(BTreeMergeJoinTest)
		tests.append(BTreeMergeJoinParallelTest)
		tests.append(BTreeMergeSemiJoinTest)
		tests.append(BTreeMergeAntiJoinTest)
		tests.append(BTreeNestLoopTest)
		tests.append(BTreeNestLoopParallelTest)
		tests.append(BTreeNestLoopAntiTest)
		tests.append(BTreeNestLoopSemiTest)
		tests.append(GistRandomTest)
		tests.append(GistIncrementalTest)
		tests.append(GistOrderedRandomTest)
		tests.append(GistOrderedIncrementalTest)
		tests.append(GistOrderedPointsRandomTest)
		tests.append(GistOrderedPointsIncrementalTest)
		tests.append(HashIncrementalTest)
		tests.append(HashNestLoopTest)
		tests.append(HashRandomTest)
		tests.append(HashNestLoopAntiTest)
		tests.append(HashNestLoopSemiTest)
		tests.append(BTreeNestLoopLimitTest)
		tests.append(BTreeNestLoopLateralTest)

	num_workers = args.workers
	if num_workers == 0:
		num_workers = os.cpu_count()

	idx = 0
	workers = []
	while True:

		for t in tests:
			idx += 1
			workers.append(t(idx))

		if len(workers) >= num_workers:
			break

	workers.append(VacuumWorker())

	[w.start() for w in workers]
	[w.join() for w in workers]
