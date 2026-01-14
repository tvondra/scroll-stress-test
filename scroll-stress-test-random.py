#!/usr/bin/env python3
#
# randomized stress test of scrollable cursor with b-tree index scans
#
# 1. picks random parameters for a run (seed, fuzz, fillfactors, ios, ...)
# 2. generates a table (1-10 columns) with pseudo-random data (seed+fuzz)
# 3. creates a b-tree index on the table
# 4. declares a scrollable cursor with random SAOP conditions
# 5. randomly scrolls through the cursor (to the end and back)
# 6. steps are random (but skewed to forward/backward direction)
# 7. each step compares results from master and patched connection
#

import io
import logging
from multiprocessing import Process
import os
import psycopg2
import psycopg2.extras
import random
import time

USER=os.environ.get("USER")	# used to connect to the instances (no password)
PORT_MASTER=5001	# connection to master instance
PORT_PREFETCH=5002	# connection to patched instance

ROWS=100000		# number of rows
LOOPS=100		# number of passes in either direction
STEP=100		# maximum FETCH step (random 1..STEP)

# up to 10 columns
COLUMNS = ['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j']

# primes used to generate 'random' data
PRIMES = [19, 23, 29, 31, 37, 41, 43, 47, 53, 59, 61, 67, 71, 73, 79,
		  83, 89, 97, 101, 103, 107, 109, 113, 127, 131, 137, 139, 149,
		  151, 157, 163, 167, 173, 179, 181, 191, 193, 197, 199, 211,
		  223, 227, 229, 233, 239, 241, 251, 257, 263, 269, 271, 277,
		  281, 283, 293, 307, 311, 313, 317, 331, 337, 347, 349, 353,
		  359, 367, 373, 379, 383, 389, 397, 401, 409, 419, 421, 431,
		  433, 439, 443, 449, 457, 461, 463, 467, 479, 487, 491, 499,
		  503, 509, 521, 523, 541]

def run_sql(wid, did, cur, sql, log):
	'''
	log SQL and execute it
	'''

	if log:
		logger = logging.getLogger(f'worker-{wid}-{did}')
		logger.info(f'SQL: {sql};')

	cur.execute(sql)


def create_table(wid, did, conn, columns, fillfactor_index, fillfactor_table, deduplicate_items, log):
	'''
	create table and index
	'''

	table_cols = ', '.join([(COLUMNS[c] + ' bigint') for c in range(0, columns)])
	index_cols = ', '.join([COLUMNS[c] for c in range(0, columns)])

	with conn.cursor() as c:
		run_sql(wid, did, c, f'drop table if exists t_{wid}', log)
		run_sql(wid, did, c, f'create table t_{wid} ({table_cols}) with (fillfactor = {fillfactor_table})', log)
		run_sql(wid, did, c, f'create index on t_{wid} ({index_cols}) with (fillfactor = {fillfactor_index}, deduplicate_items = {deduplicate_items})', log)
		run_sql(wid, did, c, 'commit', log)

def generate_data(wid, did, conn, columns, rows, seed, fuzz, log = True):
	'''
	generate random-looking data (the primes are picked at random, but
	after that the query itself is deterministic)
	'''

	cols = ', '.join([('(i / ' + str(random.choice(PRIMES)) + ')') for c in range(0, columns)])

	with conn.cursor() as c:
		run_sql(wid, did, c, f'insert into t_{wid} select {cols} from generate_series(1, {rows}) s(i) order by i + mod(i::bigint * {seed}, {fuzz}), md5(i::text)', log)
		run_sql(wid, did, c, 'commit', log)
		run_sql(wid, did, c, f'vacuum freeze t_{wid}', log)
		run_sql(wid, did, c, f'analyze t_{wid}', log)

def copy_data(wid, did, conn_src, conn_dst):
	'''
	copy data from table on src connection to dst connection
	'''

	f = io.StringIO()

	with conn_src.cursor() as src:
		src.copy_to(f, f't_{wid}')

	#print(len(f))
	f.seek(0)

	with conn_dst.cursor() as dst:
		dst.copy_from(f, f't_{wid}')

def declare_cursor(wid, did, conn, columns, modulo, direction, ios, max_batches, log):
	'''
	declare cursor selecting data for a particular value
	'''

	logger = logging.getLogger(f'worker-{wid}-{did}')

	with conn.cursor() as c:
		run_sql(wid, did, c, 'begin', log)
		run_sql(wid, did, c, 'set enable_seqscan = off', log)
		run_sql(wid, did, c, 'set enable_bitmapscan = off', log)
		run_sql(wid, did, c, f'set enable_indexonlyscan = {ios}', log)
		run_sql(wid, did, c, 'set cursor_tuple_fraction = 1.0', log)

		if max_batches:
			run_sql(wid, did, c, f'set index_scan_max_batches = {max_batches}', log)

	c = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

	conds = []
	cols = []

	for x in range(0, columns):

		col = COLUMNS[x]
		cols.append(f'{col} {direction}')

		c.execute(f'select distinct {col} AS v from t_{wid} where mod({col}, {modulo}) = 0 order by {col}')
		values = ','.join([str(r['v']) for r in c.fetchall()])

		conds.append(f'{col} in ({values})')

	s = ' and '.join(conds)
	o = ', '.join(cols)

	c.execute(f'explain select * from t_{wid} where {s} order by {o}')

	for r in c.fetchall():
		logger.info(r['QUERY PLAN'])

	run_sql(wid, did, c, f'declare c_{wid} scroll cursor for select * from t_{wid} where {s} order by {o}', log)

	return c


def close_cursor(wid, did, conn, log):
	'''
	close the declared cursor
	'''

	with conn.cursor() as c:
		run_sql(wid, did, c, f'CLOSE c_{wid}', log)
		run_sql(wid, did, c, 'rollback', log)


def fetch_data(wid, did, conn, cur, direction, count, log):
	'''
	fetch data from the cursor (requested number of rows)
	'''

	# evict data from shared buffers, to force prefetching / look-ahead
	run_sql(wid, did, cur, 'select pg_buffercache_evict_all()', log)

	run_sql(wid, did, cur, f'fetch {direction} {count} from c_{wid}', log)
	return cur.fetchall()


def test_worker(wid):

	logger = logging.getLogger(f'worker-{wid}')

	conn_master = psycopg2.connect(f'host=localhost port={PORT_MASTER} user={USER} dbname=test')
	conn_prefetch = psycopg2.connect(f'host=localhost port={PORT_PREFETCH} user={USER} dbname=test')

	did = 0

	while True:

		did += 1

		logger = logging.getLogger(f'worker-{wid}-{did}')

		seed = random.randint(1, 1000000)
		fuzz = random.randint(1, int(ROWS / 100))
		fill_table = random.randint(10, 100)
		fill_index = random.randint(10, 100)
		dedup = random.choice(['on', 'off'])
		columns = random.randint(1,10)
		modulo = random.randint(1, 10)
		direction = random.choice(['asc', 'desc'])
		ios = random.choice(['on', 'off'])
		max_batches = random.randint(2, 64)

		logger.info(f'PARAMETERS: seed {seed} fuzz {fuzz} modulo {modulo} table fillfactor {fill_table} index fillfactor {fill_index} dedup {dedup} dir {direction} ios {ios} max-batches {max_batches}')

		logger.info('creating table(s)')
		create_table(wid, did, conn_master,   columns, fill_table, fill_index, dedup, True)
		create_table(wid, did, conn_prefetch, columns, fill_table, fill_index, dedup, False)

		logger.info('generating data (master)')
		generate_data(wid, did, conn_master, columns, ROWS, seed, fuzz)

		logger.info('copying data (prefetch)')
		copy_data(wid, did, conn_master, conn_prefetch)

		cur_master = declare_cursor(wid, did, conn_master, columns, modulo, direction, ios, None, False)
		cur_prefetch = declare_cursor(wid, did, conn_prefetch, columns, modulo, direction, ios, max_batches, True)

		# random forward/backwad steps through the data

		idx = 0
		loops = 0
		scan_direction = 'forward'
		scan_pos = 0

		while True:

			idx += 1

			fetch_direction = None
			fetch_count = None

			if loops > LOOPS:
				break

			if scan_direction == 'forward':
				forward_frac = 0.66
			else:
				forward_frac = 0.33

			if random.random() < forward_frac:
				fetch_direction = 'forward'
			else:
				fetch_direction = 'backward'

			fetch_count = random.randint(1, STEP)

			logger.info(f'-- scan pos {scan_pos} dir {scan_direction} frac {forward_frac} fetch {fetch_direction}')

			data_master = fetch_data(wid, did, conn_master, cur_master, fetch_direction, fetch_count, True)
			data_prefetch = fetch_data(wid, did, conn_prefetch, cur_prefetch, fetch_direction, fetch_count, False)

			cnt = len(data_master)
			logger.info(f'-- loop {loops} index {idx} count {cnt}')

			if len(data_prefetch) != len(data_master):
				m = len(data_master)
				p = len(data_prefetch)
				logger.info(f'ERROR: count mismatch master {m} != prefetch {p}')
				exit(1)

			if data_prefetch != data_master:
				logger.info("ERROR: results do not match")
				logger.info("master: ", data_master)
				logger.info("prefetch: ", data_prefetch)
				exit(2)

			fetch_count = len(data_master)

			if fetch_direction == 'forward':
				scan_pos += fetch_count
			else:
				scan_pos -= fetch_count

			if (fetch_count == 0) and (fetch_direction == 'forward') and (scan_direction == 'forward'):
				scan_direction = 'backward'
				loops += 1
			elif (fetch_count == 0) and (fetch_direction == 'backward') and (scan_direction == 'backward'):
				scan_direction = 'forward'
				scan_pos = 0
				loops += 1

		close_cursor(wid, did, conn_master, True)
		close_cursor(wid, did, conn_prefetch, False)

		logger.info("SUCCESS")


if __name__ == '__main__':

	FORMAT = '%(asctime)s\t%(levelname)s\t%(module)s\t%(name)s\t%(message)s'
	logging.basicConfig(level=logging.INFO, format=FORMAT)

	workers = [Process(target=test_worker, args=(i,)) for i in range(0, os.cpu_count())]
	[w.start() for w in workers]
	[w.join() for w in workers]
