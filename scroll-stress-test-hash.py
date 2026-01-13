#!/usr/bin/env python3
#
# randomized stress test of scrollable cursor with hash index scans
#
# 1. picks random parameters for a run (seed, fuzz, fillfactors, ios, ...)
# 2. generates a table (1-10 columns) with pseudo-random data (seed+fuzz)
# 3. creates a b-tree index on the table
# 4. declares a scrollable cursor with a random condition (random value)
# 5. scrolls further and further in the cursor and back to start
# 6. after each fetch compares results from master and patched connection
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

ROWS=100000
LOOPS=1000
STEP=100		# largest allowed step

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

def create_table(wid, did, conn, fillfactor_index, fillfactor_table, log):
	'''
	create table and index
	'''

	with conn.cursor() as c:
		run_sql(wid, did, c, f'drop table if exists t_{wid}', log)
		run_sql(wid, did, c, f'create table t_{wid} (a bigint) with (fillfactor = {fillfactor_table})', log)
		run_sql(wid, did, c, f'create index on t_{wid} using hash (a) with (fillfactor = {fillfactor_index})', log)
		run_sql(wid, did, c, 'commit')

def generate_data(wid, did, conn, rows, seed, fuzz, log = True):
	'''
	generate random-looking data (the primes are picked at random, but
	after that the query itself is deterministic)
	'''

	col = '(i / ' + str(random.choice(PRIMES)) + ')'

	with conn.cursor() as c:
		run_sql(wid, did, c, f'insert into t_{wid} select {col} from generate_series(1, {rows}) s(i) order by i + mod(i::bigint * {seed}, {fuzz}), md5(i::text)', log)
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

def get_distinct_values(wid, did, cur):
	'''
	determine distinct values for each column
	'''
	cur.execute(f'select distinct a from t_{wid}')
	return [v[0] for v in cur.fetchall()]

def count_rows(wid, did, conn, param):
	'''
	count all rows returned by the query
	'''
	with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as c:
		c.execute(f'select count(*) as cnt from t_{wid} where a = {param}')
		return c.fetchone()['cnt']


def declare_cursor(wid, did, conn, param, max_batches, log):
	'''
	declare cursor selecting data for a particular value
	'''

	logger = logging.getLogger(f'worker-{wid}-{did}')

	c = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

	run_sql(wid, did, c, 'begin', log)
	run_sql(wid, did, c, 'set enable_seqscan = off', log)
	run_sql(wid, did, c, 'set enable_bitmapscan = off', log)
	run_sql(wid, did, c, 'set cursor_tuple_fraction = 1.0', log)

	if max_batches:
		run_sql(wid, did, c, f'set index_scan_max_batches = {max_batches}', log)

	conds = []		# WHERE conditions

	# print explain of the constructed query
	c.execute(f'explain select * from t_{wid} where a = {param}')
	for r in c.fetchall():
		logger.info(r['QUERY PLAN'])

	# actually declare the cursor
	run_sql(wid, did, c, f'declare c_{wid} scroll cursor for select * from t_{wid} where a = {param}', log)

	return c


def close_cursor(wid, did, conn, log):
	'''
	close the declared cursor
	'''

	with conn.cursor() as c:
		run_sql(wid, did, c, f'CLOSE c_{wid}', log)
		run_sql(wid, did, c, 'rollback', log)


def fetch_data(wid, did, conn, cur, direction, cnt, log):
	'''
	fetch a row from the data
	'''

	# evict data from shared buffers, to force prefetching / look-ahead
	run_sql(wid, did, cur, 'select pg_buffercache_evict_all()', log)

	run_sql(wid, did, cur, f'fetch {direction} {cnt} from c_{wid}', log)
	return cur.fetchall()


def test_worker(wid):

	conn_master = psycopg2.connect(f'host=localhost port={PORT_MASTER} user={USER} dbname=test')
	conn_prefetch = psycopg2.connect(f'host=localhost port={PORT_PREFETCH} user={USER} dbname=test')

	did = 0

	while True:

		did += 1

		logger = logging.getLogger(f'worker-{wid}-{did}')

		seed = random.randint(1, 1000000)
		fuzz = random.randint(0, int(ROWS / 100))
		fill_table = random.randint(10, 100)
		fill_index = random.randint(10, 100)
		max_batches = random.randint(2, 64)

		# step affects how often we evict data (which may affect prefetching)
		step = random.randint(1, STEP)

		logger.info(f'PARAMETERS: did {did} seed {seed} fuzz {fuzz} table fillfactor {fill_table} index fillfactor {fill_index} step {step} max-batches {max_batches}')

		logger.info('creating table(s)')
		create_table(wid, did, conn_master,   fill_table, fill_index, True)
		create_table(wid, did, conn_prefetch, fill_table, fill_index, False)

		logger.info('generating data (master)')
		generate_data(wid, did, conn_master, ROWS, seed, fuzz)

		logger.info('copying data (prefetch)')
		copy_data(wid, did, conn_master, conn_prefetch)

		logger.info('determining distinct values for the column')
		values = get_distinct_values(wid, did, conn_master.cursor())

		l = len(values)
		logger.info(f'column distinct values {l}')

		# shuffle values
		random.shuffle(values)

		# query random combinations of column values
		for loop in range(0, LOOPS):

			# there might not be enough values
			if loop >= len(values):
				break

			param = values[loop]

			total_cnt = count_rows(wid, did, conn_master, param)

			cur_master = declare_cursor(wid, did, conn_master, param, None, True)
			cur_prefetch = declare_cursor(wid, did, conn_prefetch, param, max_batches, False)

			logger.info(f'total rows {total_cnt}')

			# fetch more and more rows, up to the whole data set (but that's unlikely)
			for count in range(1, (total_cnt + 2)):

				logger.info(f'scrolling to {count}')

				# fetch row by row in lockstep from both connections (forward)
				logger.info(f'scroll forward')
				steps = count
				while (steps > 0):

					cnt = min(step, steps)
					steps -= cnt

					data_master = fetch_data(wid, did, conn_master, cur_master, 'forward', cnt, True)
					data_prefetch = fetch_data(wid, did, conn_prefetch, cur_prefetch, 'forward', cnt, False)

					if data_master != data_prefetch:
						logger.info(f'ERROR: row mismatch master {data_master} != prefetch {data_prefetch}')
						exit(1)

				# fetch row by row in lockstep from both connections (backward)
				logger.info(f'scroll backward')
				steps = count
				while (steps > 0):

					cnt = min(step, steps)
					steps -= cnt

					data_master = fetch_data(wid, did, conn_master, cur_master, 'backward', cnt, True)
					data_prefetch = fetch_data(wid, did, conn_prefetch, cur_prefetch, 'backward', cnt, False)

					if data_master != data_prefetch:
						logger.info(f'ERROR: row mismatch master {data_master} != prefetch {data_prefetch}')
						exit(1)

			close_cursor(wid, did, conn_master, True)
			close_cursor(wid, did, conn_prefetch, False)

		logger.info("SUCCESS")


if __name__ == '__main__':

	FORMAT = '%(asctime)s\t%(levelname)s\t%(module)s\t%(name)s\t%(message)s'
	logging.basicConfig(level=logging.INFO, format=FORMAT)

	workers = [Process(target=test_worker, args=(i,)) for i in range(0,os.cpu_count())]
	[w.start() for w in workers]
	[w.join() for w in workers]
