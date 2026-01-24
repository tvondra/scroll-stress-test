#!/usr/bin/env python3
#
# randomized stress test of scrollable cursor with b-tree index scans
#
# 1. picks random parameters for a run (seed, fuzz, fillfactors, ios, ...)
# 2. generates a table (1-10 columns) with pseudo-random data (seed+fuzz)
# 3. creates a b-tree index on the table
# 4. declares a scrollable cursor with random SAOP conditions
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

ROWS=1000

# up to 10 columns
COLUMNS = ['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j']

# primes used to generate 'random' data
PRIMES = [2, 3, 5, 7, 11, 13, 17, 19, 23, 29, 31, 37, 41, 43, 47, 53, 59, 61, 67, 71]

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

def declare_cursor(wid, did, conn, columns, direction, ios, log):
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

	c = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

	cols = [(COLUMNS[x] + ' ' +  direction) for x in range(0, columns)]
	cols_sql = ', '.join(cols)

	c.execute(f'explain select * from t_{wid} order by {cols_sql}')
	for r in c.fetchall():
		logger.info(r['QUERY PLAN'])

	run_sql(wid, did, c, f'declare c_{wid} scroll cursor for select * from t_{wid} order by {cols_sql}', log)

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
		direction = random.choice(['asc', 'desc'])
		ios = random.choice(['on', 'off'])
		max_batches = random.randint(2, 64)

		logger.info(f'PARAMETERS: seed {seed} fuzz {fuzz} table fillfactor {fill_table} index fillfactor {fill_index} dedup {dedup} dir {direction} ios {ios}')

		logger.info('creating table(s)')
		create_table(wid, did, conn_master,   columns, fill_table, fill_index, dedup, True)
		create_table(wid, did, conn_prefetch, columns, fill_table, fill_index, dedup, False)

		logger.info('generating data (master)')
		generate_data(wid, did, conn_master, columns, ROWS, seed, fuzz)

		logger.info('copying data (prefetch)')
		copy_data(wid, did, conn_master, conn_prefetch)

		cur_master = declare_cursor(wid, did, conn_master, columns, direction, ios, False)
		cur_prefetch = declare_cursor(wid, did, conn_prefetch, columns, direction, ios, True)

		# scroll futher and further to the data set, and back

		# XXX maybe we should do the scroll in smaller steps? that way there
		# will be evictions during the scroll
		for fetch_count in range(1, (ROWS + 2)):

			# forward fetch
			data_master = fetch_data(wid, did, conn_master, cur_master, 'forward', fetch_count, True)
			data_prefetch = fetch_data(wid, did, conn_prefetch, cur_prefetch, 'forward', fetch_count, False)

			cnt = len(data_master)
			logger.info(f'-- forward fetch {fetch_count} count {cnt}')

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

			# backward fetch
			data_master = fetch_data(wid, did, conn_master, cur_master, 'backward', fetch_count, True)
			data_prefetch = fetch_data(wid, did, conn_prefetch, cur_prefetch, 'backward', fetch_count, False)

			cnt = len(data_master)
			logger.info(f'-- backward fetch {fetch_count} count {cnt}')

			if len(data_prefetch) != len(data_master):
				m = len(data_master)
				p = len(data_prefetch)
				logger.info(f'ERROR: count mismatch master {m} != prefetch {p}')
				exit(3)

			if data_prefetch != data_master:
				logger.info("ERROR: results do not match")
				logger.info("master: ", data_master)
				logger.info("prefetch: ", data_prefetch)
				exit(4)

		close_cursor(wid, did, conn_master, True)
		close_cursor(wid, did, conn_prefetch, False)

		logger.info("SUCCESS")


if __name__ == '__main__':

	FORMAT = '%(asctime)s\t%(levelname)s\t%(module)s\t%(name)s\t%(message)s'
	logging.basicConfig(level=logging.INFO, format=FORMAT)

	workers = [Process(target=test_worker, args=(i,)) for i in range(0, os.cpu_count())]
	[w.start() for w in workers]
	[w.join() for w in workers]

