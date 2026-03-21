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
LOOPS=10		# number of passes in either direction
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

class BTreeMergeJoinParallelTest(Process):

	def __init__(self, wid):
		self._wid = wid
		super().__init__()

	def run(self):

		logger = logging.getLogger(f'worker-{self._wid}')

		conn_master = psycopg2.connect(f'host=localhost port={PORT_MASTER} user={USER} dbname=test')
		conn_prefetch = psycopg2.connect(f'host=localhost port={PORT_PREFETCH} user={USER} dbname=test')

		did = 0

		while True:

			did += 1

			logger = logging.getLogger(f'worker-{self._wid}-{did}')

			seed = random.randint(1, 1000000)
			fuzz = random.randint(1, int(ROWS / 100))
			fill_table = random.randint(10, 100)
			fill_index = random.randint(10, 100)
			dedup = random.choice(['on', 'off'])
			columns = random.randint(1,10)
			modulo = random.randint(1, 10)
			direction = random.choice(['asc', 'desc'])
			ios = random.choice(['on', 'off'])
			workers = random.randint(0, 4)

			logger.info(f'PARAMETERS: seed {seed} fuzz {fuzz} modulo {modulo} table fillfactor {fill_table} index fillfactor {fill_index} dedup {dedup} dir {direction} ios {ios} workers {workers}')

			logger.info('creating table(s)')
			self._create_table(did, conn_master,   columns, fill_table, fill_index, dedup, workers, True)
			self._create_table(did, conn_prefetch, columns, fill_table, fill_index, dedup, workers, False)

			logger.info('generating data (master)')
			self._generate_data(did, conn_master, columns, ROWS, seed, fuzz)

			logger.info('copying data (prefetch)')
			self._copy_data(did, conn_master, conn_prefetch)

			cur_master = self._declare_cursor(did, conn_master, columns, modulo, direction, ios, workers, False)
			cur_prefetch = self._declare_cursor(did, conn_prefetch, columns, modulo, direction, ios, workers, True)

			# fetch the data
			data_master = self._fetch_data(did, conn_master, cur_master, True)
			data_prefetch = self._fetch_data(did, conn_prefetch, cur_prefetch, False)

			cnt = len(data_master)

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

			logger.info("SUCCESS")

	def _run_sql(self, did, cur, sql, log):
		'''
		log SQL and execute it
		'''

		if log:
			logger = logging.getLogger(f'worker-{self._wid}-{did}')
			logger.info(f'SQL: {sql};')

		cur.execute(sql)


	def _create_table(self, did, conn, columns, fillfactor_index, fillfactor_table, deduplicate_items, workers, log):
		'''
		create table and index
		'''

		table_cols = ', '.join([(COLUMNS[c] + ' bigint') for c in range(0, columns)])
		index_cols = ', '.join([COLUMNS[c] for c in range(0, columns)])

		with conn.cursor() as c:
			self._run_sql(did, c, f'drop table if exists t_{self._wid}_1', log)
			self._run_sql(did, c, f'drop table if exists t_{self._wid}_2', log)
			self._run_sql(did, c, f'create table t_{self._wid}_1 ({table_cols}) with (fillfactor = {fillfactor_table}, parallel_workers = {workers})', log)
			self._run_sql(did, c, f'create table t_{self._wid}_2 ({table_cols}) with (fillfactor = {fillfactor_table}, parallel_workers = {workers})', log)
			self._run_sql(did, c, f'create index on t_{self._wid}_1 ({index_cols}) with (fillfactor = {fillfactor_index}, deduplicate_items = {deduplicate_items})', log)
			self._run_sql(did, c, f'create index on t_{self._wid}_2 ({index_cols}) with (fillfactor = {fillfactor_index}, deduplicate_items = {deduplicate_items})', log)
			self._run_sql(did, c, 'commit', log)


	def _generate_data(self, did, conn, columns, rows, seed, fuzz, log = True):
		'''
		generate random-looking data (the primes are picked at random, but
		after that the query itself is deterministic)
		'''

		cols = ', '.join([('(i / ' + str(random.choice(PRIMES)) + ')') for c in range(0, columns)])

		with conn.cursor() as c:
			self._run_sql(did, c, f'insert into t_{self._wid}_1 select {cols} from generate_series(1, {rows}) s(i) order by i + mod(i::bigint * {seed}, {fuzz}), md5(i::text)', log)
			self._run_sql(did, c, f'insert into t_{self._wid}_2 select {cols} from generate_series(1, {rows}) s(i) order by i + mod(i::bigint * {seed}, {fuzz}), md5(i::text)', log)
			self._run_sql(did, c, 'commit', log)
			self._run_sql(did, c, f'vacuum freeze t_{self._wid}_1', log)
			self._run_sql(did, c, f'vacuum freeze t_{self._wid}_2', log)
			self._run_sql(did, c, f'analyze t_{self._wid}_1', log)
			self._run_sql(did, c, f'analyze t_{self._wid}_2', log)


	def _copy_data(self, did, conn_src, conn_dst):
		'''
		copy data from table on src connection to dst connection
		'''

		f = io.StringIO()

		with conn_src.cursor() as src:
			src.copy_to(f, f't_{self._wid}_1')

		#print(len(f))
		f.seek(0)

		with conn_dst.cursor() as dst:
			dst.copy_from(f, f't_{self._wid}_1')

		f = io.StringIO()

		with conn_src.cursor() as src:
			src.copy_to(f, f't_{self._wid}_2')

		#print(len(f))
		f.seek(0)

		with conn_dst.cursor() as dst:
			dst.copy_from(f, f't_{self._wid}_2')


	def _declare_cursor(self, did, conn, columns, modulo, direction, ios, workers, log):
		'''
		declare cursor selecting data for a particular value
		'''

		logger = logging.getLogger(f'worker-{self._wid}-{did}')

		with conn.cursor() as c:
			self._run_sql(did, c, 'begin', log)
			self._run_sql(did, c, 'set enable_seqscan = off', log)
			self._run_sql(did, c, 'set enable_bitmapscan = off', log)
			self._run_sql(did, c, 'set enable_nestloop = off', log)
			self._run_sql(did, c, 'set enable_hashjoin = off', log)
			self._run_sql(did, c, 'set enable_memoize = off', log)
			self._run_sql(did, c, 'set enable_material = off', log)
			self._run_sql(did, c, f'set enable_indexonlyscan = {ios}', log)
			self._run_sql(did, c, 'set cursor_tuple_fraction = 1.0', log)
			self._run_sql(did, c, 'set enable_sort = off', log)

			# force parallel query
			self._run_sql(did, c, 'set cpu_tuple_cost = 1000', log)
			self._run_sql(did, c, 'set parallel_tuple_cost = 0', log)
			self._run_sql(did, c, 'set parallel_setup_cost = 0', log)
			self._run_sql(did, c, f'set max_parallel_workers_per_gather = {workers}', log)

		c = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

		conds = []
		cols = []

		for x in range(0, columns):

			col = COLUMNS[x]
			cols.append(f'{col}')

		tmp = ', '.join(cols)

		c.execute(f'explain select count(*), bit_xor(crc32(md5((x.*)::text)::bytea)::bit(32)) from (select * from t_{self._wid}_1 join t_{self._wid}_2 using ({tmp}) order by {tmp}) x')

		for r in c.fetchall():
			logger.info(r['QUERY PLAN'])

		# evict data from shared buffers, to force prefetching / look-ahead
		self._run_sql(did, c, 'select pg_buffercache_evict_all()', log)

		self._run_sql(did, c, f'select count(*), bit_xor(crc32(md5((x.*)::text)::bytea)::bit(32)) from (select * from t_{self._wid}_1 join t_{self._wid}_2 using ({tmp}) order by {tmp}) x', log)

		return c


	def _fetch_data(self, did, conn, cur, log):
		'''
		fetch data from the cursor (requested number of rows)
		'''

		return cur.fetchall()
