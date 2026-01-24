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

class BTreeIncrementalTest(Process):

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
			direction = random.choice(['asc', 'desc'])
			ios = random.choice(['on', 'off'])

			logger.info(f'PARAMETERS: seed {seed} fuzz {fuzz} table fillfactor {fill_table} index fillfactor {fill_index} dedup {dedup} dir {direction} ios {ios}')

			logger.info('creating table(s)')
			self._create_table(did, conn_master,   columns, fill_table, fill_index, dedup, True)
			self._create_table(did, conn_prefetch, columns, fill_table, fill_index, dedup, False)

			logger.info('generating data (master)')
			self._generate_data(did, conn_master, columns, ROWS, seed, fuzz)

			logger.info('copying data (prefetch)')
			self._copy_data(did, conn_master, conn_prefetch)

			cur_master = self._declare_cursor(did, conn_master, columns, direction, ios, False)
			cur_prefetch = self._declare_cursor(did, conn_prefetch, columns, direction, ios, True)

			# scroll futher and further to the data set, and back

			# XXX maybe we should do the scroll in smaller steps? that way there
			# will be evictions during the scroll
			for fetch_count in range(1, (ROWS + 2)):

				# forward fetch
				data_master = self._fetch_data(did, conn_master, cur_master, 'forward', fetch_count, True)
				data_prefetch = self._fetch_data(did, conn_prefetch, cur_prefetch, 'forward', fetch_count, False)

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
				data_master = self._fetch_data(did, conn_master, cur_master, 'backward', fetch_count, True)
				data_prefetch = self._fetch_data(did, conn_prefetch, cur_prefetch, 'backward', fetch_count, False)

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

			self._close_cursor(did, conn_master, True)
			self._close_cursor(did, conn_prefetch, False)

			logger.info("SUCCESS")


	if __name__ == '__main__':

		FORMAT = '%(asctime)s\t%(levelname)s\t%(module)s\t%(name)s\t%(message)s'
		logging.basicConfig(level=logging.INFO, format=FORMAT)

		workers = [Process(target=test_worker, args=(i,)) for i in range(0, os.cpu_count())]
		[w.start() for w in workers]
		[w.join() for w in workers]


	def _run_sql(self, did, cur, sql, log):
		'''
		log SQL and execute it
		'''

		if log:
			logger = logging.getLogger(f'worker-{self._wid}-{did}')
			logger.info(f'SQL: {sql};')

		cur.execute(sql)


	def _create_table(self, did, conn, columns, fillfactor_index, fillfactor_table, deduplicate_items, log):
		'''
		create table and index
		'''

		table_cols = ', '.join([(COLUMNS[c] + ' bigint') for c in range(0, columns)])
		index_cols = ', '.join([COLUMNS[c] for c in range(0, columns)])

		with conn.cursor() as c:
			self._run_sql(did, c, f'drop table if exists t_{self._wid}', log)
			self._run_sql(did, c, f'create table t_{self._wid} ({table_cols}) with (fillfactor = {fillfactor_table})', log)
			self._run_sql(did, c, f'create index on t_{self._wid} ({index_cols}) with (fillfactor = {fillfactor_index}, deduplicate_items = {deduplicate_items})', log)
			self._run_sql(did, c, 'commit', log)


	def _generate_data(self, did, conn, columns, rows, seed, fuzz, log = True):
		'''
		generate random-looking data (the primes are picked at random, but
		after that the query itself is deterministic)
		'''

		cols = ', '.join([('(i / ' + str(random.choice(PRIMES)) + ')') for c in range(0, columns)])

		with conn.cursor() as c:
			self._run_sql(did, c, f'insert into t_{self._wid} select {cols} from generate_series(1, {rows}) s(i) order by i + mod(i::bigint * {seed}, {fuzz}), md5(i::text)', log)
			self._run_sql(did, c, 'commit', log)
			self._run_sql(did, c, f'vacuum freeze t_{self._wid}', log)
			self._run_sql(did, c, f'analyze t_{self._wid}', log)


	def _copy_data(self, did, conn_src, conn_dst):
		'''
		copy data from table on src connection to dst connection
		'''

		f = io.StringIO()

		with conn_src.cursor() as src:
			src.copy_to(f, f't_{self._wid}')

		#print(len(f))
		f.seek(0)

		with conn_dst.cursor() as dst:
			dst.copy_from(f, f't_{self._wid}')


	def _declare_cursor(self, did, conn, columns, direction, ios, log):
		'''
		declare cursor selecting data for a particular value
		'''

		logger = logging.getLogger(f'worker-{self._wid}-{did}')

		with conn.cursor() as c:
			self._run_sql(did, c, 'begin', log)
			self._run_sql(did, c, 'set enable_seqscan = off', log)
			self._run_sql(did, c, 'set enable_bitmapscan = off', log)
			self._run_sql(did, c, f'set enable_indexonlyscan = {ios}', log)
			self._run_sql(did, c, 'set cursor_tuple_fraction = 1.0', log)

		c = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

		cols = [(COLUMNS[x] + ' ' +  direction) for x in range(0, columns)]
		cols_sql = ', '.join(cols)

		c.execute(f'explain select * from t_{self._wid} order by {cols_sql}')
		for r in c.fetchall():
			logger.info(r['QUERY PLAN'])

		self._run_sql(did, c, f'declare c_{self._wid} scroll cursor for select * from t_{self._wid} order by {cols_sql}', log)

		return c


	def _close_cursor(self, did, conn, log):
		'''
		close the declared cursor
		'''

		with conn.cursor() as c:
			self._run_sql(did, c, f'CLOSE c_{self._wid}', log)
			self._run_sql(did, c, 'rollback', log)


	def _fetch_data(self, did, conn, cur, direction, count, log):
		'''
		fetch data from the cursor (requested number of rows)
		'''

		# evict data from shared buffers, to force prefetching / look-ahead
		self._run_sql(did, cur, 'select pg_buffercache_evict_all()', log)

		self._run_sql(did, cur, f'fetch {direction} {count} from c_{self._wid}', log)
		return cur.fetchall()
