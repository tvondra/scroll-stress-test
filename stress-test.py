#!/usr/bin/env python3

import io
import logging
from multiprocessing import Process
import psycopg2
import psycopg2.extras
import random
import time

USER=os.getlogin()
PORT_MASTER=5001
PORT_PREFETCH=5002

ROWS=100000
LOOPS=100
STEP=100

# up to 10 columns
COLUMNS = ['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j']

PRIMES = [19, 23, 29, 31, 37, 41, 43, 47, 53, 59, 61, 67, 71, 73, 79,
		  83, 89, 97, 101, 103, 107, 109, 113, 127, 131, 137, 139, 149,
		  151, 157, 163, 167, 173, 179, 181, 191, 193, 197, 199, 211,
		  223, 227, 229, 233, 239, 241, 251, 257, 263, 269, 271, 277,
		  281, 283, 293, 307, 311, 313, 317, 331, 337, 347, 349, 353,
		  359, 367, 373, 379, 383, 389, 397, 401, 409, 419, 421, 431,
		  433, 439, 443, 449, 457, 461, 463, 467, 479, 487, 491, 499,
		  503, 509, 521, 523, 541]

def create_table(conn, wid, columns, fillfactor_index, fillfactor_table, deduplicate_items):

	logger = logging.getLogger(f'worker-{wid}')

	table_cols = ', '.join([(COLUMNS[c] + ' bigint') for c in range(0, columns)])
	index_cols = ', '.join([COLUMNS[c] for c in range(0, columns)])

	with conn.cursor() as c:

		logger.info(f'SQL: drop table if exists t_{wid};')
		c.execute(f'drop table if exists t_{wid}')

		#c.execute(f'create table t (a bigint, b bigint) with (fillfactor = {fillfactor_table})')
		#c.execute(f'create index on t (a) with (fillfactor = {fillfactor_table}, deduplicate_items = {deduplicate_items})')
		#c.execute(f'create table t (a bigint, b bigint, c bigint, d bigint, e bigint, f bigint) with (fillfactor = {fillfactor_table})')
		#c.execute(f'create table t (a bigint, b bigint, c bigint, d bigint, e bigint, f bigint) with (fillfactor = {fillfactor_table})')
		#c.execute(f'create index on t (a, b, c, d, e, f) with (fillfactor = {fillfactor_table}, deduplicate_items = {deduplicate_items})')

		logger.info(f'SQL: create table t_{wid} ({table_cols}) with (fillfactor = {fillfactor_table});')
		c.execute(f'create table t_{wid} ({table_cols}) with (fillfactor = {fillfactor_table})')

		logger.info(f'SQL: create index on t_{wid} ({index_cols}) with (fillfactor = {fillfactor_index}, deduplicate_items = {deduplicate_items});')
		c.execute(f'create index on t_{wid} ({index_cols}) with (fillfactor = {fillfactor_index}, deduplicate_items = {deduplicate_items})')

		c.execute('commit')

def generate_data(conn, wid, columns, rows, seed, fuzz):

	logger = logging.getLogger(f'worker-{wid}')

	cols = ', '.join([('(i / ' + str(random.choice(PRIMES)) + ')') for c in range(0, columns)])

	with conn.cursor() as c:
		#c.execute(f'insert into t select i, i from generate_series(1, {rows}) s(i) order by i + mod(i::bigint * {seed}, {fuzz}), md5(i::text)')
		#c.execute(f'insert into t select (i/113), (i/131), (i/149), (i/157), (i/167), (i/173) from generate_series(1, {rows}) s(i) order by i + mod(i::bigint * {seed}, {fuzz}), md5(i::text)')

		logger.info(f'SQL: insert into t_{wid} select {cols} from generate_series(1, {rows}) s(i) order by i + mod(i::bigint * {seed}, {fuzz}), md5(i::text);')
		c.execute(f'insert into t_{wid} select {cols} from generate_series(1, {rows}) s(i) order by i + mod(i::bigint * {seed}, {fuzz}), md5(i::text)')

		logger.info('SQL: commit;')
		c.execute('commit')

		logger.info(f'vacuum freeze t_{wid};')
		c.execute(f'vacuum freeze t_{wid}')

		logger.info(f'analyze t_{wid};')
		c.execute(f'analyze t_{wid}')

def copy_data(conn_src, conn_dst, wid):

	f = io.StringIO()

	with conn_src.cursor() as src:
		src.copy_to(f, f't_{wid}')

	#print(len(f))
	f.seek(0)

	with conn_dst.cursor() as dst:
		dst.copy_from(f, f't_{wid}')

def declare_cursor(conn, wid, columns, modulo):

	logger = logging.getLogger(f'worker-{wid}')

	with conn.cursor() as c:
		logger.info('SQL: begin;')
		c.execute('begin')

		logger.info('SQL: set enable_seqscan = off;')
		c.execute('set enable_seqscan = off')

		logger.info('SQL: set enable_bitmapscan = off;')
		c.execute('set enable_bitmapscan = off')

		logger.info('SQL: set enable_indexonlyscan = off;')
		c.execute('set enable_indexonlyscan = off')

		logger.info('SQL: set cursor_tuple_fraction = 1.0;')
		c.execute('set cursor_tuple_fraction = 1.0')

	c = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

	conds = []
	cols = []

	for x in range(0, columns):

		col = COLUMNS[x]

		cols.append(col)

		c.execute(f'select distinct {col} AS v from t_{wid} where mod({col}, {modulo}) = 0 order by {col}')
		values = ','.join([str(r['v']) for r in c.fetchall()])

		conds.append(f'{col} in ({values})')

	s = ' and '.join(conds)
	o = ', '.join(cols)

	c.execute(f'explain select * from t_{wid} where {s} order by {o}')

	for r in c.fetchall():
		logger.info(r['QUERY PLAN'])

	logger.info(f'SQL: declare c_{wid} scroll cursor for select * from t_{wid} where {s} order by {o};')
	c.execute(f'declare c_{wid} scroll cursor for select * from t_{wid} where {s} order by {o}')

	return c


def close_cursor(conn, wid):

	with conn.cursor() as c:
		c.execute(f'CLOSE c_{wid}')


def fetch_data(conn, wid, cur, direction, count):

	# evict data from shared buffers, to force prefetching / look-ahead
	with conn.cursor() as c:
		c.execute('select pg_buffercache_evict_all()')

	cur.execute(f'fetch {direction} {count} from c_{wid}')
	return cur.fetchall()


def test_worker(wid):

	logger = logging.getLogger(f'worker-{wid}')

	conn_master = psycopg2.connect(f'host=localhost port={PORT_MASTER} user={USER} dbname=test')
	conn_prefetch = psycopg2.connect(f'host=localhost port={PORT_PREFETCH} user={USER} dbname=test')

	while True:

		seed = random.randint(1, 1000000)
		fuzz = random.randint(1, int(ROWS / 100))
		fill_table = random.randint(10, 100)
		fill_index = random.randint(10, 100)
		dedup = random.choice(['on', 'off'])
		columns = random.randint(1,10)
		modulo = random.randint(1, 10)

		logger.info(f'PARAMETERS: seed {seed} fuzz {fuzz} modulo {modulo} table fillfactor {fill_table} index fillfactor {fill_index} dedup {dedup}')

		logger.info('creating table(s)')
		create_table(conn_master,   wid, columns, fill_table, fill_index, dedup)
		create_table(conn_prefetch, wid, columns, fill_table, fill_index, dedup)

		logger.info('generating data (master)')
		generate_data(conn_master, wid, columns, ROWS, seed, fuzz)

		logger.info('copying data (prefetch)')
		copy_data(conn_master, conn_prefetch, wid)

		cur_master = declare_cursor(conn_master, wid, columns, modulo)
		cur_prefetch = declare_cursor(conn_prefetch, wid, columns, modulo)

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

			logger.info(f'SQL: fetch {fetch_direction} {fetch_count} from c_{wid};')

			data_master = fetch_data(conn_master, wid, cur_master, fetch_direction, fetch_count)
			data_prefetch = fetch_data(conn_prefetch, wid, cur_prefetch, fetch_direction, fetch_count)

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

		close_cursor(conn_master, wid)
		close_cursor(conn_prefetch, wid)

		logger.info("SUCCESS")


if __name__ == '__main__':

	logging.basicConfig(level=logging.INFO)

	logger = logging.getLogger(__name__)

	workers = [Process(target=test_worker, args=(i,)) for i in range(1,8)]
	[w.start() for w in workers]
	[w.join() for w in workers]

