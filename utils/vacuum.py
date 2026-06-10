#
# randomly vacuum the whole database
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

class VacuumWorker(Process):

	def __init__(self):
		super().__init__()

	def run(self):

		logger = logging.getLogger(f'vacuum')

		conn_master = psycopg2.connect(f'host=localhost port={PORT_MASTER} user={USER} dbname=test')
		conn_master.autocommit = True

		conn_prefetch = psycopg2.connect(f'host=localhost port={PORT_PREFETCH} user={USER} dbname=test')
		conn_prefetch.autocommit = True

		while True:

			logger.info('vacuum master')

			with conn_master.cursor() as cur:
				cur.execute('vacuum')
				cur.close()

			logger.info('vacuum patched')

			with conn_prefetch.cursor() as cur:
				cur.execute('vacuum')
				cur.close()

			# sleep for 1-10 seconds
			time.sleep(1.0 + random.random() * 9.0)
