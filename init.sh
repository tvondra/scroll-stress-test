#!/usr/bin/env bash

killall -9 postgres
rm -Rf data*

~/builds/master/bin/pg_ctl -D data-master init
~/builds/patched/bin/pg_ctl -D data-patched init

echo 'port = 5001' >> data-master/postgresql.conf
echo 'port = 5002' >> data-patched/postgresql.conf

echo 'restart_after_crash = on' >> data-master/postgresql.conf
echo 'restart_after_crash = on' >> data-patched/postgresql.conf

~/builds/master/bin/pg_ctl -D data-master -l pg-master.log start
~/builds/patched/bin/pg_ctl -D data-patched -l pg-patched.log start

~/builds/master/bin/createdb -p 5001 test
~/builds/patched/bin/createdb -p 5002 test

~/builds/master/bin/psql -p 5001 test -c "create extension pg_buffercache"
~/builds/patched/bin/psql -p 5002 test -c "create extension pg_buffercache"
