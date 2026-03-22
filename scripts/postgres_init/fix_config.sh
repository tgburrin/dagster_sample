#!/bin/bash

MAX_CONNECTIONS=1000

cd /var/lib/postgresql/data || exit 1
sed -i "s/^max_connections = [0-9]\+\(\s\+.*\)/max_connections = $MAX_CONNECTIONS\1/g" postgresql.conf
sed -i 's/#fsync = on\(.*\)/fsync = off\1/' postgresql.conf
sed -i 's/^shared_buffers = 128MB\(.*\)/shared_buffers = 4GB\1/' postgresql.conf
sed -i 's/^#work_mem = 4MB\(.*\)/work_mem = 1GB\1/' postgresql.conf
sed -i 's/^#temp_buffers = 8MB\(.*\)/temp_buffers = 256MB\1/' postgresql.conf
sed -i 's/^#max_locks_per_transaction = 64\(.*\)/max_locks_per_transaction = 1024\1/' postgresql.conf
#sed -i 's/^#autovacuum = on\(.*\)/autovacuum = off\1/' postgresql.conf
sed -i 's/^#autovacuum_vacuum_threshold = 50\(.*\)/autovacuum_vacuum_threshold = 1000000\1/' postgresql.conf
sed -i 's/^#autovacuum_vacuum_scale_factor = 0.2\(.*\)/autovacuum_vacuum_scale_factor = 0.5\1/' postgresql.conf
sed -i 's/^#full_page_writes = on\(.*\)/full_page_writes = off\1/' postgresql.conf
sed -i 's/^#synchronous_commit = on\(.*\)/synchronous_commit = off\1/' postgresql.conf
sed -i 's/^#wal_writer_flush_after = 1MB\(.*\)/wal_writer_flush_after = 0\1/' postgresql.conf
sed -i 's/^#wal_writer_delay = 200ms\(.*\)/wal_writer_delay = 10000ms\1/' postgresql.conf
sed -i 's/^#checkpoint_flush_after = 256kB\(.*\)/checkpoint_flush_after = 0\1/' postgresql.conf
sed -i 's/^#checkpoint_timeout = 5min\(.*\)/checkpoint_timeout = 60min\1/' postgresql.conf
#sed -i 's/^#stats_temp_directory = .*/stats_temp_directory = \x27\/pg_stat_tmp\x27/' postgresql.conf
