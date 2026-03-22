#!/bin/bash

cat << EOF > /tmp/build_it.sql
\c postgres
create role dagster with login password 'dagster';
create database dagster with owner dagster;
create role analytics with login password 'passw0rd';
create database analytics with owner analytics;
EOF

psql -U postgres -f /tmp/build_it.sql

cat << EOF > /tmp/build_it.sql
create schema dagster;
EOF

psql -U dagster -f /tmp/build_it.sql

cat << EOF > /tmp/build_it.sql
create schema analytics;
drop schema public;
EOF
psql -U analytics -f /tmp/build_it.sql

cat << EOF > /tmp/build_it.sql
create table if not exists nypd_arrests (
	arrest_key						bigint		not null,
	create_dt						timestamptz not null default current_timestamp,
	update_dt						timestamptz not null default current_timestamp,
	arrest_date						date		not null,
	arrest_code						int			not null,
	arrest_code_description			text,
	arrest_class_code				bigint,
	arrest_class_code_description	text,
	law_code						text,
	law_code_description			text,
	borough_code					char,
	precinct						int			not null,
	jurisdiction_code				int			not null,
	age_group						int4range,
	gender							char,
	race							text		not null,
	location						point		not null,
	primary key (arrest_key)
);
EOF

psql -U analytics -f /tmp/build_it.sql
