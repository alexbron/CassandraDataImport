CREATE KEYSPACE lightspeed WITH REPLICATION = { 'class' : 'NetworkTopologyStrategy',  'Cassandra' : 3 };

create table lightspeed.analytics_dev_active_users_hourly
(
	partner_filter_gb1_gb2 text,
dt_gb1_gb2 text,
dim_gb1 text,
dim_gb2 text,
f text,
m1 text,
m2 text,
m3 text ,
m4 text,
m5 text,
m6 text	,
PRIMARY KEY (partner_filter_gb1_gb2, dt_gb1_gb2)
) with compaction = { 'class' : 'LeveledCompactionStrategy' };


create table lightspeed.analytics_dev_funnel_hourly
(
	partner_filter_gb1_gb2 text,
	dt_gb1_gb2 text,
	dim_gb1 text,
	dim_gb2 text,
	f text,
	m1 text,
	m2 text,
	m3 text ,
	m4 text,
	m5 text,
	m6 text	,
	PRIMARY KEY (partner_filter_gb1_gb2, dt_gb1_gb2)
) with compaction = { 'class' : 'LeveledCompactionStrategy' };

create table lightspeed.analytics_dev_funnel_daily
(
	partner_filter_gb1_gb2 text,
	dt_gb1_gb2 text,
	dim_gb1 text,
	dim_gb2 text,
	f text,
	m1 text,
	m2 text,
	m3 text ,
	m4 text,
	m5 text,
	m6 text	,
	PRIMARY KEY (partner_filter_gb1_gb2, dt_gb1_gb2)
) with compaction = { 'class' : 'LeveledCompactionStrategy' };

create table lightspeed. analytics_dev_maxtimestamps1
(
	report  text,
	maxtimestamp text,
	PRIMARY KEY (report)
);
