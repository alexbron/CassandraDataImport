CREATE KEYSPACE lightspeed WITH REPLICATION = { 'class' : 'NetworkTopologyStrategy',  'AMZ_EAST' : 3 };

create table lightspeed.analytics_dev_active_users_hourly
(
	partner_filter_gb1_gb2 text,
dt_gb1_gb2 text,
dim_gb1 text,
dim_gb2 text,
f text,
m1 int,
m2 int,
m3 int ,
m4 int,
m5 int,
m6 int	,
PRIMARY KEY (partner_filter_gb1_gb2, dt_gb1_gb2)
) with compaction = { 'class' : 'LeveledCompactionStrategy' };


create table lightspeed.analytics_dev_funnel_hourly
(
	partner_filter_gb1_gb2 text,
	dt_gb1_gb2 text,
	dim_gb1 text,
	dim_gb2 text,
	f text,
	m1 int,
	m2 int,
	m3 int ,
	m4 int,
	m5 int,
	m6 int	,
	PRIMARY KEY (partner_filter_gb1_gb2, dt_gb1_gb2)
) with compaction = { 'class' : 'LeveledCompactionStrategy' };

create table lightspeed.analytics_dev_funnel_daily
(
	partner_filter_gb1_gb2 text,
	dt_gb1_gb2 text,
	dim_gb1 text,
	dim_gb2 text,
	f text,
	m1 int,
	m2 int,
	m3 int ,
	m4 int,
	m5 int,
	m6 int	,
	PRIMARY KEY (partner_filter_gb1_gb2, dt_gb1_gb2)
) with compaction = { 'class' : 'LeveledCompactionStrategy' };

create table lightspeed. analytics_dev_maxtimestamps1
(
	report  text,
	maxtimestamp text,
	PRIMARY KEY (report)
);
