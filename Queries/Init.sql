CREATE KEYSPACE lightspeed WITH REPLICATION = { 'class' : 'NetworkTopologyStrategy',  'AMZ_EAST' : 3 };

create table lightspeed.analytics_stg_active_users_hourly
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



create table lightspeed.analytics_stg_usage_daily
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
	m7 int,
	m8 int,
	m9 int ,
	m10 int,
	m11 int,
	m12 int	,
	m13 int,
	m14 int,
	PRIMARY KEY (partner_filter_gb1_gb2, dt_gb1_gb2)
) with compaction = { 'class' : 'LeveledCompactionStrategy' };

create table lightspeed.analytics_stg_active_users_daily
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


create table lightspeed.analytics_stg_usage_hourly
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

create table lightspeed.analytics_stg_usage_daily
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
	m7 int,
	m8 int,
	m9 int ,
	m10 int,
	m11 int,
	m12 int,
	m13 int,
	m14 int,
	PRIMARY KEY (partner_filter_gb1_gb2, dt_gb1_gb2)
) with compaction = { 'class' : 'LeveledCompactionStrategy' };

create table lightspeed.analytics_stg_external_data_daily
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
	m6 int,
	PRIMARY KEY (partner_filter_gb1_gb2, dt_gb1_gb2)
) with compaction = { 'class' : 'LeveledCompactionStrategy' };

create table lightspeed.analytics_stg_au_cohort_total_daily
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



create table lightspeed.analytics_stg_funnel_daily
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


create table lightspeed.analytics_stg_funnel_hourly
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



create table lightspeed. analytics_stg_maxtimestamps
(
	report  text,
	maxtimestamp text,
	PRIMARY KEY (report)
);
