-- Databricks notebook source
-- DBTITLE 1,create database
create database testdb

-- COMMAND ----------

-- DBTITLE 1,show databases
show databases

-- COMMAND ----------

-- DBTITLE 1,describe database
describe database testdb

-- COMMAND ----------

-- DBTITLE 1,use database
use database testdb

-- COMMAND ----------

-- DBTITLE 1,list database folder
-- MAGIC %fs ls 'dbfs:/user/hive/warehouse/testdb.db/'

-- COMMAND ----------

-- MAGIC %fs rm -r 'dbfs:/user/hive/warehouse/testdb.db'

-- COMMAND ----------

select current_user()

-- COMMAND ----------

create or replace table testtb (
  t1 varchar(3), 
  n1 integer
) USING DELTA CLUSTER BY (t1)
-- location 'dbfs:/user/hive/warehouse/testdb.db/testtb/'

-- COMMAND ----------

-- DBTITLE 1,describe table
describe table testtb

-- COMMAND ----------

-- DBTITLE 1,describe table extended
describe table extended testtb

-- COMMAND ----------

drop table if exists testtable2

-- COMMAND ----------

-- MAGIC %fs rm -r dbfs:/user/hive/warehouse/testdb.db/testtable2

-- COMMAND ----------

show tables

-- COMMAND ----------

-- create table testtable (i1 int, t2 varchar(100))

create table testtable2 LOCATION 'dbfs:/user/hive/warehouse/testdb.db/testtable2';

-- COMMAND ----------

describe table testtable2

-- COMMAND ----------

-- DBTITLE 1,alter column
alter table testtable2 add column t2 varchar(3);
alter table testtable2 add column i1 integer ;

-- COMMAND ----------

alter table testtable2 alter column t2 type varchar(10)

-- COMMAND ----------

 alter table testtable2 alter column t2 comment 'type updated'

-- COMMAND ----------

describe table extended testtable2

-- COMMAND ----------

insert into testtable2 (i1, t2) values (1, 'prems');
insert into testtable2 (i1, t2) values (2, 'deux');
insert into testtable2 (i1, t2) values (3, 'trois');
insert into testtable2 (i1, t2) values (4, 'quatre');

-- COMMAND ----------

select * from testtable2

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC dbutils.fs.ls("/user/hive/warehouse/testdb.db/testtable2")

-- COMMAND ----------

-- MAGIC %fs ls /user/hive/warehouse/testdb.db/testtable2

-- COMMAND ----------

-- MAGIC %fs ls 'dbfs:/user/hive/warehouse/testdb.db/testtable2/_delta_log'

-- COMMAND ----------

-- DBTITLE 1,describe history
describe history testtable2
