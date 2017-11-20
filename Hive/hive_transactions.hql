
CREATE TABLE hive_transactions (col1 int, col2 string);

-- Inserts in hive are always auto-commit. There is no rollback supported or commit transaction required.
-- This insert will be performed in old style not in transactional style
INSERT INTO TABLE hive_transactions VALUES (1, 'ItVersity');

-- Delete-- This won't work as the table that needs to support delete and update needs to use AcidOutputFormat and should be bucketed.
DELETE FROM hive_transactions WHERE col1 = 1;

DROP TABLE hive_transactions;


-- Required Properites
SET hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
SET hive.support.concurrency= true;
SET hive.enforce.bucketing=true;
SET hive.exec.dynamic.partition.mode=nonstrict;


-- For Insert Update Delete to work it must support Hive Transactons
-- which means it need to be bucketed, stored as orc and Table property 'transactional' set to true
CREATE TABLE hive_transactions (col1 int, col2 string) CLUSTERED BY(col1) INTO 4 BUCKETS STORED AS ORC TBLPROPERTIES ('transactional' = 'true');

INSERT INTO TABLE hive_transactions VALUES (1, 'ItVersity');

dfs -ls /apps/hive/warehouse/hive_transactions/


INSERT INTO TABLE hive_transactions VALUES(2, 'HIRW');

dfs -ls /apps/hive/warehouse/hive_transactions/

SELECT * from hive_transactions;

--NOTE: Partition and Bucket columns cannot be updated, but they can be used in the where clause of update statement.
UPDATE hive_transactions SET col2 = 'HDPCD' WHERE col1 = 1 ;

dfs -ls /apps/hive/warehouse/hive_transactions;

SELECT * from hive_transactions;

DELETE FROM hive_transactions WHERE col1 = 2;

dfs -ls /apps/hive/warehouse/hive_transactions;

SELECT * from hive_transactions;

-- Do the major compaction. This will merge the delta files per bucket for each of the insert update and delete commands with the base file per bucket
-- and create a new base file per bucket.
ALTER TABLE hive_transactions COMPACT 'MAJOR';
