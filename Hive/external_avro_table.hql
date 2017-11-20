CREATE EXTERNAL TABLE retail_demo.departments_avro 
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
LOCATION 'hdfs://sandbox.hortonworks.com:8020/user/root/sqoop_temp_test/departments'
TBLPROPERTIES('avro.schema.url'='hdfs://sandbox.hortonworks.com/user/root/sqoop/departments.avsc');


-- OR 


CREATE EXTERNAL TABLE retail_demo.departments_avro
 ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
 STORED AS AVRO
 LOCATION 'hdfs://sandbox.hortonworks.com:8020/user/root/sqoop_temp_test/departments'
 TBLPROPERTIES('avro.schema.url'='hdfs://sandbox.hortonworks.com/user/root/sqoop/departments.avsc');



-- OR

-- NOTE: for the below to work the column names and types should exactly match with those mentioned in the AVSC file.
CREATE EXTERNAL TABLE retail_demo.departments_avro (department_id int, department_name string) 
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe' 
STORED AS AVRO LOCATION 'hdfs://sandbox.hortonworks.com:8020/user/root/sqoop_temp_test/departments';
