categories = LOAD 'retail_db.categories' USING org.apache.hive.hcatalog.pig.HCatLoader();
departments = LOAD 'retail_db.departments' USING org.apache.hive.hcatalog.pig.HCatLoader();

replicated_join = JOIN categories BY category_department_id LEFT, departments BY department_id USING 'replicated';
EXPLAIN replicated_join;
--ILLUSTRATE replicated_join;
--DUMP replicated_join;
