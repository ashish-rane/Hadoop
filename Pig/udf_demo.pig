REGISTER '/usr/hdp/2.3.0.0-2557/pig/lib/piggybank.jar' 
DEFINE makeUpper org.apache.pig.piggybank.evaluation.string.UPPER();
departments = LOAD 'retail_db.departments' USING org.apache.hive.hcatalog.pig.HCatLoader();

dep_name_upper = FOREACH departments GENERATE makeUpper(department_name) AS dep_name;
DUMP dep_name_upper;

