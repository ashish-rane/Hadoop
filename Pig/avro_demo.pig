REGISTER /usr/hdp/current/pig-client/lib/piggybank.jar
REGISTER /usr/hdp/current/pig-client/lib/avro-*.jar
REGISTER /usr/hdp/current/pig-client/lib/jackson-core-asl-*.jar
REGISTER /usr/hdp/current/pig-client/lib/jackson-mapper-asl-*.jar
REGISTER /usr/hdp/current/pig-client/lib/json-simple-*.jar

stocks = LOAD 'datafiles/hirw/stocks' USING PigStorage(',')  as (exchnge:chararray, symbol:chararray, date:chararray, open:float, high:float, low:float, close:float, volume:int, adj_close:float);

-- STORE AS AVRO
STORE stocks INTO 'output/pig/avrofile/stocks_avro_comp' USING org.apache.pig.piggybank.storage.avro.AvroStorage(
'
{
            "schema": {
				"namespace": "com.hirw.avro",
				 "type": "record",
				 "name": "Stock",
				 "fields": [
					 {"name": "exchnge", "type": "string"},
					 {"name": "symbol",  "type": ["string", "null"]},
					 {"name": "date", "type": ["string", "null"]},
					 {"name": "open", "type": "float"},
					 {"name": "high",  "type": ["float", "null"]},
					 {"name": "low", "type": ["float", "null"]},
					 {"name": "close", "type": "float"},
					 {"name": "volume",  "type": ["int", "null"]},
					 {"name": "adj_close", "type": ["float", "null"]}
				 ]
            }
        }
');

-- Can also use where schema is in a file. this does not work currently
--STORE stocks INTO 'output/pig/avrofile/stocks_avro_comp' USING org.apache.pig.piggybank.storage.avro.AvroStorage('-f stocks.avsc');

-- Verify files
fs -ls output/pig/avrofile/stocks_avro_comp;
fs -copyToLocal output/pig/avrofile/stocks_avro_comp/part-m-00000.avro
sh vi part-m-00000.avro

--Load the avro file
stocks_avro = LOAD 'output/pig/avrofile/stocks_avro_comp' USING org.apache.pig.piggybank.storage.avro.AvroStorage('no_schema_check', 'schema_file','stocks.avsc');

top10 = LIMIT stocks_avro 10;
DUMP top10;

