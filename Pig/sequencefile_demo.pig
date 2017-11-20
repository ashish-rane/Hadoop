-- We need following libraries to store sequence files
REGISTER '/usr/hdp/current/pig-client/lib/elephant-bird-pig-4.0.jar';
REGISTER '/usr/hdp/current/pig-client/lib/elephant-bird-hadoop-compat-4.0.jar';
REGISTER '/usr/hdp/current/pig-client/lib/elephant-bird-core-4.0.jar';

-- We require the Piggybank to load from sequence file
REGISTER '/usr/hdp/current/pig-client/lib/piggybank.jar';

-- Define aliases for ease of use
%DECLARE TEXT_CONVERTER 'com.twitter.elephantbird.pig.util.TextConverter';
%declare LONG_CONVERTER 'com.twitter.elephantbird.pig.util.LongWritableConverter';

stocks = LOAD 'datafiles/hirw/stocks' USING PigStorage() AS (line:chararray);

stocks_len = FOREACH stocks GENERATE ToUnixTime(CurrentTime()) AS now, line;

-- STORE
STORE stocks_len INTO 'output/pig/fileformats/sequencefile/pig-sequence' USING com.twitter.elephantbird.pig.store.SequenceFileStorage(
	'-c $LONG_CONVERTER',
	'-c $TEXT_CONVERTER');

-- Load the sequence file
DEFINE SequenceFileLoader org.apache.pig.piggybank.storage.SequenceFileLoader();

seq_dataset = LOAD 'hdfs://sandbox.hortonworks.com:8020/user/root/output/pig/fileformats/sequencefile/pig-sequence' USING SequenceFileLoader() AS (key:long, value:chararrray);

split_value = FOREACH seq_dataset GENERATE FLATTEN(STRSPLIT(value, ',' ,9));
sym_vol = FOREACH split_value GENERATE (chararray)$1 as sym, (double)$7 as vol;

grp_sym = GROUP sym_vol BY sym;

avg_vol = FOREACH grp_sym GENERATE group, AVG(sym_vol.vol);
top10 = LIMIT avg_vol 10;
DUMP top10;

-- Store results back to sequence file
STORE avg_vol INTO 'output/pig/fileformats/sequencefile/pig-results' USING com.twitter.elephantbird.pig.store.SequenceFileStorage(
	'-c $TEXT_CONVERTER',
	'-c $TEXT_CONVERTER'
);
	

