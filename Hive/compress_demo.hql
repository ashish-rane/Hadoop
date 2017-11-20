CREATE DATABASE cards;

use cards;

CREATE TABLE deckOfCards_raw (color string, suit string, pip string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' STORED AS TEXTFILE;


LOAD DATA LOCAL INPATH '/root/Projects/Datafiles/cards/largedeck.txt.gz' OVERWRITE INTO TABLE  deckOfCards_raw;

dfs -ls /apps/hive/warehouse/cards.db/deckcfcards_raw;

CREATE TABLE deckOfCards_seq (color string, suit string, pip string) STORED AS SEQUENCEFILE;


CREATE TABLE deckOfCards_txt_comp (color string, suit string, pip string) STORED AS TEXTFILE;

SET hive.exec.compress.output=true;
SET io.seqfile.compression.type=BLOCK; // NONE/RECORD/BLOCK

INSERT OVERWRITE TABLE deckOfCards_seq SELECT * FROM deckOfCards_raw;


INSERT INTO TABLE deckOfCards_txt_comp SELECT * from deckOfCards_raw;
