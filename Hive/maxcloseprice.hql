### CREATE EXTERNAL TABLE ###
hive> CREATE EXTERNAL TABLE IF NOT EXISTS stocks_starterkit(
exch STRING,
symbol STRING,
trxDate STRING,
price_current FLOAT,
price_open FLOAT,
price_close FLOAT,
price_high FLOAT,
price_low FLOAT,
volume INT)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
LOCATION '/user/root/mapreduceInputs';

### SELECT 100 Records ###
hive> SELECT * FROM stocks_starterkit
LIMIT 100;

### DESCRIBE TO GET MORE INFO About Table ###
hive>DESCRIBE FORMATTED stocks_starterkit;

### Calculate Max closing price ###
hive>SELECT symbol, max(price_close) max_close FROM stocks_starterkit
GROUP BY symbol;


