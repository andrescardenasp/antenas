-- beeline connection: !connect jdbc:hive2://localhost:10000
-- Cities Table
CREATE EXTERNAL TABLE antennas.cities ( cityname string, population string, lat1 double, lon1 double, lat2 double, lon2 double, lat3 double, lon3 double, lat4 double, lon4 double, lat5 double, lon5 double, cityid BIGINT) STORED AS PARQUET LOCATION 'hdfs://localhost:9000/cleanData/cities/';

-- Antennas Table
CREATE EXTERNAL TABLE antennas.antennas ( antennaid STRING, intensity INT, x DOUBLE, y DOUBLE , cityid BIGINT) STORED AS PARQUET LOCATION 'hdfs://localhost:9000/cleanData/antennas/';

-- Clients Table
CREATE EXTERNAL TABLE antennas.clients ( clientid string, age integer, gender string, nationality string, civilstatus string, socioeconomiclevel string ) STORED AS PARQUET LOCATION 'hdfs://localhost:9000/cleanData/clients/';

-- Events Table
CREATE EXTERNAL TABLE antennas.events ( clientid string, date string, antennaid string, time string, day string, month string, year string, dayofweek string, hour string, minute string ) STORED AS PARQUET LOCATION 'hdfs://localhost:9000/cleanData/clients/';

-- Events With prediction table
CREATE EXTERNAL TABLE antennas.eventspredicted ( clientid string, antennaid string, dayofweek string, hour string, intensity integer, x double, y double, cityid long, age integer, gender string, nationality string, civilstatus string, socioeconomiclevel string, prediction integer ) STORED AS PARQUET LOCATION 'hdfs://localhost:9000/modeldata/predictions/';