-- beeline connection: !connect jdbc:hive2://localhost:10000
-- Cities Table
CREATE EXTERNAL TABLE antennas.cities ( cityname string, population string, lat1 double, lon1 double, lat2 double, lon2 double, lat3 double, lon3 double, lat4 double, lon4 double, lat5 double, lon5 double, cityid BIGINT) STORED AS PARQUET LOCATION 'hdfs://localhost:9000/cleanData/cities/';

-- Antennas Table
CREATE EXTERNAL TABLE antennas.antennas ( antennaid STRING, intensity INT, x DOUBLE, y DOUBLE , cityid BIGINT) STORED AS PARQUET LOCATION 'hdfs://localhost:9000/cleanData/antennas/';

-- Clients Table
CREATE EXTERNAL TABLE antennas.clients ( clientid string, age integer, gender string, nationality string, civilstatus string, socioeconomiclevel string ) STORED AS PARQUET LOCATION 'hdfs://localhost:9000/cleanData/clients/';

-- Events Table
CREATE EXTERNAL TABLE antennas.events ( clientid string, date string, antennaid string, time string, day string, month string, year string, dayofweek string, hour string, minute string, weekhour string ) STORED AS PARQUET LOCATION 'hdfs://localhost:9000/cleanData/events/';

-- Events With prediction table
CREATE EXTERNAL TABLE antennas.eventspredicted ( clientid string , antennaid string , `1-00` BIGINT , `1-01` BIGINT , `1-02` BIGINT , `1-03` BIGINT , `1-04` BIGINT , `1-05` BIGINT , `1-06` BIGINT , `1-07` BIGINT , `1-08` BIGINT , `1-09` BIGINT , `1-10` BIGINT , `1-11` BIGINT , `1-12` BIGINT , `1-13` BIGINT , `1-14` BIGINT , `1-15` BIGINT , `1-16` BIGINT , `1-17` BIGINT , `1-18` BIGINT , `1-19` BIGINT , `1-20` BIGINT , `1-21` BIGINT , `1-22` BIGINT , `1-23` BIGINT , `2-00` BIGINT , `2-01` BIGINT , `2-02` BIGINT , `2-03` BIGINT , `2-04` BIGINT , `2-05` BIGINT , `2-06` BIGINT , `2-07` BIGINT , `2-08` BIGINT , `2-09` BIGINT , `2-10` BIGINT , `2-11` BIGINT , `2-12` BIGINT , `2-13` BIGINT , `2-14` BIGINT , `2-15` BIGINT , `2-16` BIGINT , `2-17` BIGINT , `2-18` BIGINT , `2-19` BIGINT , `2-20` BIGINT , `2-21` BIGINT , `2-22` BIGINT , `2-23` BIGINT , `3-00` BIGINT , `3-01` BIGINT , `3-02` BIGINT , `3-03` BIGINT , `3-04` BIGINT , `3-05` BIGINT , `3-06` BIGINT , `3-07` BIGINT , `3-08` BIGINT , `3-09` BIGINT , `3-10` BIGINT , `3-11` BIGINT , `3-12` BIGINT , `3-13` BIGINT , `3-14` BIGINT , `3-15` BIGINT , `3-16` BIGINT , `3-17` BIGINT , `3-18` BIGINT , `3-19` BIGINT , `3-20` BIGINT , `3-21` BIGINT , `3-22` BIGINT , `3-23` BIGINT , `4-00` BIGINT , `4-01` BIGINT , `4-02` BIGINT , `4-03` BIGINT , `4-04` BIGINT , `4-05` BIGINT , `4-06` BIGINT , `4-07` BIGINT , `4-08` BIGINT , `4-09` BIGINT , `4-10` BIGINT , `4-11` BIGINT , `4-12` BIGINT , `4-13` BIGINT , `4-14` BIGINT , `4-15` BIGINT , `4-16` BIGINT , `4-17` BIGINT , `4-18` BIGINT , `4-19` BIGINT , `4-20` BIGINT , `4-21` BIGINT , `4-22` BIGINT , `4-23` BIGINT , `5-00` BIGINT , `5-01` BIGINT , `5-02` BIGINT , `5-03` BIGINT , `5-04` BIGINT , `5-05` BIGINT , `5-06` BIGINT , `5-07` BIGINT , `5-08` BIGINT , `5-09` BIGINT , `5-10` BIGINT , `5-11` BIGINT , `5-12` BIGINT , `5-13` BIGINT , `5-14` BIGINT , `5-15` BIGINT , `5-16` BIGINT , `5-17` BIGINT , `5-18` BIGINT , `5-19` BIGINT , `5-20` BIGINT , `5-21` BIGINT , `5-22` BIGINT , `5-23` BIGINT , `6-00` BIGINT , `6-01` BIGINT , `6-02` BIGINT , `6-03` BIGINT , `6-04` BIGINT , `6-05` BIGINT , `6-06` BIGINT , `6-07` BIGINT , `6-08` BIGINT , `6-09` BIGINT , `6-10` BIGINT , `6-11` BIGINT , `6-12` BIGINT , `6-13` BIGINT , `6-14` BIGINT , `6-15` BIGINT , `6-16` BIGINT , `6-17` BIGINT , `6-18` BIGINT , `6-19` BIGINT , `6-20` BIGINT , `6-21` BIGINT , `6-22` BIGINT , `6-23` BIGINT , `7-00` BIGINT , `7-01` BIGINT , `7-02` BIGINT , `7-03` BIGINT , `7-04` BIGINT , `7-05` BIGINT , `7-06` BIGINT , `7-07` BIGINT , `7-08` BIGINT , `7-09` BIGINT , `7-10` BIGINT , `7-11` BIGINT , `7-12` BIGINT , `7-13` BIGINT , `7-14` BIGINT , `7-15` BIGINT , `7-16` BIGINT , `7-17` BIGINT , `7-18` BIGINT , `7-19` BIGINT , `7-20` BIGINT , `7-21` BIGINT , `7-22` BIGINT , `7-23` BIGINT , prediction INT ) STORED AS PARQUET LOCATION 'hdfs://localhost:9000/modeldata/predictions/';
