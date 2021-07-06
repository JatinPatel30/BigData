
-- Create table Airport

create table airports(airport_id INT, name STRING, city STRING,country STRING,iata_faa STRING,icao STRING, latitude DECIMAL(9,6),longitude DECIMAL(9,6), altitude INT, timezone DECIMAL(4,2), dst CHAR(1), tz STRING)
row format delimited
fields terminated by ','
stored as textfile
location '/user/edureka_1548787/projects/project1/airports';
------------------------------------------------------------------------------------------------------------------------------------------
-- Create table Airlines

Create table airlines(airline_id Int,name STRING,alias STRING,iata_faa String,icao String,callsign string,country String,active char(1))
row format delimited
fields terminated by ','
stored as textfile
location '/user/edureka_1548787/projects/project1/airlines';

------------------------------------------------------------------------------------------------------------------------------------------
-- Create table RouteData


create table routes(airline String, airline_id INT, source_airport String, source_airport_id INT, destination_airport String, 
destination_airport_id INT, codeshare CHAR(1), stops INT, equipment String)
row format delimited
fields terminated by ','
stored as textfile
location '/user/edureka_1548787/projects/project1/routes';

------------------------------------------------------------------------------------------------------------------------------------------
-- Problem Statement - 1 : Find list of Airports operating in the Country India
------------------------------------------------------------------------------------------------------------------------------------------

-- Print result on Console
Select airport_id, name, country from airport where country = 'India';

-- Store result in table with stores as ORC File
CREATE TABLE RESULT_PS1
row format delimited
fields terminated by ','
stored as ORC
location '/user/edureka_1548787/projects/project1/hive_result_ps1'
 AS
Select airport_id, name from airport where country = 'India';


-- Store result in file
INSERT OVERWRITE DIRECTORY "/user/edureka_1548787/projects/output/ps1" ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
Select airport_id, name from airport where country = 'India';

------------------------------------------------------------------------------------------------------------------------------------------
-- Problem Statement - 2 : Find the list of Airlines having zero stops
------------------------------------------------------------------------------------------------------------------------------------------

SELECT DISTINCT al.* FROM airlines al JOIN routes r ON al.airline_id = r.airline_id AND stops = 0;

-- Store result in table with stores as RCFile
CREATE TABLE RESULT_PS2
row format delimited
fields terminated by ','
stored as textfile
location '/user/edureka_1548787/projects/project1/hive_result_ps2'
 AS
SELECT DISTINCT al.* FROM airlines al JOIN routes r ON al.airline_id = r.airline_id AND stops = 0;

-- Store result in file
INSERT OVERWRITE DIRECTORY "/user/edureka_1548787/projects/output/ps2" ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
SELECT DISTINCT al.* FROM airlines al JOIN routes r ON al.airline_id = r.airline_id AND stops = 0;




------------------------------------------------------------------------------------------------------------------------------------------
-- Problem Statement - 3 : List of Airlines operating with code share
------------------------------------------------------------------------------------------------------------------------------------------
Select distinct al.* from airlines al JOIN routes r ON al.airline_id = r.airline_id and r.codeshare = 'Y';

-- Store result in table with stores as RCFile
CREATE TABLE RESULT_PS3
row format delimited
fields terminated by ','
stored as ORC
location '/user/edureka_1548787/projects/project1/hive_result_ps3'
 AS
Select distinct al.* from airlines al JOIN routes r ON al.airline_id = r.airline_id and r.codeshare = 'Y';

-- Store result in file
INSERT OVERWRITE DIRECTORY "/user/edureka_1548787/projects/output/ps3" ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
Select distinct al.* from airlines al JOIN routes r ON al.airline_id = r.airline_id and r.codeshare = 'Y';

------------------------------------------------------------------------------------------------------------------------------------------
-- Problem Statement - 4 : Which country (or) territory having highest Airports

SELECT country, count(airport_id) as num_of_airports
    from airports group by country order by num_of_airports desc limit 1;

-- Store result in table
CREATE TABLE RESULT_PS4
row format delimited
fields terminated by ','
stored as AVRO
location '/user/edureka_1548787/projects/project1/hive_result_ps4'
 AS
SELECT country, count(airport_id) as num_of_airports from airports group by country order by num_of_airports desc limit 1;

-- Store result in file on specified location
INSERT OVERWRITE DIRECTORY "/user/edureka_1548787/projects/output/ps4" ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
SELECT country, count(airport_id) as num_of_airports from airports group by country order by num_of_airports desc limit 1;


------------------------------------------------------------------------------------------------------------------------------------------
-- Problem Statement - 5 : Find the list of Active Airlines in United state

-- Print result on console
SELECT * FROM airlines where country = 'United States' and active = 'Y';

-- Store result in table
CREATE TABLE RESULT_PS5
row format delimited
fields terminated by ','
stored as Parquet
location '/user/edureka_1548787/projects/project1/hive_result_ps5'
 AS
SELECT * FROM airlines where country = 'United States' and active = 'Y';

-- Store result in file on specified location
INSERT OVERWRITE DIRECTORY "/user/edureka_1548787/projects/output/ps5" ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
SELECT * FROM airlines where country = 'United States' and active = 'Y';