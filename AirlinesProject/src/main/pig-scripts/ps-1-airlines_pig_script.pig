-- Find list of Airports operating in the Country India

LOAD_AIRPORTS = LOAD 'Projects/Project-1/airports_mod.dat' using PigStorage(',') 
	as (airport_id:long, name:chararray, city:chararray, country:chararray, iata_faa:chararray, icao:chararray, latitude:bigdecimal, 
	longitude:bigdecimal, altitude:int, timezone:bigdecimal, dst:chararray, tz:chararray);
	
	
LOAD_AIRLINES = LOAD 'Projects/Project-1/Final_airlines' using PigStorage(',')
	as (airline_id:long, name:chararray, alias:chararray, iata_faa:chararray, 
	icao:chararray, callsign:chararray, country:chararray, active:chararray);
	
LOAD_ROUTES = LOAD 'Projects/Project-1/routes.dat' using PigStorage(',')
	as (airline:chararray, airline_id:long, source_airport:chararray, source_airport_id:long, destination_airport:chararray, 
	destination_airport_id:long, codeshare:chararray, stops:long, equipment:chararray);
	
	
SELECT_ALL_COLUMN_AIRPORTS = FOREACH LOAD_AIRPORTS GENERATE airport_id, name, city, country, iata_faa, icao ,latitude ,longitude, altitude, timezone, dst, tz;

SELECT_ALL_COLUMN_AIRLINES = FOREACH LOAD_AIRLINES GENERATE airline_id, name, alias, iata_faa, icao, callsign, country, active;

SELECT_ALL_COLUMN_ROUTES = FOREACH LOAD_ROUTES GENERATE airline, airline_id, source_airport, source_airport_id, destination_airport, destination_airport_id, codeshare, stops, equipment;

------------------------------------------------------------------------------------------------------------------------------------------
-- Problem Statement - 1 : Find list of Airports operating in the Country India --------------------------------------------
------------------------------------------------------------------------------------------------------------------------------------------
FILTER_AIRPORTS_BY_COUNTRY_INDIA = FILTER SELECT_ALL_COLUMN_AIRPORTS by country == 'India';

INDIAN_AIRPORTS = FOREACH FILTER_AIRPORTS_BY_COUNTRY_INDIA GENERATE name;

-- dump INDIAN_AIRPORTS
-- STORE INDIAN_AIRPORTS into 'Indian_Airports';

------------------------------------------------------------------------------------------------------------------------------------------
-- Problem Statement - 2 : Find the list of Airlines having zero stops ---------------------------------
------------------------------------------------------------------------------------------------------------------------------------------
SELECT_AIRLINE_REQUIRE_COLUMNS = FOREACH SELECT_ALL_COLUMN_AIRLINES GENERATE airline_id, name;
SELECT_ROUTE_REQUIRE_COLUMNS = FOREACH SELECT_ALL_COLUMN_ROUTES GENERATE airline_id, stops;
FILTER_AIRLINES_HAVING_ZERO_STOPS = FILTER SELECT_ROUTE_REQUIRE_COLUMNS by stops == 0l;
DISTINCT_ROUTE_COLUMN = DISTINCT FILTER_AIRLINES_HAVING_ZERO_STOPS;
JOIN_AIRLINES_ROUTES = JOIN SELECT_AIRLINE_REQUIRE_COLUMNS BY airline_id, DISTINCT_ROUTE_COLUMN BY airline_id ;
DUMP JOIN_AIRLINES_ROUTES;

------------------------------------------------------------------------------------------------------------------------------------------
-- Problem statement - 3 : List of Airlines operating with code share -----------------------------------------------
------------------------------------------------------------------------------------------------------------------------------------------

SELECT_AIRLINE_REQUIRE_COLUMNS = FOREACH SELECT_ALL_COLUMN_AIRLINES GENERATE airline_id, name;
SELECT_ROUTE_REQUIRE_COLUMNS = FOREACH SELECT_ALL_COLUMN_ROUTES GENERATE airline_id, codeshare;
FILTER_AIRLINES_HAVING_ZERO_STOPS = FILTER SELECT_ROUTE_REQUIRE_COLUMNS by codeshare == 'Y';
DISTINCT_ROUTE_COLUMN = DISTINCT FILTER_AIRLINES_HAVING_ZERO_STOPS;
JOIN_AIRLINES_ROUTES = JOIN SELECT_AIRLINE_REQUIRE_COLUMNS BY airline_id, DISTINCT_ROUTE_COLUMN BY airline_id ;
DUMP JOIN_AIRLINES_ROUTES;

------------------------------------------------------------------------------------------------------------------------------------------
-- Problem Statement - 4 : Which country (or) territory having highest Airports
------------------------------------------------------------------------------------------------------------------------------------------

SELECT_REQUIRED_COLUMNS_AIRPORTS = FOREACH LOAD_AIRPORTS GENERATE airport_id, name, country;
GROUP_BY_COUNTRY = GROUP SELECT_REQUIRED_COLUMNS_AIRPORTS BY country;
COUNT_AIRPORTS = FOREACH GROUP_BY_COUNTRY GENERATE group, COUNT(SELECT_REQUIRED_COLUMNS_AIRPORTS);
ORDER_BY_COUNT = ORDER COUNT_AIRPORTS BY $1 DESC;
LIMIT_1_RECORD = LIMIT ORDER_BY_COUNT 1 ;
DUMP LIMIT_1_RECORD;

------------------------------------------------------------------------------------------------------------------------------------------
-- Problem Statement - 5 : Find the list of Active Airlines in United state
------------------------------------------------------------------------------------------------------------------------------------------

ACTIVE_AIRLINES = FILTER SELECT_ALL_COLUMN_AIRLINES BY active == 'Y';
DUMP ACTIVE_AIRLINES;

