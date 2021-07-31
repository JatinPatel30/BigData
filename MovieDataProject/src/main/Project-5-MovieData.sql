

create external table movies(
id int,
name string,
year_of_release int,
rating float,
duration int)
row format delimited
fields terminated by ','
stored as textfile
location '/user/edureka_1548787/projects/project5/movies';


---------------------------------------------------------------------------------------------------------------------------------------------------
--- Problem statement - 1 :Find the number of movies released between 1950 and 1960.
---------------------------------------------------------------------------------------------------------------------------------------------------
select * from movies where year_of_release between 1950 and 1960;
select count(*) from movies where year_of_release between 1950 and 1960;
---------------------------------------------------------------------------------------------------------------------------------------------------
--- Problem statement - 2 :Find the number of movies having rating more than 4.
---------------------------------------------------------------------------------------------------------------------------------------------------
select * from movies where rating > 4;
select count(*) from movies where rating > 4;
---------------------------------------------------------------------------------------------------------------------------------------------------

--- Problem statement - 3 : Find the movies whose rating are between 3 and 4.
---------------------------------------------------------------------------------------------------------------------------------------------------
select * from movies where rating between 3 and 4;
select count(*) from movies where rating between 3 and 4;
---------------------------------------------------------------------------------------------------------------------------------------------------
--- Problem statement - 4 :Find the number of movies with duration more than 2 hours (7200 second).
---------------------------------------------------------------------------------------------------------------------------------------------------
select * from movies where duration >= 7200;
select count(*) from movies where duration >= 7200;
---------------------------------------------------------------------------------------------------------------------------------------------------
--- Problem statement - 5 :Find the list of years and number of movies released each year.
---------------------------------------------------------------------------------------------------------------------------------------------------
select year_of_release, count(id) from movies group by year_of_release;
---------------------------------------------------------------------------------------------------------------------------------------------------
--- Problem statement - 6 :Find the total number of movies in the dataset.
---------------------------------------------------------------------------------------------------------------------------------------------------
select count(id) from movies;
---------------------------------------------------------------------------------------------------------------------------------------------------