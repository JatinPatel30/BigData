
-------------------------------------------------------------------------------------------------------------
--- Create table books
-------------------------------------------------------------------------------------------------------------

create external table books(
     ISBN string,
     book_title string,
     book_author string,
     year_of_publication int,
     publisher string,
     image_url_s string,
     image_url_m string,
     image_url_l string
) ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
  WITH SERDEPROPERTIES (
     "separatorChar" = "\;",
     "quoteChar"     = "\""
  ) stored as textfile location '/user/edureka_1548787/projects/project3/books'
TBLPROPERTIES ('skip.header.line.count'='1');

-------------------------------------------------------------------------------------------------------------
--- Create table book_ratings
-------------------------------------------------------------------------------------------------------------


create external table book_ratings(
     userId int,
     ISBN string,
     book_rating int
) ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
  WITH SERDEPROPERTIES (
     "separatorChar" = "\;",
     "quoteChar"     = "\""
  ) stored as textfile location '/user/edureka_1548787/projects/project3/ratings'
TBLPROPERTIES ('skip.header.line.count'='1');


-------------------------------------------------------------------------------------------------------------
--- Create table book_users
-------------------------------------------------------------------------------------------------------------
create external table book_users(
     userId int,
     location string,
     age int
) ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
  WITH SERDEPROPERTIES (
     "separatorChar" = "\;",
     "quoteChar"     = "\""
  ) stored as textfile location '/user/edureka_1548787/projects/project3/users'
TBLPROPERTIES ('skip.header.line.count'='1');


-------------------------------------------------------------------------------------------------------------
--- Problem statement 1: Find out the frequency of books published each year. (Hint: Use Boooks.csv file for this)
-------------------------------------------------------------------------------------------------------------
select year_of_publication, count(DISTINCT book_title) as counts from books group by year_of_publication order by counts DESC;

-------------------------------------------------------------------------------------------------------------
--- Problem statement 2 : Find out in which year maximum number of books were published
-------------------------------------------------------------------------------------------------------------

select year_of_publication, count(ISBN) as counts from books group by year_of_publication order by counts desc limit 1 ;


-------------------------------------------------------------------------------------------------------------
--- Problem statement 3 : Find out how many book were published based on ranking in the year 2002. ( Hint: Use Book.csv and Book-Ratings.csv)
-------------------------------------------------------------------------------------------------------------

select br.book_rating, count(br.ISBN) from books b JOIN book_ratings br ON  b.ISBN = br.ISBN AND b.year_of_publication = 2002
group by br.book_rating order by cast(br.book_rating as bigint) desc;