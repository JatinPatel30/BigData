
-- Load Data

YOUTUBE_DATA = load '/user/edureka_1548787/projects/project2/youtubedata/youtubedata.txt' using PigStorage('\t')
 as (videoid:chararray,uploader:chararray,age:int,category:chararray,length:int,views:int,rate:float,rating:int,comments:int,related_id:chararray);

--------------------------------------------------------------------------------------------------------------------------------
-------------- Problem statement 1 : Find out the top 5 categories with maximum number of videos uploaded.
--------------------------------------------------------------------------------------------------------------------------------
REQUIRED_COLUMNS = foreach YOUTUBE_DATA generate videoid,category;
GROUP_BY_CATEGORY = GROUP REQUIRED_COLUMNS BY category;
COUNT_NUM_OF_VIDEOS = FOREACH GROUP_BY_CATEGORY GENERATE group, COUNT(REQUIRED_COLUMNS);
ORDER_BY_COUNT = ORDER COUNT_NUM_OF_VIDEOS BY $1 DESC;
LIMIT_5_RECORD = LIMIT ORDER_BY_COUNT 5 ;
DUMP LIMIT_5_RECORD;



--------------------------------------------------------------------------------------------------------------------------------
-------------- Problem Statment 2 : Find out the top 10 rated videos.
--------------------------------------------------------------------------------------------------------------------------------

REQUIRED_COLUMNS = foreach YOUTUBE_DATA generate videoid, rate;
ORDER_BY_COUNT = ORDER REQUIRED_COLUMNS BY $1 DESC;
LIMIT_10_RECORD = LIMIT ORDER_BY_COUNT 10 ;
DUMP LIMIT_10_RECORD;
--------------------------------------------------------------------------------------------------------------------------------
-------------- Problem statement 3 : Find out the most viewed videos
--------------------------------------------------------------------------------------------------------------------------------

REQUIRED_COLUMNS = FOREACH YOUTUBE_DATA GENERATE videoid, views;
GROUP_IN_SINGLE_BAG = GROUP REQUIRED_COLUMNS ALL;
FIND_MAX_BAG = FOREACH GROUP_IN_SINGLE_BAG GENERATE MAX(REQUIRED_COLUMNS.views) as max;
FILTER_FOR_MAX = FILTER REQUIRED_COLUMNS BY views == FIND_MAX_BAG.max;
DUMP FILTER_FOR_MAX;
