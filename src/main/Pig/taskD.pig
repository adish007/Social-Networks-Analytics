pages = LOAD '/Users/engjellramadani/Desktop/VSCODE/Project2_CS4433/PythonCodeCreation/MyPage.csv' USING PigStorage(',') AS (page_id:int, page_name:chararray, nationality:chararray, country_code:int, hobby:chararray);
friends = LOAD '/Users/engjellramadani/Desktop/VSCODE/Project2_CS4433/PythonCodeCreation/Friends.csv' USING PigStorage(',') AS (friend_rel:int, person_id:int, my_friend:int, date_of_friendship:int, Description:chararray);

-- Group by page owner and count the number of friends for each owner
friend_counts = GROUP friends BY my_friend;
happiness_factors = FOREACH friend_counts GENERATE group AS owner, COUNT(friends) AS num_friends;

-- Join the page owner and happiness factor information with the page information
page_happiness_factors = JOIN pages BY page_id LEFT OUTER, happiness_factors BY owner;

-- If an owner has no friends, set their happiness factor to 0
result = FOREACH page_happiness_factors GENERATE pages::page_name AS page_name, pages::nationality AS nationality, pages::country_code AS country_code, pages::hobby AS hobby, (num_friends is null ? 0 : num_friends) AS happiness_factor;

Dump result;
