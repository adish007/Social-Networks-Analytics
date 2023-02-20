-- Load the pages data
pages = LOAD '/Users/engjellramadani/Desktop/VSCODE/Project2_CS4433/PythonCodeCreation/MyPage.csv' USING PigStorage(',') AS (ID:int, Name:chararray, Nationality:chararray, CountryCode:int, hobby:chararray);

-- Load the friends data
friends = LOAD '/Users/engjellramadani/Desktop/VSCODE/Project2_CS4433/PythonCodeCreation/Friends.csv' USING PigStorage(',') AS (FriendRel:int, PersonID:int, MyFriend:int, DateofFriendship:int, Description:chararray);


-- Join the pages and friends data to get the number of friends for each page owner
joined_data = JOIN pages BY ID, friends BY PersonID;
num_friends = FOREACH joined_data GENERATE pages::Name AS name, COUNT(friends::MyFriend) AS friend_count;

-- Calculate the average number of friends across all owners
avg_friends = FOREACH (GROUP num_friends BY name) GENERATE AVG(num_friends.friend_count);

-- Filter for owners with more friends than the average
famous_owners = FILTER num_friends BY friend_count > avg_friends.$0;


DUMP famous_owners;
