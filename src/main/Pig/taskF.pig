friends = LOAD '/Users/engjellramadani/Desktop/VSCODE/Project2_CS4433/PythonCodeCreation/Friends.csv' USING PigStorage(',') AS (FriendRel:int, PersonID:int, MyFriend:int, DateofFriendship:int, Description:chararray);
access_log = LOAD '/Users/engjellramadani/Desktop/VSCODE/Project2_CS4433/PythonCodeCreation/AccessLog.csv' USING PigStorage(',') AS (AccessId:int, ByWho:int, WhatPage:int, TypeOfAccess:int, AccessTime:int);

-- Join the friends data with the access log data to get the friends' page accesses
friend_accesses = JOIN friends BY MyFriend, access_log BY ByWho;

-- Group the friend accesses by person and friend, and count the number of page accesses
friend_access_counts = GROUP friend_accesses BY (friends::PersonID, friends::MyFriend);
friend_access_counts = FOREACH friend_access_counts GENERATE
                       FLATTEN(group) AS (PersonID:int, MyFriend:int),
                       COUNT(friend_accesses) AS num_accesses;

-- Filter the friend access counts to only include people who have never accessed their friends' pages
never_accessed_friends_page = FILTER friend_access_counts BY num_accesses == 0;

-- Join the never-accessed friend data with the person data to get their names
person_names = JOIN never_accessed_friends_page BY PersonID, friends BY PersonID;
person_names = FOREACH person_names GENERATE never_accessed_friends_page::PersonID AS PersonID, friends::MyFriend AS FriendID, friends::Descr AS FriendshipDesc, friends::DateofFriendship AS DateofFriendship, friends::PersonID AS FriendPersonID, friends::MyFriend AS MyFriendID;

-- Join the person names with the friends data to get their friend names
friend_names = JOIN person_names BY FriendID, friends BY MyFriend;
friend_names = FOREACH friend_names GENERATE person_names::PersonID AS PersonID, friend_names::MyFriendID AS MyFriendID, friend_names::Descr AS FriendshipDesc, friend_names::DateofFriendship AS DateofFriendship, friend_names::FriendPersonID AS FriendPersonID, friends::Descr AS FriendName;

Dump friend_names;
