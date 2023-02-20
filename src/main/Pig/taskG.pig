pages = LOAD '/Users/engjellramadani/Desktop/VSCODE/Project2_CS4433/PythonCodeCreation/MyPage.csv' USING PigStorage(',') AS (ID:int, Name:chararray, Nationality:chararray, CountryCode:int, hobby:chararray);
access_log = LOAD '/Users/engjellramadani/Desktop/VSCODE/Project2_CS4433/PythonCodeCreation/AccessLog.csv' USING PigStorage(',') AS (AccessId:int, ByWho:int, WhatPage:int, TypeOfAccess:int, AccessTime:int);

-- Join the two files on the ID and WhatPage fields
joined = JOIN pages BY ID, access_log BY WhatPage;

-- Group the data by the page ID and person ID and compute the last access time
grouped = GROUP joined BY (pages::ID, access_log::ByWho);
last_access = FOREACH grouped GENERATE FLATTEN(group) AS (ID, PersonID), MAX(joined.access_log::AccessTime) AS last_access;

-- Filter out the pages where the last access date is greater than 14 days ago
filtered = FILTER last_access BY (last_access.last_access > 2000);

-- Get the list of people who have lost interest
lost_interest = FOREACH filtered GENERATE pages::Name AS PersonName;

-- Store the results
Dump lost_interest;
