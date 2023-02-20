MyPage = LOAD '/Users/engjellramadani/Desktop/VSCODE/Project2_CS4433/PythonCodeCreation/MyPage.csv' USING PigStorage(',') AS (ID:int, Name:chararray, Nationality:chararray, CountryCode:int, hobby:chararray);
AccessLog = LOAD '/Users/engjellramadani/Desktop/VSCODE/Project2_CS4433/PythonCodeCreation/AccessLog.csv' USING PigStorage(',') AS (AccessId:int, ByWho:int, WhatPage:int, TypeOfAccess:int, AccessTime:int);

joinedTable = JOIN MyPage BY ID, AccessLog BY WhatPage;

groupedJoin = GROUP joinedTable BY  AccessLog::WhatPage;

AccessCounts = FOREACH groupedJoin GENERATE AccessLog::WhatPage AS ID, MyPage::Name AS Name, MyPage::Nationality AS Nationality, COUNT(joinedTable) AS count;

Result = ORDER AccessCounts by count DESC;
Result = LIMIT Result 8;
Dump Result;
