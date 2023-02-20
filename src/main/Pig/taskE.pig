access_log = LOAD '/Users/engjellramadani/Desktop/VSCODE/Project2_CS4433/PythonCodeCreation/AccessLog.csv' USING PigStorage(',') AS (AccessId:int, ByWho:int, WhatPage:int, TypeOfAccess:int, AccessTime:int);

pages = LOAD '/Users/engjellramadani/Desktop/VSCODE/Project2_CS4433/PythonCodeCreation/MyPage.csv' USING PigStorage(',') AS (ID:int, Name:chararray, Nationality:chararray, CountryCode:int, hobby:chararray);

-- Join the access log with the page data to get the owner of each page
page_owners = JOIN access_log BY WhatPage, pages BY ID;

-- Group the data by page owner and count the total number of accesses and distinct pages accessed
access_counts = FOREACH (GROUP page_owners BY pages::Name) {
    total_accesses = COUNT(page_owners);
    distinct_pages = COUNT(DISTINCT (page_owners.access_logs::WhatPage));
    GENERATE group AS owner_name, total_accesses AS total_accesses, distinct_pages AS distinct_pages;
}

Dump access_counts;
