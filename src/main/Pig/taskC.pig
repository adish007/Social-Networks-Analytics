Data_in = LOAD '/Users/engjellramadani/Desktop/VSCODE/Project2_CS4433/PythonCodeCreation/MyPage.csv' USING PigStorage(',') AS (ID:int, Name:chararray, Nationality:chararray, CountryCode:int, hobby:chararray);

-- Group by country and count the number of citizens with a page
citizens_by_country = GROUP Data_in BY country;
result = FOREACH citizens_by_country GENERATE group AS country, COUNT(Data_in.has_page) AS num_citizens_with_page;

Dump result;

