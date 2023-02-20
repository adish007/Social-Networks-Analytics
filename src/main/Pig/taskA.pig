Data_in = LOAD '/Users/engjellramadani/Desktop/VSCODE/Project2_CS4433/PythonCodeCreation/MyPage.csv' USING PigStorage(',') AS (ID:int, Name:chararray, Nationality:chararray, CountryCode:int, hobby:chararray);

Filtered = FILTER Data_in by Nationality  == 'CanadaCanadaCanada';
Result = FOREACH Filtered GENERATE Name, hobby;
Dump Result;
