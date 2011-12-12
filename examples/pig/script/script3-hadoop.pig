REGISTER ./tutorial.jar;

-- Use the PigStorage function to load the excite log file into the raw bag as an array of records.
-- Input: (user,time,query) 

-- Delete existing output folder
-- fs -rmr script3-hadoop-results

-- Copy input file
fs -put excite-small.log /user/harry/excite-small.log

raw = LOAD 'excite-small.log' USING PigStorage('\t') AS (user: chararray, time: chararray, query: chararray);
STORE raw INTO 'script3-hadoop-results' USING PigStorage();