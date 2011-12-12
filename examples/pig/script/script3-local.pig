REGISTER ./tutorial.jar;

-- Use the PigStorage function to load the excite log file into the raw bag as an array of records.
-- Input: (user,time,query) 
raw = LOAD 'excite-small.log' USING PigStorage('\t') AS (user: chararray, time: chararray, query: chararray);
STORE raw INTO 'script3-local-results.txt' USING PigStorage();