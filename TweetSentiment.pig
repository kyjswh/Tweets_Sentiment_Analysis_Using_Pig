--Read the records
r1= LOAD '/user/huser38/Homework5/tweets/tweets_20121102.txt' USING PigStorage('|') AS (Date:chararray, Second:chararray, Third:chararray, Fourth:chararray,Fifth:chararray, Sixth: chararray, Seven: chararray, Eight: chararray, Nine:chararray, tweets:chararray);
r2= LOAD '/user/huser38/Homework5/tweets/tweets_20121103.txt' USING PigStorage('|') AS (Date:chararray, Second:chararray, Third:chararray, Fourth:chararray,Fifth:chararray, Sixth: chararray, Seven: chararray, Eight: chararray, Nine:chararray, tweets:chararray);

records = UNION r1,r2;

REGISTER 'myudfs.jar';

tweets = FOREACH records GENERATE tweets AS t;


tweets_filtered = FILTER tweets BY t is not null;

sentiments = FOREACH tweets_filtered GENERATE myudfs.Yang_2(t) AS indicator;

t = GROUP sentiments BY indicator;
d = FOREACH t GENERATE group, COUNT(sentiments);



STORE d INTO 'output2';












