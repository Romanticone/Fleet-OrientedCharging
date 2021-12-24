REGISTER /home/wangguang/jar/datafu-1.2.0.jar;
REGISTER /home/wangguang/jar/piggybank.jar;
REGISTER /home/wangguang/jar/joda-time-1.6.jar;
REGISTER /home/wangguang/jar/DeteCharEve.jar
DEFINE MarkovPairs datafu.pig.stats.MarkovPairs();
DEFINE HaversineDistInMiles datafu.pig.geo.HaversineDistInMiles(); 
DEFINE ISOSecondsBetween org.apache.pig.piggybank.evaluation.datetime.diff.ISOSecondsBetween();
DEFINE ABS org.apache.pig.piggybank.evaluation.math.ABS();
DEFINE ISOToUnix org.apache.pig.piggybank.evaluation.datetime.convert.ISOToUnix();
DEFINE GetDateHour deteChar.GetDateHour();
DEFINE getMaxHour deteChar.getMaxHour();
DEFINE GetDate deteChar.GetDate();
DEFINE GetHour getTime.GetHour();
SET default_parallel 1;
SET pig.tmpfilecompression true
SET pig.tmpfilecompression.codec gz


data_ChargeEvent = LOAD '$INPUT1' USING PigStorage(',') AS (car_id:chararray,begin_time:chararray,end_time:chararray,distDiff:double,charge_duration:long,car_lon:double,car_lat:double,flag:int,charge_name:chararray);

station_withpile1 = LOAD '$INPUT2' USING PigStorage(',') AS (station_id1:int,station_name1:chararray,station_lon1:double,station_lat1:double,join_type1:int,pile_number1:int);

data_flt = FOREACH data_ChargeEvent GENERATE car_id,GetHour(begin_time) AS begin_time:chararray,end_time,distDiff,charge_duration,car_lon,car_lat,charge_name;

data_grp = GROUP data_flt BY (charge_name);

count_chartime = FOREACH data_grp GENERATE FLATTEN(group),COUNT(data_flt.end_time);

jnt = JOIN count_chartime BY $0, station_withpile1 BY station_name1;

countNum = FOREACH jnt GENERATE $0 AS station_name:chararray,$1 AS num,$2 AS station_Id, $4, $5, $7 AS pileNum:double;

DUMP countNum
--STORE count_chartime INTO '$OUTPUT' USING PigStorage(',');