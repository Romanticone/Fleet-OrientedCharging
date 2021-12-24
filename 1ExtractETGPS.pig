REGISTER /home/wangguang/jar/datafu-1.2.0.jar;
REGISTER /home/wangguang/jar/piggybank.jar;
REGISTER /home/wangguang/jar/joda-time-1.6.jar;
REGISTER /home/wangguang/jar/DeteCharEve.jar
DEFINE MarkovPairs datafu.pig.stats.MarkovPairs();
DEFINE HaversineDistInMiles datafu.pig.geo.HaversineDistInMiles(); 
DEFINE ISOSecondsBetween org.apache.pig.piggybank.evaluation.datetime.diff.ISOSecondsBetween();
DEFINE CustomFormatToISO org.apache.pig.piggybank.evaluation.datetime.convert.CustomFormatToISO();

--TaxiGPS
--GPS data Clean, filter out the vehicles that not belong to this city, and format the time stamp
--before this step, we already do the map matching and filtered out the errant data

et_records = LOAD '$INPUT' USING PigStorage(',') AS (sys_time:chararray, car_id:chararray,car_lon:double,car_lat:double,loadtime:chararray,noMean1,speed,direction,noMean2,flag:int,noMean3, noMean4,color:chararray,speed2,noMean5,noMean6,noMean7);

taxi_rec = FOREACH et_records GENERATE car_id,1 AS taxicomp,car_lon,car_lat,CustomFormatToISO(loadtime,'YY-MM-dd HH:mm:ss') AS realTime:chararray, color,speed, 1 AS sta;

taxi_sz = FILTER taxi_rec BY car_id MATCHES '粤BD.*';

taxi_et = FOREACH taxi_sz GENERATE car_id,taxicomp,car_lon,car_lat,realTime,color,speed,sta;

taxi_et1 = ORDER taxi_et BY car_id, realTime;

taxi_et2 = FOREACH taxi_et1 GENERATE $0, $1, $2, $3, $4, $5, $6, $7;
-- Store ET data

STORE taxi_et2 INTO '$OUTPUT' USING PigStorage(',');