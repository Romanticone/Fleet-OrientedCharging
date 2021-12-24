REGISTER /home/wangguang/jar/datafu-1.2.0.jar;
REGISTER /home/wangguang/jar/piggybank.jar;
REGISTER /home/wangguang/jar/joda-time-1.6.jar;
REGISTER /home/wangguang/jar/DeteCharEve.jar

--set Markov pairs to jump n steps
--i.e. set two steps in a, b, c and get ((a),(b)) ((b),(c)) ((a),(c))

DEFINE DeteStation deteChar.DeteStation();
DEFINE ISOSecondsBetween org.apache.pig.piggybank.evaluation.datetime.diff.ISOSecondsBetween();   
DEFINE HaversineDistInMiles datafu.pig.geo.HaversineDistInMiles(); 



ev_pce = LOAD '$INPUT1' USING PigStorage(',') AS (car_id:chararray,car_lon:double,car_lat:double,before_time:chararray,after_time:chararray,distDiff:double,timeDiff:long,stop_lon:double,stop_lat:double);

ev_select = FOREACH ev_pce GENERATE car_id,before_time,after_time,distDiff,timeDiff,stop_lon,stop_lat, 1 AS join_type:int;

station_withpile = LOAD '$INPUT2' USING PigStorage(',') AS (station_id:int,station_name:chararray,station_lon:double,station_lat:double,join_type:int,pile_number:int);

station_select = FOREACH station_withpile GENERATE station_name,station_lon,station_lat,join_type,pile_number;

jnt = JOIN ev_select BY join_type, station_select BY join_type;

--count the distance between EVs and CSs
dist = FOREACH jnt GENERATE $0 AS car_id:chararray,$1 AS before_time:chararray,$2 AS after_time:chararray,$3 AS distDiff:double,$4 AS timeDiff:long,$5 AS stop_lon:double,$6 AS stop_lat:double,$8 AS station_name:chararray,HaversineDistInMiles($10,$9,$6,$5)*1609.344 AS distance:double;


flt = FILTER dist BY distance<200;

--

event_flt = FOREACH flt GENERATE $0 AS car_id:chararray,$1 AS before_time:chararray,$2 AS after_time:chararray,$3 AS distDiff:double,$4 AS timeDiff:long,$5 AS stop_lon:double,$6 AS stop_lat:double,$7 AS station_name:chararray,$8 AS distance:double;

--When an charging event is assigned into different charging station, the nearest station is selected

event_flt_group = GROUP event_flt BY ($0,$1,$2);

FCE = FOREACH event_flt_group {
	sort = ORDER event_flt BY distance;
	top = limit sort 1;
	GENERATE FLATTEN(top.car_id) AS car_id:chararray,FLATTEN(top.before_time) AS before_time:chararray,FLATTEN(top.after_time) AS after_time:chararray,FLATTEN(top.distDiff) AS distDiff:double,FLATTEN(top.timeDiff) AS timeDiff:long,FLATTEN(top.stop_lon) AS stop_lon:double,FLATTEN(top.stop_lat) AS stop_lat:double, FLATTEN(top.station_name) AS station_name:chararray;
};

event_sort = ORDER FCE BY $0, $1 ASC;

STORE event_sort INTO '$OUTPUT' USING PigStorage(',');