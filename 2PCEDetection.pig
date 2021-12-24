REGISTER /home/wangguang/jar/datafu-1.2.0.jar;
REGISTER /home/wangguang/jar/piggybank.jar;
REGISTER /home/wangguang/jar/joda-time-1.6.jar;
REGISTER /home/wangguang/jar/DeteCharEve.jar
REGISTER /home/wangguang/DeteEventNew2.jar

DEFINE DeteEventNewStay3 deteChar.DeteEventNewStay3();

--set Markov pairs to jump n steps
--i.e. set two steps in a, b, c and get ((a),(b)) ((b),(c)) ((a),(c))

DEFINE ISOSecondsBetween org.apache.pig.piggybank.evaluation.datetime.diff.ISOSecondsBetween();   
DEFINE HaversineDistInMiles datafu.pig.geo.HaversineDistInMiles(); 

--lookahead_steps
--extract the possible charging events from the cleaned GPS data
--we designed an event detection function in DeteCharEve.jar

ev_record = LOAD '$INPUT' USING PigStorage(',') AS (car_id:chararray,type:chararray,car_lon:double,car_lat:double,loadtime:chararray,status:int,flag:int);

ev_dist = DISTINCT ev_record;

ev_records = FOREACH ev_dist GENERATE $0 AS car_id:chararray, $1 AS type:chararray, $2 AS car_lon:double, $3 AS car_lat:double, $4 AS loadtime:chararray, $5 AS status:int, $6 AS flag:int;

--Nearby records are set into a pair as (ELEM1, ELEM2)

grp_tmp = GROUP ev_records BY car_id;

data_pairs = FOREACH grp_tmp{
	sort = ORDER ev_records BY loadtime;
	GENERATE FLATTEN(DeteEventNewStay3(sort)) AS (ELEM1:TUPLE(car_id:chararray,type:chararray,car_lon:double,car_lat:double,loadtime:chararray,status:int,flag:int), 
		ELEM2:TUPLE(car_id:chararray,type:chararray,car_lon:double,car_lat:double,loadtime:chararray,status:int,flag:int),ELEM3:TUPLE(car_id:chararray,type:chararray,car_lon:double,car_lat:double,loadtime:chararray,status:int,flag:int));	
	};


data_compare = FOREACH data_pairs GENERATE ELEM1.car_id AS car_id,ELEM1.car_lon AS car_lon,ELEM1.car_lat AS car_lat,ELEM1.loadtime AS before_time:chararray,ELEM2.loadtime AS after_time:chararray,HaversineDistInMiles(ELEM2.car_lon,ELEM2.car_lat,ELEM1.car_lon,ELEM1.car_lat)*1609.344 AS distDiff:double,ISOSecondsBetween(ELEM2.loadtime,ELEM1.loadtime) AS timeDiff,ELEM3.car_lon AS stop_lon,ELEM3.car_lat AS stop_lat;

STORE data_compare INTO '$OUTPUT' USING PigStorage(',');