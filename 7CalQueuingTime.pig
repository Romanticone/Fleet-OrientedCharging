REGISTER /home/wangguang/jar/datafu-1.2.0.jar;
REGISTER /home/wangguang/jar/piggybank.jar;
REGISTER /home/wangguang/jar/joda-time-1.6.jar;
REGISTER /home/wangguang/jar/DeteCharEve.jar
DEFINE MarkovPairs datafu.pig.stats.MarkovPairs();
DEFINE HaversineDistInMiles datafu.pig.geo.HaversineDistInMiles(); 
DEFINE ISOSecondsBetween org.apache.pig.piggybank.evaluation.datetime.diff.ISOSecondsBetween();

--PCE
ev_pce1 = LOAD '$INPUT1' USING PigStorage(',') AS (car_id:chararray,car_lon:double,car_lat:double,before_time:chararray,after_time:chararray,distDiff:double,timeDiff:long,stop_lon:double,stop_lat:double);

pce1_gen = FOREACH ev_pce1 GENERATE car_id,car_lon,car_lat,before_time,after_time,distDiff,timeDiff,stop_lon,stop_lat;

--AllCharEvent1
ev_ace = LOAD '$INPUT2' USING PigStorage(',') AS (car_id:chararray,before_time:chararray,after_time:chararray,distDiff:double,timeDiff:long,stop_lon:double,stop_lat:double,flag:int,station_name:chararray);

data_ace = FOREACH ev_ace GENERATE car_id,before_time,after_time,stop_lon,stop_lat,1 AS flg,station_name;
--Combine
data_jn = JOIN data_ace BY (car_id,stop_lon,stop_lat,before_time), pce1_gen BY (car_id,stop_lon,stop_lat,before_time);

data_out = FOREACH data_jn GENERATE data_ace::car_id AS car_id:chararray,pce1_gen::car_lon AS car_lon:double,pce1_gen::car_lat AS car_lat:double,data_ace::before_time AS before_time:chararray,data_ace::after_time AS after_time:chararray,data_ace::stop_lon AS stop_lon:double,data_ace::stop_lat AS stop_lat:double,data_ace::station_name AS station_name:chararray;

--GPS
gps_records = LOAD '$INPUT3' USING PigStorage(',') AS (car_id:chararray,type:chararray,car_lon:double,car_lat:double,loadtime:chararray,status:int,flag:int);

gps_gen = FOREACH gps_records GENERATE car_id,car_lon,car_lat,loadtime;

ev_jn = JOIN data_out BY (car_id,stop_lon,stop_lat), gps_gen BY (car_id,car_lon,car_lat);

jn_out = FOREACH ev_jn GENERATE data_out::car_id,data_out::car_lon,data_out::car_lat,data_out::before_time,gps_gen::loadtime,gps_gen::car_lon,gps_gen::car_lat,data_out::station_name AS station_name:chararray;

join_time = FOREACH jn_out GENERATE $0,$1,$2,$3,$4,$5,$6,$7,ISOSecondsBetween($4,$3) AS queuingTime:long;

join_flt = FILTER join_time BY (queuingTime>=0);

join_group = GROUP join_flt BY ($0,$1,$2,$3,$7);

join_out = FOREACH join_group GENERATE FLATTEN(group), MIN(join_flt.$8);

join_order = ORDER join_out BY $0,$3;

STORE join_order INTO '$OUTPUT' USING PigStorage(',');