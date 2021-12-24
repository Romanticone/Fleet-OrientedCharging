REGISTER /home/wangguang/jar/datafu-1.2.0.jar;
REGISTER /home/wangguang/jar/piggybank.jar;
REGISTER /home/wangguang/jar/joda-time-1.6.jar;

DEFINE MarkovPairs datafu.pig.stats.MarkovPairs();
DEFINE HaversineDistInMiles datafu.pig.geo.HaversineDistInMiles(); 
DEFINE ISOSecondsBetween org.apache.pig.piggybank.evaluation.datetime.diff.ISOSecondsBetween();


--input all FCE
ev_pce = LOAD '$INPUT1' USING PigStorage(',') AS (car_id:chararray,before_time:chararray,after_time:chararray,distDiff:double,timeDiff:long,stop_lon:double,stop_lat:double,station_name:chararray);

ev_ce = FOREACH ev_pce GENERATE car_id,before_time,after_time, 0 AS flag1:int;

--input the transcation records and extract the cleaned deal records of ev.

data_deal1 = LOAD '$INPUT2' USING PigStorage(',') AS (date:chararray, car_id:chararray, mileage:long, uptime0:chararray, offtime0:chararray, money:double, unitprice:double, occudist:long, offdist:long,upLongi:double, upLati:double, downLongi:double, downLati:double, onBoardTime:long);

ev_deal1 = FOREACH data_deal1 GENERATE car_id,CONCAT(SUBSTRING(uptime0,0,23), 'Z') AS uptime:chararray, CONCAT(SUBSTRING(offtime0,0,23), 'Z') AS offtime:chararray;

data_deal2 = LOAD '$INPUT3' USING PigStorage(',') AS (date:chararray, car_id:chararray, mileage:long, uptime1:chararray, offtime1:chararray, money:double, unitprice:double, occudist:long, offdist:long,upLongi:double, upLati:double, downLongi:double, downLati:double, onBoardTime:long);

ev_deal2 = FOREACH data_deal2 GENERATE car_id,CONCAT(SUBSTRING(uptime1,0,23), 'Z') AS uptime1:chararray, CONCAT(SUBSTRING(offtime1,0,23), 'Z') AS offtime1:chararray;

ev_deal_un = UNION ev_deal1, ev_deal2;

ev_deal = FOREACH ev_deal_un GENERATE $0 AS car_id, $1 AS uptime1, $2 AS offtime1;

--filter the data from 10-16th, a week long
deal_filter = FILTER ev_deal BY (SUBSTRING(uptime1,8,10)=='10' OR SUBSTRING(uptime1,8,10)=='11' OR SUBSTRING(uptime1,8,10)=='12' OR SUBSTRING(uptime1,8,10)=='13' OR SUBSTRING(uptime1,8,10)=='14' OR SUBSTRING(uptime1,8,10)=='15' OR SUBSTRING(uptime1,8,10)=='16');

deal_filter_gen = FOREACH deal_filter GENERATE $0, $1, $2, 1 AS flag2:int;

--union PCE and deal data
data_un = UNION ev_ce, deal_filter_gen;

data_un_gen = FOREACH data_un GENERATE $0, $1, $2, $3;

data_un_od = ORDER data_un_gen BY $0, $1;

data_un_od_gen0 = FOREACH data_un_od GENERATE $0 AS car_id, $1 AS time1, $2 AS time2, $3 AS flag;
--filter ET
taxi_sz = FILTER data_un_od_gen0 BY car_id MATCHES '粤BD.*';

data_un_od_gen = FOREACH taxi_sz GENERATE $0 AS car_id, $1 AS time1, $2 AS time2, $3 AS flag;

--the near record is set as a pair. The format is (ELEM1,ELEM2)
grp_tmp = GROUP data_un_od_gen BY $0;

data_pairs = FOREACH grp_tmp{
	sort = ORDER data_un_od_gen BY time1;
	GENERATE FLATTEN(MarkovPairs(sort)) AS (ELEM1:TUPLE(car_id:chararray,time1:chararray,time2:chararray, flag:int), 
		ELEM2:TUPLE(car_id:chararray,time1:chararray,time2:chararray, flag:int));
	};


data_compare = FOREACH data_pairs GENERATE ELEM1.car_id AS car_id,ELEM1.time1 AS time1,ELEM1.time2 AS time2,ELEM1.flag AS flag,ELEM2.car_id AS car_id1:chararray,ELEM2.time1 AS time3,ELEM2.time2 AS time4,ELEM2.flag AS flag1;

--filter the latter one with flag is 0
data_flt = FILTER data_compare BY (flag1==0);

data_gen = FOREACH data_flt GENERATE $0 AS car_id, $1 AS time1, $2 AS time2, $3 AS flag, $4 AS car_id1:chararray, $5 AS time3, $6 AS time4, $7 AS flag1;

data_gen1 = FOREACH data_gen GENERATE $0 AS car_id, $1 AS time1, $2 AS time2, $3 AS flag, $5 AS time3, $6 AS time4, $7 AS flag1, ISOSecondsBetween(time3,time2) AS IdleTime;

data_gen2 = FILTER data_gen1 BY ((flag==1) AND (flag1==0) AND (IdleTime>=0));

data_gen3 = FOREACH data_gen2 GENERATE car_id, time3, time4, time1, time2, IdleTime;
--DUMP data_gen2
STORE data_gen3 INTO '$OUTPUT' USING PigStorage(',');