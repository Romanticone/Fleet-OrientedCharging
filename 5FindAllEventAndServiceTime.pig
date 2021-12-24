-- We can obtain all Charging Evenets and Service Time

REGISTER /home/wangguang/jar/datafu-1.2.0.jar;
REGISTER /home/wangguang/jar/piggybank.jar;
REGISTER /home/wangguang/jar/joda-time-1.6.jar;
REGISTER /home/wangguang/jar/DeteCharEve.jar

DEFINE getMaxHour keepTwoPoint.getMaxHour(); 
DEFINE KeepTwoDecimal keepTwoPoint.KeepTwoDecimal();
DEFINE GetDateHour deteChar.GetDateHour();
DEFINE ISOSecondsBetween org.apache.pig.piggybank.evaluation.datetime.diff.ISOSecondsBetween(); 
DEFINE DeteStation1 deteChar.DeteStation1();
DEFINE DeleLess2Hour deteChar.DeleLess2Hour();
DEFINE DeleLess2Hour1 deteChar.DeleLess2Hour1();
--PCE
ev_pce = LOAD '$INPUT1' USING PigStorage(',') AS (car_id:chararray,car_lon:double,car_lat:double,before_time:chararray,after_time:chararray,distDiff:double,timeDiff:long,stop_lon:double,stop_lat:double);
ev_ce = FOREACH ev_pce GENERATE car_id,car_lon,car_lat,before_time,after_time,distDiff,timeDiff,stop_lon,stop_lat;

--unionGpsDeal
ev_union = LOAD '$INPUT2' USING PigStorage(',') AS (car_id:chararray,stop_lon1:double,stop_lat1:double,before_time1:chararray,stop_lon2:double,stop_lat2:double,before_time2:chararray,charge_time:long,totalmoney:int,totaloccudist:long,totaloffdist:long);


ev_fce = LOAD '$INPUT3' USING PigStorage(',') AS (car_id:chararray,before_time:chararray,after_time:chararray,distDiff:double,timeDiff:long,stop_lon:double,stop_lat:double,station_name:chararray);

ev_fce1 = LOAD '$INPUT4' USING PigStorage(',') AS (car_id:chararray,before_time:chararray,after_time:chararray,distDiff:double,timeDiff:long,stop_lon:double,stop_lat:double,station_name:chararray);


union_simple = FOREACH ev_union GENERATE car_id, stop_lon1,stop_lat1,before_time1,before_time2,(double)charge_time/3600 AS chargeHour,totalmoney AS totalRMB, totaloccudist AS occuKM, totaloffdist AS offKM;

plot_each = FOREACH union_simple GENERATE car_id,before_time1,before_time2,getMaxHour(chargeHour) AS chargehour,totalRMB,occuKM,offKM,(occuKM+offKM) AS totalKM;

--filter distance >300 per time
union_flt = FILTER plot_each BY (totalKM>300);

--jion by id
join1 = JOIN ev_ce BY $0, union_flt BY $0;
-- 
find_other = FILTER join1 BY ((ISOSecondsBetween(ev_ce::before_time,union_flt::before_time1)>0) and (ISOSecondsBetween(union_flt::before_time2,ev_ce::before_time)>0)); 

other1 = FOREACH find_other GENERATE ev_ce::car_id,ev_ce::before_time,ev_ce::after_time,ev_ce::distDiff,ev_ce::timeDiff,ev_ce::stop_lon,ev_ce::stop_lat,0 AS flag:int,'slowSta' AS slow_station:chararray;
--Add the name of charging station as slowSta
other = FOREACH other1 GENERATE car_id,before_time,after_time,distDiff,timeDiff,stop_lon,stop_lat,flag,slow_station;
--FCE
ev_fc = FOREACH ev_fce GENERATE car_id,before_time,after_time,distDiff,timeDiff,stop_lon,stop_lat,1 AS flag1:int,station_name;
--combine into the charging events with all possibilities
union_all = UNION ev_fc,other;

--group the data by the license plate number
union_group = GROUP union_all BY $0;

data_pairs = FOREACH union_group{
	sort_pce = ORDER union_all BY $1;
	GENERATE FLATTEN(DeleLess2Hour(sort_pce)) AS (ELEM1:TUPLE(car_id:chararray,before_time:chararray,after_time:chararray,distDiff:double,timeDiff:long,stop_lon:double,stop_lat:double,flag:int,station_name:chararray),
		ELEM2:TUPLE(car_id:chararray,before_time:chararray,after_time:chararray,distDiff:double,timeDiff:long,stop_lon:double,stop_lat:double,flag:int,station_name:chararray));	
	};
--delete non-charging events
data_cal = FOREACH data_pairs GENERATE ELEM2.car_id AS car_id,ELEM2.before_time AS before_time,ELEM2.after_time AS after_time,ELEM2.distDiff AS distDiff,ELEM2.timeDiff AS timeDiff,ELEM2.stop_lon AS stop_lon,ELEM2.stop_lat AS stop_lat,ELEM2.flag AS flag,ELEM2.station_name AS station_name;
--
data_disti = JOIN union_all BY ($0,$1) LEFT OUTER, data_cal BY ($0,$1);

data_flt = FILTER data_disti BY (data_cal::before_time IS NULL);

all_CE1 = FOREACH data_flt GENERATE $0,$1,$2,$3,$4,$5,$6,$7,$8;
--------------------------------------------------
--The above two steps is to delete PCEs which are not in the charging station.
--The following step is to delete the PCE in the charging station
--Since it will not charge again in only two hours. The previous step is considered as waiting in the queue. 

union_group2 = GROUP all_CE1 BY $0;

data_pairs2 = FOREACH union_group2{
	sort_pce2 = ORDER all_CE1 BY $1;
	GENERATE FLATTEN(DeleLess2Hour1(sort_pce2)) AS (ELEM1:TUPLE(car_id:chararray,before_time:chararray,after_time:chararray,distDiff:double,timeDiff:long,stop_lon:double,stop_lat:double,flag:int,station_name:chararray),
		ELEM2:TUPLE(car_id:chararray,before_time:chararray,after_time:chararray,distDiff:double,timeDiff:long,stop_lon:double,stop_lat:double,flag:int,station_name:chararray));	
	};
--delete the non-charging event and the previous one
data_cal2 = FOREACH data_pairs2 GENERATE ELEM1.car_id AS car_id,ELEM1.before_time AS before_time,ELEM1.after_time AS after_time,ELEM1.distDiff AS distDiff,ELEM1.timeDiff AS timeDiff,ELEM1.stop_lon AS stop_lon,ELEM1.stop_lat AS stop_lat,ELEM1.flag AS flag,ELEM1.station_name AS station_name;
--
data_disti2 = JOIN all_CE1 BY ($0,$1) LEFT OUTER, data_cal2 BY ($0,$1);
data_flt2 = FILTER data_disti2 BY (data_cal2::before_time IS NULL);
all_CE2 = FOREACH data_flt2 GENERATE $0,$1,$2,$3,$4,$5,$6,$8;

--union FCE
all_union = UNION ev_fce1, all_CE2;

all_union_gen = FOREACH all_union GENERATE $0,$1,$2,$3,$4,$5,$6,$7;
--drop the repeated lines
all_union_gen_dist = DISTINCT all_union_gen;

all_union_gen1 = FOREACH all_union_gen_dist GENERATE $0,$1,$2,$3,$4,$5,$6,$7;

--store all the charging events
STORE all_union_gen1 INTO '$OUTPUT' USING PigStorage(','); 