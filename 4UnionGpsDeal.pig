REGISTER /home/wangguang/jar/datafu-1.2.0.jar;
REGISTER /home/wangguang/jar/piggybank.jar;
REGISTER /home/wangguang/jar/joda-time-1.6.jar;
REGISTER /home/wangguang/jar/DeteCharEve.jar

DEFINE ISOSecondsBetween org.apache.pig.piggybank.evaluation.datetime.diff.ISOSecondsBetween(); 
DEFINE CalDistBetwChar2 deteChar.CalDistBetwChar2();

--Combine fine charging events and transaction data

--get the distances between two charging events
--the disances including with and without passengers

ev_pce = LOAD '$INPUT1' USING PigStorage(',') AS (car_id:chararray,before_time:chararray,after_time:chararray,distDiff:double,timeDiff:long,stop_lon:double,stop_lat:double,station_name:chararray);
ev_gps = FOREACH ev_pce GENERATE car_id,before_time,after_time,stop_lon,stop_lat,100000 AS occudis:long,100000 AS offdis:long;
--datatype: chararray,chararray,chararray,double,double,long,long
--input the records of transcations and extract the deal records of ev


-- --data_deal = LOAD '$INPUT2' USING PigStorage(',') AS (car_id:chararray,date:chararray,uptime:chararray,offtime:chararray,unitprice:long,occudist:long,ontime:chararray,money:double,offdist:long,type:chararray,company:chararray);
-- data_deal = LOAD '$INPUT2' USING PigStorage(',') AS (date:chararray, car_id:chararray, mileage:long, uptime:chararray, offtime:chararray, money:double, unitprice:double, occudist:long, offdist:long,upLongi:double, upLati:double, downLongi:double, downLati:double, onBoardTime:long);


data_deal1 = LOAD '$INPUT2' USING PigStorage(',') AS (date:chararray, car_id:chararray, mileage:long, uptime0:chararray, offtime0:chararray, money:double, unitprice:double, occudist:long, offdist:long,upLongi:double, upLati:double, downLongi:double, downLati:double, onBoardTime:long);

ev_deal1 = FOREACH data_deal1 GENERATE car_id,CONCAT(SUBSTRING(uptime0,0,23), 'Z') AS uptime:chararray, CONCAT(SUBSTRING(offtime0,0,23), 'Z') AS offtime:chararray, 0.0 AS ontime:double, money,occudist,offdist;

data_deal2 = LOAD '$INPUT3' USING PigStorage(',') AS (date:chararray, car_id:chararray, mileage:long, uptime1:chararray, offtime1:chararray, money1:double, unitprice:double, occudist1:long, offdist1:long,upLongi:double, upLati:double, downLongi:double, downLati:double, onBoardTime:long);

ev_deal2 = FOREACH data_deal2 GENERATE car_id,CONCAT(SUBSTRING(uptime1,0,23), 'Z') AS uptime1:chararray, CONCAT(SUBSTRING(offtime1,0,23), 'Z') AS offtime1:chararray, 0.0 AS ontime1:double, money1,occudist1,offdist1;

ev_deal_un = UNION ev_deal1, ev_deal2;

ev_deal = FOREACH ev_deal_un GENERATE $0 AS car_id, $1 AS uptime:chararray, $2 AS offtime:chararray,$3 AS ontime:double,$4 AS money, $5 AS occudist, $6 AS offdist;
--data type: chararray,chararray,chararray,double,double,long,long, transform the data of gps and deal into corresponding data type
--combine the charging event and transcation records by time series
--calculate the distance between two charging time of passenger and without passenger
union_gps_deal = UNION ev_deal,ev_gps;

--group the deal by license plate number
union_group = GROUP union_gps_deal BY $0;

data_pairs = FOREACH union_group{
	sort_pce = ORDER union_gps_deal BY $1;
	GENERATE FLATTEN(CalDistBetwChar2(sort_pce)) AS (ELEM1:TUPLE(car_id:chararray,before_time:chararray,after_time:chararray,stop_lon:double,stop_lat:double,occudis:long,offdis:long), 
		ELEM2:TUPLE(car_id:chararray,before_time:chararray,after_time:chararray,stop_lon:double,stop_lat:double,occudis:long,offdis:long),ELEM3:TUPLE(car_id:chararray,uptime:chararray,offtime:chararray,ontime:double,totalmoney:double,totaloccudist:long,totaloffdist:long));	
	};

data_cal = FOREACH data_pairs GENERATE ELEM1.car_id AS car_id,ELEM1.stop_lon AS stop_lon1,ELEM1.stop_lat AS stop_lat1,ELEM1.before_time AS before_time1,ELEM2.stop_lon AS stop_lon2,ELEM2.stop_lat AS stop_lat2,ELEM2.before_time AS before_time2,ISOSecondsBetween(ELEM1.after_time,ELEM1.before_time) AS charge_time,(int)ELEM3.totalmoney AS totalmoney,ELEM3.totaloccudist AS totaloccudist,ELEM3.totaloffdist AS totaloffdist;


data_cal_flt = FILTER data_cal BY ($9!=0);

STORE data_cal_flt INTO '$OUTPUT' USING PigStorage(','); 