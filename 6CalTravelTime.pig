REGISTER /home/wangguang/jar/datafu-1.2.0.jar;
REGISTER /home/wangguang/jar/piggybank.jar;
REGISTER /home/wangguang/jar/joda-time-1.6.jar;
REGISTER /home/wangguang/jar/DeteCharEve.jar
DEFINE MarkovPairs datafu.pig.stats.MarkovPairs();
DEFINE HaversineDistInMiles datafu.pig.geo.HaversineDistInMiles(); 
DEFINE ISOSecondsBetween org.apache.pig.piggybank.evaluation.datetime.diff.ISOSecondsBetween();
DEFINE ABS org.apache.pig.piggybank.evaluation.math.ABS();
DEFINE ISOToUnix org.apache.pig.piggybank.evaluation.datetime.convert.ISOToUnix();
DEFINE CustomFormatToISO org.apache.pig.piggybank.evaluation.datetime.convert.CustomFormatToISO();
DEFINE GetDateHour deteChar.GetDateHour();
DEFINE getMaxHour deteChar.getMaxHour();
DEFINE GetDate deteChar.GetDate();
DEFINE DeteLastDrop deteChar.DeteLastDrop();
SET default_parallel 1;
SET pig.tmpfilecompression true
SET pig.tmpfilecompression.codec gz

--AllCharEvent as input
ev_ace = LOAD '$INPUT1' USING PigStorage(',') AS (car_id:chararray,before_time:chararray,after_time:chararray,distDiff:double,timeDiff:long,stop_lon:double,stop_lat:double,flag:int,station_name:chararray);

data_ace = FOREACH ev_ace GENERATE car_id,before_time,after_time;

--input the transcation records and extract the cleaned deal records of ev. 

data_deal = LOAD '$INPUT2' USING PigStorage(',') AS (car_id:chararray,date:chararray,uptime:chararray,offtime:chararray,unitprice:long,occudist:long,ontime:chararray,money:double,offdist:long,type:chararray,company:chararray);
ev_deal_flt = FILTER data_deal BY (type=='蓝的');
ev_deal = FOREACH ev_deal_flt GENERATE car_id,uptime,offtime;
/*
-- deal data before cleaning
data_deal = LOAD '$INPUT2' USING PigStorage(',') AS (car_id:chararray,uptime:chararray,offtime:chararray,unitprice:long,occumile:long,ontime:chararray,money:long,offmile:long, cardNum:chararray, cardMoney:int,leftMoney:int, offMun:int, time1:chararray, a:int, b:int, c:chararray, d:chararray, e:chararray);
ev_deal = FOREACH data_deal GENERATE car_id,CustomFormatToISO(uptime,'YY-MM-dd HH:mm:ss') AS uptime:chararray, CustomFormatToISO(offtime,'YY-MM-dd HH:mm:ss') AS offtime:chararray;
*/
--combine AllCharEvent and the data of deal
ce_deal = JOIN data_ace BY car_id, ev_deal BY car_id;
--
ce_deal_gen = FOREACH ce_deal GENERATE $0 AS id:chararray,$1 AS char_before:chararray,$2 AS char_after:chararray,$4 AS uptime:chararray, $5 AS offtime:chararray;
--char_before- passenger offtime
ce_deal_gen1 = FOREACH ce_deal_gen GENERATE id,char_before,char_after,uptime,offtime,ISOSecondsBetween(char_before,offtime) AS travelTime:long;
--filter the data by time > 0
ce_gen_flt = FILTER ce_deal_gen1 BY travelTime>0;
--
ce_gen_flt1 = FOREACH ce_gen_flt GENERATE id,char_before,char_after,uptime,offtime,travelTime;

ce_grp = GROUP ce_gen_flt1 BY (id,char_before);

travel_time = FOREACH ce_grp {
	sort = ORDER ce_gen_flt1 BY travelTime;
	top = limit sort 1;
	GENERATE FLATTEN(top.id) AS id:chararray,FLATTEN(top.char_before) AS char_before:chararray,FLATTEN(top.char_after) AS char_after:chararray,FLATTEN(top.uptime) AS uptime:chararray,FLATTEN(top.offtime) AS offtime:chararray,FLATTEN(top.travelTime) AS travelTime:long;
};

STORE travel_time INTO '$OUTPUT' USING PigStorage(','); 