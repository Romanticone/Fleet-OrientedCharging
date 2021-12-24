REGISTER /home/wangguang/jar/datafu-1.2.0.jar;
REGISTER /home/wangguang/jar/piggybank.jar;
REGISTER /home/wangguang/jar/joda-time-1.6.jar;
REGISTER /home/wangguang/jar/DeteCharEve.jar


DEFINE ISOSecondsBetween org.apache.pig.piggybank.evaluation.datetime.diff.ISOSecondsBetween(); 
--calculate per-time charging cost
DEFINE CalCost deteChar.CalCost(); 
--calculate charging time in each slot(half hour), so we have 48 points
DEFINE CalSlot deteChar.CalSlot();
--calculate energy consumption in each slot(half hour), so we have 48 points
DEFINE CalEnergy deteChar.CalEnergy();
--calculate charging cost in each slot(half hour), so we have 48 points
DEFINE CalCostHour deteChar.CalCostHour();


ev_ce = LOAD '$INPUT' USING PigStorage(',') AS (car_id:chararray,before_time:chararray,after_time:chararray,distDiff:double,timeDiff:long,stop_lon:double,stop_lat:double,station_name:chararray);

event_flt = FOREACH ev_ce GENERATE car_id,SUBSTRING(before_time,11,13) AS char_hour;

event_flt_grp = GROUP event_flt BY char_hour;

event_flt_grp_gen = FOREACH event_flt_grp GENERATE FLATTEN(group), COUNT(event_flt.$0);


DUMP event_flt_grp_gen