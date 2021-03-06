REGISTER '/home/xiangping/piglib/piggybank-0.15.0.jar'
DEFINE CSVExcelStorage org.apache.pig.piggybank.storage.CSVExcelStorage;
SET default_parallel 30;

customer  = LOAD '/users/xiangping/qianmi/datasource/customer_no.csv' USING CSVExcelStorage(',', 'NO_MULTILINE', 'UNIX', 'SKIP_INPUT_HEADER') AS (customer_no:chararray);
msgdata  = LOAD '/users/xiangping/qianmi/datasource/msgdata.csv' USING CSVExcelStorage(',', 'NO_MULTILINE', 'UNIX', 'SKIP_INPUT_HEADER')
                        AS (customer_no:chararray,
                                id_card:chararray,
                                phone:chararray,
                                user_name:chararray,
                                trade_way:int,
                                receiver_phone:chararray,
                                send_time:chararray);

filter_msgdata = JOIN customer BY customer_no LEFT OUTER, msgdata BY customer_no;
grp_msgdata = GROUP filter_msgdata BY msgdata::customer_no;
msgdata_agg = FOREACH grp_msgdata {
                                l = LIMIT filter_msgdata 1;
                                trade_way_one = FILTER filter_msgdata BY msgdata::trade_way==1;
                                trade_way_two = FILTER filter_msgdata BY msgdata::trade_way==2;
                                GENERATE         group AS customer_no,
                                                 FLATTEN(l.msgdata::phone) AS phone_number,
                                                 COUNT(trade_way_one.trade_way) AS way_one_count,
                                                 COUNT(trade_way_two.trade_way) AS way_two_count;
        }
STORE msgdata_agg INTO '/users/xiangping/qianmi/result/msgdatafilter2/' USING PigStorage(',','-schema');
