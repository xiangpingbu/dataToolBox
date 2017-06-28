REGISTER '/home/xiangping/piglib/piggybank-0.15.0.jar'
DEFINE CSVExcelStorage org.apache.pig.piggybank.storage.CSVExcelStorage;
SET default_parallel 10;
customer  = LOAD '/users/xiangping/qianmi/datasource/customer_no.csv' USING CSVExcelStorage(',', 'NO_MULTILINE', 'UNIX', 'SKIP_INPUT_HEADER')
                        AS (customer_no:chararray);
teldata  = LOAD '/users/xiangping/qianmi/datasource/teldata.csv' USING CSVExcelStorage(',', 'NO_MULTILINE', 'UNIX', 'SKIP_INPUT_HEADER')
                        AS (customer_no:chararray,
                                id_card:chararray,
                                phone:chararray,
                                user_name:chararray,
                                trade_type:int,
                                call_time:chararray,
                                receive_phone:chararray,
                                trade_time:int,
                                call_type: int,
                                trade_addr:chararray);

filter_teldata= JOIN customer BY customer_no LEFT OUTER, teldata BY customer_no;
grp_teldata = GROUP filter_teldata BY teldata::customer_no;
teldata_agg = FOREACH grp_teldata {
                                l = LIMIT filter_teldata 1;
                                trade_type_one = FILTER filter_teldata BY teldata::trade_type==1;
                                trade_type_two = FILTER filter_teldata BY teldata::trade_type==2;
                                call_type_one = FILTER filter_teldata BY teldata::call_type==1;
                                call_type_two = FILTER filter_teldata BY teldata::call_type==2;

                                GENERATE group AS customer_no,
                                                        FLATTEN(l.teldata::phone) AS phone_number,

                                                        COUNT(trade_type_one.trade_type) AS trade_type_one_count,
                                                        COUNT(trade_type_two.trade_type) AS trade_type_two_count,
                                                        COUNT(call_type_one.call_type) AS call_type_one_count,
                                                        COUNT(call_type_two.call_type) AS call_type_two_count,

                                                        COUNT(filter_teldata.trade_time) AS total_count_trade_time,
                                                        SUM(filter_teldata.trade_time) AS total_sum_trade_time,
                                                        AVG(filter_teldata.trade_time) AS total_avg_trade_time,

                                                        SUM(trade_type_one.trade_time) AS trade_one_total_sum_trade_time,
                                                        AVG(trade_type_one.trade_time) AS trade_one_avg_sum_trade_time,
                                                        SUM(trade_type_two.trade_time) AS trade_two_total_sum_trade_time,
                                                        AVG(trade_type_two.trade_time) AS trade_two_avg_sum_trade_time,

                                                        SUM(call_type_one.trade_time) AS call_one_total_sum_trade_time,
                                                        AVG(call_type_one.trade_time) AS call_one_avg_sum_trade_time,
                                                        SUM(call_type_two.trade_time) AS call_two_total_sum_trade_time,
                                                        AVG(call_type_two.trade_time) AS call_two_avg_sum_trade_time;
        }

STORE teldata_agg INTO '/users/xiangping/qianmi/result/teldatafilter/' USING PigStorage(',','-schema');
