REGISTER '/home/xiangping/piglib/piggybank-0.15.0.jar'
DEFINE CSVExcelStorage org.apache.pig.piggybank.storage.CSVExcelStorage;
SET default_parallel 20;

customer  = LOAD '/users/xiangping/qianmi/datasource/customer_no.csv' USING CSVExcelStorage(',', 'NO_MULTILINE', 'UNIX', 'SKIP_INPUT_HEADER') AS (customer_no:chararray);
billdata  = LOAD '/users/xiangping/qianmi/datasource/billdata.csv' USING CSVExcelStorage(',', 'NO_MULTILINE', 'UNIX', 'SKIP_INPUT_HEADER') AS (customer_no:chararray,id_card:chararray,phone:chararray,user_name:chararray,call_pay:double,month:int);

filter_billdata = JOIN customer BY customer_no LEFT OUTER, billdata BY customer_no;
grp_billdata = GROUP filter_billdata BY billdata::customer_no;
billdata_agg = FOREACH grp_billdata {
				sorted = ORDER filter_billdata BY month DESC;
				l_6m = LIMIT sorted 6;
				l_3m = LIMIT sorted 3;
				l_1m = LIMIT sorted 1;
				GENERATE FLATTEN(l_1m.customer::customer_no), AVG(l_6m.call_pay), MAX(l_6m.call_pay), MIN(l_6m.call_pay), (MAX(l_6m.call_pay)-MIN(l_6m.call_pay)) ,AVG(l_3m.call_pay), AVG(l_1m.call_pay);
	} 
STORE billdata_agg INTO '/users/xiangping/qianmi/result/billdatafilter/' USING PigStorage(',','-schema');


