
REGISTER '/home/xiangping/piglib/piggybank-0.15.0.jar'
DEFINE CSVExcelStorage org.apache.pig.piggybank.storage.CSVExcelStorage;
customer  = LOAD '/users/xiangping/qianmi/datasource/customer_no.csv' USING CSVExcelStorage(',', 'NO_MULTILINE', 'UNIX', 'SKIP_INPUT_HEADER') 
			AS (customer_no:chararray);
userdata  = LOAD '/users/xiangping/qianmi/datasource/userdata.csv' USING CSVExcelStorage(',', 'NO_MULTILINE', 'UNIX', 'SKIP_INPUT_HEADER') 
			AS (customer_no:chararray,
				user_source:chararray,
				update_time:chararray,
				addr:chararray,
				id_card:chararray,
				reg_time:chararray,
				real_name:chararray,
				phone:chararray,
				phone_remain:chararray);
			
filter_userdata = JOIN customer BY customer_no LEFT OUTER, userdata BY customer_no;
grp_userdata = GROUP filter_userdata BY userdata::customer_no;
userdata_agg = FOREACH grp_userdata {
				l = LIMIT filter_userdata 1;
				GENERATE FLATTEN(l.customer::customer_no) AS customer_no, 
						 FLATTEN(l.userdata::user_source) AS user_source,
						 FLATTEN(l.userdata::update_time) AS update_time,
						 FLATTEN(l.userdata::addr) AS addr,
						 FLATTEN(l.userdata::id_card) AS id_card,
						 FLATTEN(l.userdata::reg_time) AS reg_time,
						 FLATTEN(l.userdata::real_name) AS real_name,
						 FLATTEN(l.userdata::phone) AS phone,
						 FLATTEN(l.userdata::phone_remain) AS phone_remain;
	}
STORE userdata_agg INTO '/users/xiangping/qianmi/result/userdatafilter/' USING PigStorage(',','-schema');
