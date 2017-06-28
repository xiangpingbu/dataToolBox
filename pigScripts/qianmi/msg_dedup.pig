msgdata  = LOAD '/users/xiangping/qianmi/result/msgdatafilter/' USING PigStorage(',','-schema');
grp_msgdata = GROUP msgdata BY customer_no;
msgdata_agg = FOREACH grp_msgdata {
				l = LIMIT msgdata 1;
				GENERATE FLATTEN(l.customer_no) AS customer_no, 
						 FLATTEN(l.phone_number) AS phone_number, 
						 FLATTEN(l.way_one_count) AS way_one_count, 
						 FLATTEN(l.way_two_count) AS way_two_count;
	} 
STORE msgdata_agg INTO '/users/xiangping/qianmi/result/msgdatafilterdedup/' USING PigStorage(',','-schema');
