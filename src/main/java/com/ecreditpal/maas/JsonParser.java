package com.ecreditpal.maas;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import org.json.JSONObject;

import java.io.IOException;

/**
 * Created by xpbu on 26/06/2017.
 */
public class JsonParser extends EvalFunc<String>
{
    public String exec(Tuple input) throws IOException {
        if (input == null || input.size() == 0)
            return null;
        try{
            String str = (String)input.get(0);
            String field = (String)input.get(1);
            JSONObject jo = new JSONObject(str);
            String ret = jo.get(field).toString();
            return ret;
        }catch(Exception e){
            throw new IOException("Caught exception processing input row ", e);
        }
    }

    public static void main(String[] args) {
        String test = "{\"content\": {\"monthly_consumption\": [{\"call_consumption\": \"83.00\"}],\"head_info\": {\"user_type\": 1,\"search_id\": \"14697858861011544153\",\"report_time\": \"2016-07-29 17:53\"}},\"status\": 200,\"statusMsg\": \"\"}";
        JSONObject jo = new JSONObject(test);
        String a = jo.get("content").toString();
        System.out.println(a);
    }

}
