package io.transwarp.udf;

import com.alibaba.fastjson.JSONObject;
import org.apache.hadoop.hive.ql.exec.UDF;
import redis.clients.jedis.Jedis;
import io.transwarp.udaf.jsonUtils;

public class updateRedis extends UDF {

    private static Jedis jedis = new Jedis("10.11.220.15", 6379, 10000);
    private static JSONObject json=JSONObject.parseObject("{}");
    private static String hget;
    private static String ds2 ;
    private static String city2 ;
    private static String dist2 ;
    private static String keyCity2;
    private static String pc2;
    private static String prov2;

    public String evaluate(String id, String ds, String countryallcnt, String countryallprice, String city, String dist, String keyCity, String pc, String prov, String sString, String secondspeed, String flag, String sp) {

        hget = jedis.hget("JSON_STORE", "cachedD" + id);

        if(hget==null||hget.equals("null")){
            if(ds.equals("")){
                json.put("countryAllCnt",countryallcnt);
                json.put("countryAllPrice",countryallprice);
                json.put("s",sString);
                json.put("secondSpeed",secondspeed);
                json.put("duplicateCnt",flag);
                json.put("sp",sp);
                jedis.hset("JSON_STORE","cachedD"+id,json.toString());

                return "update cachedD" + id +" sucess!";

            }else{
                json.put("ds",JSONObject.parseObject(ds).get("ds"));
                json.put("city",JSONObject.parseObject(city).get("city"));
                json.put("dist",JSONObject.parseObject(dist).get("dist"));
                json.put("keyCity",JSONObject.parseObject(keyCity).get("keyCity"));
                json.put("pc",JSONObject.parseObject(pc).get("pc"));
                json.put("prov",JSONObject.parseObject(prov).get("prov"));
                json.put("countryAllCnt",countryallcnt);
                json.put("countryAllPrice",countryallprice);
                json.put("s",sString);
                json.put("secondSpeed",secondspeed);
                json.put("sp",sp);
                json.put("duplicateCnt",flag);//
                jedis.hset("JSON_STORE","cachedD"+id,json.toString());

                return "update cachedD" + id +" sucess!";

            }
        }
        else{
            json=JSONObject.parseObject(hget);
            if (json.containsKey("ds")&&!ds.equals("")) {
                //将redis中的值和传过来的字符串拼接
                ds2 = jsonUtils.dsAdd("{\"ds\":" +json.getJSONObject("ds")+"}", ds);
                city2 = jsonUtils.cityAdd("{\"city\":" +json.getJSONObject("city")+"}", city);
                dist2 = jsonUtils.distAdd("{\"dist\":" +json.getJSONObject("dist")+"}", dist);
                keyCity2 = jsonUtils.keyCityAdd("{\"keyCity\":" +json.getJSONObject("keyCity")+"}", keyCity);
                pc2= jsonUtils.pcAdd("{\"pc\":" +json.getJSONObject("pc")+"}", pc);
                prov2 = jsonUtils.provAdd("{\"prov\":" +json.getJSONObject("prov")+"}", prov);
                //put进去
                json.put("ds",JSONObject.parseObject(ds2).getJSONObject("ds"));
                json.put("city",JSONObject.parseObject(city2).getJSONObject("city"));
                json.put("dist",JSONObject.parseObject(dist2).getJSONObject("dist"));
                json.put("keyCity",JSONObject.parseObject(keyCity2).getJSONObject("keyCity"));
                json.put("pc",JSONObject.parseObject(pc2).getJSONObject("pc"));
                json.put("prov",JSONObject.parseObject(prov2).getJSONObject("prov"));
                json.put("countryAllCnt",json.getFloat("countryAllCnt") + Float.parseFloat(countryallcnt));
                json.put("countryAllPrice",json.getFloat("countryAllPrice") + Float.parseFloat(countryallprice));
                json.put("s",sString);
                json.put("secondSpeed",secondspeed);
                json.put("sp",sp);
                json.put("duplicateCnt",json.getFloat("duplicateCnt") + Float.parseFloat(flag));//
                jedis.hset("JSON_STORE","cachedD"+id,json.toString());

                return "update cachedD" + id +" sucess!";

            }else if(!json.containsKey("ds")&&!ds.equals("")){
                json.put("ds",JSONObject.parseObject(ds).get("ds"));
                json.put("city",JSONObject.parseObject(city).get("city"));
                json.put("dist",JSONObject.parseObject(dist).get("dist"));
                json.put("keyCity",JSONObject.parseObject(keyCity).get("keyCity"));
                json.put("pc",JSONObject.parseObject(pc).get("pc"));
                json.put("prov",JSONObject.parseObject(prov).get("prov"));
                json.put("countryAllCnt",json.getFloat("countryAllCnt") + Float.parseFloat(countryallcnt));
                json.put("countryAllPrice",json.getFloat("countryAllPrice") + Float.parseFloat(countryallprice));
                json.put("s",sString);
                json.put("secondSpeed",secondspeed);
                json.put("sp",sp);
                json.put("duplicateCnt",json.getFloat("duplicateCnt") + Float.parseFloat(flag));//
                jedis.hset("JSON_STORE","cachedD"+id,json.toString());

                return "update cachedD" + id +" sucess!";

            }else if(!json.containsKey("ds")&&ds.equals("")){
                json.put("countryAllCnt",json.getFloat("countryAllCnt") + Float.parseFloat(countryallcnt));
                json.put("countryAllPrice",json.getFloat("countryAllPrice") + Float.parseFloat(countryallprice));
                json.put("s",sString);
                json.put("secondSpeed",secondspeed);
                json.put("sp",sp);
                json.put("duplicateCnt",json.getFloat("duplicateCnt") + Float.parseFloat(flag));//
                jedis.hset("JSON_STORE","cachedD"+id,json.toString());

                return "update cachedD" + id +" sucess!";

            }else if(json.containsKey("ds")&&ds.equals("")){
                json.put("countryAllCnt",json.getFloat("countryAllCnt") + Float.parseFloat(countryallcnt));
                json.put("countryAllPrice",json.getFloat("countryAllPrice") + Float.parseFloat(countryallprice));
                json.put("s",sString);
                json.put("secondSpeed",secondspeed);
                json.put("sp",sp);
                json.put("duplicateCnt",json.getFloat("duplicateCnt") + Float.parseFloat(flag));//
                jedis.hset("JSON_STORE","cachedD"+id,json.toString());
                return "update cachedD" + id +" sucess!";
            }
        }
        return "update error!";
    }
}