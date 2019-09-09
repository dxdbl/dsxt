package io.transwarp.udaf;

import com.alibaba.fastjson.JSONObject;

public class test2 {
    public static JSONObject json=JSONObject.parseObject("{}");
    public static void main(String[] args) {
        String ds="{\"ds\":{\"10000\":{\"all\":1,\"allPrice\":224,\"prov\":{\"110000\":{\"rec\":0,\"sed\":1,\"sedPrice\":224,\"city\":{\"110113\":{\"rec\":0,\"sed\":1,\"sedPrice\":224,\"dist\":{\"110113\":{\"rec\":0,\"sed\":1,\"sedPrice\":224}}}}},\"440000\":{\"rec\":1,\"sed\":0,\"sedPrice\":0,\"city\":{\"445200\":{\"rec\":1,\"sed\":0,\"sedPrice\":0,\"dist\":{\"445202\":{\"rec\":1,\"sed\":0,\"sedPrice\":0}}}}}}}}}";

        String city="{\"city\":{\"110113\":{\"rec\":0,\"sed\":1,\"sedPrice\":224},\"445200\":{\"rec\":1,\"sed\":0,\"sedPrice\":0}}}";
        String dist="{\"dist\":{\"110113\":{\"rec\":0,\"sed\":1,\"sedPrice\":224},\"445202\":{\"rec\":1,\"sed\":0,\"sedPrice\":0}}}";
        String keyCity="{\"keyCity\": {\"110113\": {\"fromCity\": {},\"fromProv\": {},\"toCity\": {\"440000\": 1},\"toProv\": {\"445200\": 1} },\"445200\": {\"fromCity\": {\"110000\": 1},\"fromProv\": {\"110113\": 1 },\"toCity\": {},\"toProv\": {}}}}";
        String pc="{\n" +
                "    \"pc\": {\n" +
                "        \"30\": {\n" +
                "            \"all\": 1,\n" +
                "            \"allPrice\": 224,\n" +
                "            \"prov\": {\n" +
                "                \"330000\": {\n" +
                "                    \"city\": {\n" +
                "                        \"330100\": {\n" +
                "                            \"dist\": {\n" +
                "                                \"330101\": {\n" +
                "                                    \"rec\": 1,\n" +
                "                                    \"sed\": 0,\n" +
                "                                    \"sedPrice\": 0\n" +
                "                                }\n" +
                "                            },\n" +
                "                            \"rec\": 1,\n" +
                "                            \"sed\": 0,\n" +
                "                            \"sedPrice\": 0\n" +
                "                        }\n" +
                "                    },\n" +
                "                    \"rec\": 1,\n" +
                "                    \"sed\": 0,\n" +
                "                    \"sedPrice\": 0\n" +
                "                },\n" +
                "                \"370000\": {\n" +
                "                    \"city\": {\n" +
                "                        \"370100\": {\n" +
                "                            \"dist\": {\n" +
                "                                \"370101\": {\n" +
                "                                    \"rec\": 0,\n" +
                "                                    \"sed\": 1,\n" +
                "                                    \"sedPrice\": 224\n" +
                "                                }\n" +
                "                            },\n" +
                "                            \"rec\": 0,\n" +
                "                            \"sed\": 1,\n" +
                "                            \"sedPrice\": 224\n" +
                "                        }\n" +
                "                    },\n" +
                "                    \"rec\": 0,\n" +
                "                    \"sed\": 1,\n" +
                "                    \"sedPrice\": 224\n" +
                "                }\n" +
                "            }\n" +
                "        }\n" +
                "    }\n" +
                "}";
        String prov="{\"prov\":{\"440000\":{\"rec\":1,\"sed\":0,\"sedPrice\":0,\"to\":{}},\"110000\":{\"rec\":0,\"sed\":1,\"sedPrice\":224,\"to\":{\"440000\":1,\"city\":{\"445200\":1},\"dist\":{\"445202\":1}}}}}";
        String countryallcnt="192594";
        String countryallprice="29901259";
        String sString="true";
        String secondspeed="1555";
        String sp="sp";
        String flag="2";

        String hget = "{\"countryAllPrice\":\"29901259\",\"countryAllCnt\":\"192594\",\"pc\":{\"30\":{\"all\":1,\"allPrice\":224,\"prov\":{\"370000\":{\"rec\":0,\"sed\":1,\"city\":{\"370100\":{\"rec\":0,\"sed\":1,\"dist\":{\"370101\":{\"rec\":0,\"sed\":1,\"sedPrice\":224}},\"sedPrice\":224}},\"sedPrice\":224},\"330000\":{\"rec\":1,\"sed\":0,\"city\":{\"330100\":{\"rec\":1,\"sed\":0,\"dist\":{\"330101\":{\"rec\":1,\"sed\":0,\"sedPrice\":0}},\"sedPrice\":0}},\"sedPrice\":0}}}},\"s\":\"true\",\"secondSpeed\":\"1555\",\"city\":{\"110113\":{\"rec\":0,\"sed\":1,\"sedPrice\":224},\"445200\":{\"rec\":1,\"sed\":0,\"sedPrice\":0}},\"keyCity\":{\"110113\":{\"toProv\":{\"445200\":1},\"toCity\":{\"440000\":1},\"fromCity\":{},\"fromProv\":{}},\"445200\":{\"toProv\":{},\"toCity\":{},\"fromCity\":{\"110000\":1},\"fromProv\":{\"110113\":1}}},\"duplicateCnt\":\"2\",\"dist\":{\"110113\":{\"rec\":0,\"sed\":1,\"sedPrice\":224},\"445202\":{\"rec\":1,\"sed\":0,\"sedPrice\":0}},\"prov\":{\"110000\":{\"rec\":0,\"sed\":1,\"sedPrice\":224,\"to\":{\"city\":{\"445200\":1},\"dist\":{\"445202\":1},\"440000\":1}},\"440000\":{\"rec\":1,\"sed\":0,\"sedPrice\":0,\"to\":{}}},\"sp\":\"sp\",\"ds\":{\"10000\":{\"all\":1,\"allPrice\":224,\"prov\":{\"110000\":{\"rec\":0,\"sed\":1,\"city\":{\"110113\":{\"rec\":0,\"sed\":1,\"dist\":{\"110113\":{\"rec\":0,\"sed\":1,\"sedPrice\":224}},\"sedPrice\":224}},\"sedPrice\":224},\"440000\":{\"rec\":1,\"sed\":0,\"city\":{\"445200\":{\"rec\":1,\"sed\":0,\"dist\":{\"445202\":{\"rec\":1,\"sed\":0,\"sedPrice\":0}},\"sedPrice\":0}},\"sedPrice\":0}}}}}";

//        hget="{\n" +
//                "    \"countryAllCnt\": \"192594\",\n" +
//                "    \"countryAllPrice\": \"29901259\",\n" +
//                "    \"duplicateCnt\": \"2\",\n" +
//                "    \"s\": \"true\",\n" +
//                "    \"secondSpeed\": \"1555\",\n" +
//                "    \"sp\": \"sp\"\n" +
//                "}";
        ds="";

        if(hget==null||hget.equals("null")){
            if(ds.equals("")){
                json.put("countryAllCnt",countryallcnt);
                json.put("countryAllPrice",countryallprice);
                json.put("s",sString);
                json.put("secondSpeed",secondspeed);
                json.put("duplicateCnt",flag);
                json.put("sp",sp);
                System.out.println(json);
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
                System.out.println(json);
            }
        }else{
            json=JSONObject.parseObject(hget);
            if (json.containsKey("ds")&&!ds.equals("")) {
                //将redis中的值和传过来的字符串拼接
                String ds2 = jsonUtils.dsAdd("{\"ds\":" +json.getJSONObject("ds")+"}", ds);
                String city2 = jsonUtils.cityAdd("{\"city\":" +json.getJSONObject("city")+"}", city);
                String dist2 = jsonUtils.distAdd("{\"dist\":" +json.getJSONObject("dist")+"}", dist);
                String keyCity2 = jsonUtils.keyCityAdd("{\"keyCity\":" +json.getJSONObject("keyCity")+"}", keyCity);
                String pc2= jsonUtils.pcAdd("{\"pc\":" +json.getJSONObject("pc")+"}", pc);
                String prov2 = jsonUtils.provAdd("{\"prov\":" +json.getJSONObject("prov")+"}", prov);
                //put进去
                json.put("ds",JSONObject.parseObject(ds2).getJSONObject("ds"));
                json.put("city",JSONObject.parseObject(city2).getJSONObject("city"));
                json.put("dist",JSONObject.parseObject(dist2).getJSONObject("dist"));
                json.put("keyCity",JSONObject.parseObject(keyCity2).getJSONObject("keyCity"));
                json.put("pc",JSONObject.parseObject(pc2).getJSONObject("pc"));
                json.put("prov",JSONObject.parseObject(prov2).getJSONObject("prov"));
                json.put("countryAllCnt",json.getInteger("countryAllCnt")+Integer.parseInt(countryallcnt));
                json.put("countryAllPrice",json.getInteger("countryAllPrice")+Integer.parseInt(countryallprice));
                json.put("s",sString);
                json.put("secondSpeed",secondspeed);
                json.put("sp",sp);
                json.put("duplicateCnt",json.getInteger("duplicateCnt")+Integer.parseInt(flag));//
                System.out.println(json);
            }else if(!json.containsKey("ds")&&!ds.equals("")){
                json.put("ds",JSONObject.parseObject(ds).get("ds"));
                json.put("city",JSONObject.parseObject(city).get("city"));
                json.put("dist",JSONObject.parseObject(dist).get("dist"));
                json.put("keyCity",JSONObject.parseObject(keyCity).get("keyCity"));
                json.put("pc",JSONObject.parseObject(pc).get("pc"));
                json.put("prov",JSONObject.parseObject(prov).get("prov"));
                json.put("countryAllCnt",json.getInteger("countryAllCnt")+Integer.parseInt(countryallcnt));
                json.put("countryAllPrice",json.getInteger("countryAllPrice")+Integer.parseInt(countryallprice));
                json.put("s",sString);
                json.put("secondSpeed",secondspeed);
                json.put("sp",sp);
                json.put("duplicateCnt",json.getInteger("duplicateCnt")+Integer.parseInt(flag));//
                System.out.println(json);
            }else if(!json.containsKey("ds")&&ds.equals("")){
                json.put("countryAllCnt",json.getInteger("countryAllCnt")+Integer.parseInt(countryallcnt));
                json.put("countryAllPrice",json.getInteger("countryAllPrice")+Integer.parseInt(countryallprice));
                json.put("s",sString);
                json.put("secondSpeed",secondspeed);
                json.put("sp",sp);
                json.put("duplicateCnt",json.getInteger("duplicateCnt")+Integer.parseInt(flag));//
                System.out.println(json);
            }else if(json.containsKey("ds")&&ds.equals("")){
                json.put("countryAllCnt",json.getInteger("countryAllCnt")+Integer.parseInt(countryallcnt));
                json.put("countryAllPrice",json.getInteger("countryAllPrice")+Integer.parseInt(countryallprice));
                json.put("s",sString);
                json.put("secondSpeed",secondspeed);
                json.put("sp",sp);
                json.put("duplicateCnt",json.getInteger("duplicateCnt")+Integer.parseInt(flag));//
                System.out.println(json);
            }



        }



        }


}
