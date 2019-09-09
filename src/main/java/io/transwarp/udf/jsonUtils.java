package io.transwarp.udf;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

import java.io.IOException;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @description:根据json文件获取相关值
 * @author: mhf
 * @time: 2019/8/20 8:54
 */
public class jsonUtils {

    public static String dist_str;
    public static String pc_str;
    public static String ds_str;

    static {
        try {
            dist_str = hbaseUtils.getOneRecordByCol("distcode","1","f","q1");
            pc_str = hbaseUtils.getOneRecordByCol("pccode","1","f","q1");
            ds_str = hbaseUtils.getOneRecordByCol("dscode","1","f","q1");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static JSONObject distJson = JSONObject.parseObject(dist_str);
    public static JSONObject pcJson = JSONObject.parseObject(pc_str);
    public static JSONObject dsJson = JSONObject.parseObject(ds_str);

    public static String getProvinceCode(String province) {
        /**
         * @description: 获取省份code
         * @param province 示例 山东省
         * @return: java.lang.String
         * @author: mhf
         * @time: 2019/8/20 10:15
         */
        if (distJson.containsKey(province)) {
            JSONObject o2 = JSONObject.parseObject(distJson.getString(province));
            return o2.getString("v");
        }
        return "999999";
    }

    public static String getCityCode(String province, String city, String dist) {
        /**
         * @description: 获取城市code
         * @param city 示例 上海市
         * @return: java.lang.String
         * @author: mhf
         * @time: 2019/8/20 9:43
         */
        if (city.equals(province)) {
            //JSONObject o2 = JSONObject.parseObject(distJson.getString(province));
            //return o2.getString("v");
            city = dist;
        }
        if (distJson.containsKey(province)) {
            JSONObject o2 = JSONObject.parseObject(distJson.getString(province));
            if (o2.containsKey(city)) {
                JSONObject o3 = JSONObject.parseObject(o2.getString(city));
                return o3.getString("v");
            }
        }
        return "999999";
    }

    public static String getDistCode(String province, String city, String dist) {
        /**
         * @description: 获取地区的code
         * @param province 示例 山东省
         * @param city  示例 临沂市
         * @param dist  示例 罗庄区
         * @return: java.lang.String
         * @author: mhf
         * @time: 2019/8/20 10:12
         */
        if (province.equals(city)) {
            JSONObject o2 = JSONObject.parseObject(distJson.getString(province));
            if (o2.containsKey(dist)) {
                JSONObject o3 = JSONObject.parseObject(o2.getString(dist));
                return o3.getString("v");
            }
        }
        if (distJson.containsKey(province)) {
            JSONObject o2 = JSONObject.parseObject(distJson.getString(province));
            if (o2.containsKey(city)) {
                JSONObject o3 = JSONObject.parseObject(o2.getString(city));
                if (o3.containsKey(dist)) {
                    JSONObject o4 = JSONObject.parseObject(o3.getString(dist));
                    return o4.getString("v");
                }
            }
        }
        return "999999";
    }

    // 获取快递企业code
    public static String getPcCode(String pc) {
        //fastjson解析方法
        JSONArray jsonArray = JSONObject.parseArray(pcJson.getString("pcCleanDim"));
        for (int i = 0; i < jsonArray.size(); i++) {
            // 遍历 jsonarray 数组，把每一个对象转成 json 对象
            JSONObject job = jsonArray.getJSONObject(i);
            //System.out.println(job.get("reg"));
            //正则表达式匹配
            String regex = job.getString("reg");
            if (pc.matches(regex)) {
                return job.getString("m");
            }
        }
        return "9999";
    }
    // 获取电商企业code
    public static String getDsCode(String ds) {
        //fastjson解析方法
        JSONArray jsonArray = JSONObject.parseArray(dsJson.getString("dsCleanDim"));
        for (int i = 0; i < jsonArray.size(); i++) {
            // 遍历 jsonarray 数组，把每一个对象转成 json 对象
            JSONObject job = jsonArray.getJSONObject(i);
            //System.out.println(job.get("reg"));
            //正则表达式匹配
            String regex = job.getString("reg");
            if (ds.matches(regex)) {
                return job.getString("m");
            }
        }
        return "99999";
    }

    public static String clearUpStr(String str) {
        String des = "";
        if (str != null) {
            Pattern p = Pattern.compile("\\s*|\t|\r|\n");
            Matcher m = p.matcher(str);
            des = m.replaceAll("");
        }
        return des;
    }

    public static String byCity(String send, String rec) {
        String result = "";
        if (send.equals(rec)){
            result = "{\"city\":{\"" + send + "\":{\"rec\":1,\"sed\":1,\"sedPrice\":224}}}";
        }
        else {
            result = "{\"city\":{\"" + send + "\":{\"rec\":0,\"sed\":1,\"sedPrice\":224},\"" + rec + "\":{\"rec\":1,\"sed\":0,\"sedPrice\":0}}}";
        }
        //System.out.println(result);
        return result;
    }

    public static String Prov(String s_p,String r_p,String r_c,String r_d) {
        String result = "";
        if (!s_p.equals(r_p)) {
            result = "{\"prov\":{\"" + r_p + "\":{\"rec\":1,\"sed\":0,\"sedPrice\":0,\"to\":{}},\"" + s_p + "\":{\"rec\":0,\"sed\":1,\"sedPrice\":224,\"to\":{\"" + r_p + "\":1,\"city\":{\"" + r_c + "\":1},\"dist\":{\"" + r_d + "\":1}}}}}";
        } else {
            result = "{\"prov\":{\"" + r_p + "\":{\"rec\":1,\"sed\":1,\"sedPrice\":224,\"to\":{\"" + r_p + "\":1,\"city\":{\"" + r_c + "\":1},\"dist\":{\"" + r_d + "\":1}}}}}";
        }
        //System.out.println(result);
        return result;
    }

    public static String ds(String ds_code,String s_p,String s_c,String s_d,String r_p,String r_c,String r_d) {
        String ss ="";
        if(s_p == r_p && s_c != r_c && s_d != r_d) {
            ss = "{\"ds\":{\""+ds_code+"\":{\"all\":1,\"allPrice\":224,\"prov\":{\""+r_p+"\":{\"city\":{\""+r_c+"\":{\"dist\":{\""+r_d+"\":{\"rec\":1,\"sed\":0,\"sedPrice\":0}},\"rec\":1,\"sed\":0,\"sedPrice\":0},\""+s_c+"\":{\"dist\":{\""+s_d+"\":{\"rec\":0,\"sed\":1,\"sedPrice\":224}},\"rec\":0,\"sed\":1,\"sedPrice\":224}},\"rec\":1,\"sed\":1,\"sedPrice\":224}}}}}";
        }else if(s_p == r_p && s_c == r_c && s_d != r_d){
            ss = "{\"ds\":{\""+ds_code+"\":{\"all\":1,\"allPrice\":224,\"prov\":{\""+r_p+"\":{\"city\":{\""+r_c+"\":{\"dist\":{\""+r_d+"\":{\"rec\":1,\"sed\":0,\"sedPrice\":0},\""+s_d+"\":{\"rec\":0,\"sed\":1,\"sedPrice\":224}},\"rec\":1,\"sed\":1,\"sedPrice\":224}},\"rec\":1,\"sed\":1,\"sedPrice\":224}}}}}";

        }else if(s_p == r_p && s_c == r_c && s_d == r_d) {
            ss = "{\"ds\":{\""+ds_code+"\":{\"all\":1,\"allPrice\":224,\"prov\":{\""+r_p+"\":{\"city\":{\""+r_c+"\":{\"dist\":{\""+r_d+"\":{\"rec\":1,\"sed\":1,\"sedPrice\":224}},\"rec\":1,\"sed\":1,\"sedPrice\":224}},\"rec\":1,\"sed\":1,\"sedPrice\":224}}}}}";
        }else {
            ss = "{\"ds\":{\""+ds_code+"\":{\"all\":1,\"allPrice\":224,\"prov\":{\""+s_p+"\":{\"rec\":0,\"sed\":1,\"sedPrice\":224,\"city\":{\""+s_c+"\":{\"rec\":0,\"sed\":1,\"sedPrice\":224,\"dist\":{\""+s_d+"\":{\"rec\":0,\"sed\":1,\"sedPrice\":224}}}}},\""+r_p+"\":{\"rec\":1,\"sed\":0,\"sedPrice\":0,\"city\":{\""+r_c+"\":{\"rec\":1,\"sed\":0,\"sedPrice\":0,\"dist\":{\""+r_d+"\":{\"rec\":1,\"sed\":0,\"sedPrice\":0}}}}}}}}}";
        }
        return ss;
    }

    public static String keyCityDataMsg(String pcCode, String provSedCode, String citySedCode, String provRecCode, String cityRecCode) {
       String keyCityDataMsgTemp = "";
        if (citySedCode.equals(cityRecCode)) {
            keyCityDataMsgTemp = "{\"keyCity\": {\"" + provSedCode + "\": {\"fromCity\": {\"" + citySedCode + "\": 1},\"fromProv\": {\"" + provSedCode + "\": 1},\"toCity\": {\"" + cityRecCode + "\": 1},\"toProv\": {\"" + provRecCode + "\": 1} },\"" + provRecCode + "\": {\"fromCity\": {\"" + citySedCode + "\": 1},\"fromProv\": {\"" + provSedCode + "\": 1 },\"toCity\": {},\"toProv\": {}}}}";

        } else {
            keyCityDataMsgTemp = "{\"keyCity\": {\"" + provSedCode + "\": {\"fromCity\": {},\"fromProv\": {},\"toCity\": {\"" + cityRecCode + "\": 1},\"toProv\": {\"" + provRecCode + "\": 1} },\"" + provRecCode + "\": {\"fromCity\": {\"" + citySedCode + "\": 1},\"fromProv\": {\"" + provSedCode + "\": 1 },\"toCity\": {},\"toProv\": {}}}}";
        }
       return keyCityDataMsgTemp;
    }

//    public static String pcDataMsg(String pcCode, String provSedCode, String provRecCode) {
//        String pcDataMsgTemp = "";
//        if (provSedCode.equals(provRecCode)) {
//            pcDataMsgTemp = "{\"pc\":{\"" + pcCode + "\":{\"a kll\":1,\"allPrice\":224,\"prov\":{\"" + provSedCode + "\":{\"rec\":1,\"sed\":1,\"sedPrice\":224}}}}}";
//
//        } else {
//            pcDataMsgTemp = "{\"pc\":{\"" + pcCode + "\":{\"all\":1,\"allPrice\":224,\"prov\":{\"" + provSedCode + "\":{\"rec\":0,\"sed\":1,\"sedPrice\":224},\"" + provRecCode + "\":{\"rec\":1,\"sed\":0,\"sedPrice\":0}}}}}";
//        }
//
//        return pcDataMsgTemp;
//    }

    public static String pcDataMsg(String pc_code,String s_p,String s_c,String s_d,String r_p,String r_c,String r_d) {
        String ss ="";

        if(s_p .equals(r_p)  && !s_c.equals(r_c) ) {//省相同市不同
            ss = "{\"pc\":{"+pc_code+":{\"all\":1,\"allPrice\":224,\"prov\":{"+s_p+":{\"city\":{"+r_c+":{\"dist\":{"+r_d+":{\"rec\":1,\"sed\":0,\"sedPrice\":0}},\"rec\":1,\"sed\":0,\"sedPrice\":0},"+s_c+":{\"dist\":{"+s_d+":{\"rec\":0,\"sed\":1,\"sedPrice\":224}},\"rec\":0,\"sed\":1,\"sedPrice\":224}},\"rec\":1,\"sed\":1,\"sedPrice\":224}}}}}";
        }else if(s_p == r_p && s_c == r_c && s_d != r_d){//省相同市相同区不同
            ss = "{\"pc\":{"+pc_code+":{\"all\":1,\"allPrice\":224,\"prov\":{"+s_p+":{\"city\":{"+r_c+":{\"dist\":{"+r_d+":{\"rec\":1,\"sed\":0,\"sedPrice\":0},"+s_d+":{\"rec\":0,\"sed\":1,\"sedPrice\":224}},\"rec\":1,\"sed\":1,\"sedPrice\":224}},\"rec\":1,\"sed\":1,\"sedPrice\":224}}}}}";

        }else if(s_p == r_p && s_c == r_c && s_d == r_d) {//省相同市相同区相同
            ss = "{\"pc\":{"+pc_code+":{\"all\":1,\"allPrice\":224,\"prov\":{"+s_p+":{\"city\":{"+r_c+":{\"dist\":{"+r_d+":{\"rec\":1,\"sed\":1,\"sedPrice\":224}},\"rec\":1,\"sed\":1,\"sedPrice\":224}},\"rec\":1,\"sed\":1,\"sedPrice\":224}}}}}";
        }else {//省不同
            ss = "{\"pc\":{"+pc_code+":{\"all\":1,\"allPrice\":224,\"prov\":{"+r_p+":{\"city\":{"+r_c+":{\"dist\":{"+r_d+":{\"rec\":1,\"sed\":0,\"sedPrice\":0}},\"rec\":1,\"sed\":0,\"sedPrice\":0}},\"rec\":1,\"sed\":0,\"sedPrice\":0},"+s_p+":{\"city\":{"+s_c+":{\"dist\":{"+s_d+":{\"rec\":0,\"sed\":1,\"sedPrice\":224}},\"rec\":0,\"sed\":1,\"sedPrice\":224}},\"rec\":0,\"sed\":1,\"sedPrice\":224}}}}}";
        }

        return ss;
    }

    public static String DateTime(String date) {
        String dateTime = "";
        Date now = new Date();
        int hour = now.getHours();
        String a = date.split(" ")[0];
        dateTime = a.replace("-", "") + hour;
        return dateTime;
    }

    public static String Dist(String send, String rec) {
        String result = "";
        if (!send.equals(rec)) {
            result = "{\"dist\":{\"" + send + "\":{\"rec\":0,\"sed\":1,\"sedPrice\":224},\"" + rec + "\":{\"rec\":1,\"sed\":0,\"sedPrice\":0}}}";

        } else {
            result = "{\"dist\":{\"" + send + "\":{\"rec\":1,\"sed\":1,\"sedPrice\":224}}}";
        }
        //System.out.println(result);
        return result;
    }

    public static String formatProvince(String pro) {
        if (pro.equals("北京") || pro.equals("上海") || pro.equals("重庆") || pro.equals("天津")) {
            return pro + "市";
        } else if (pro.equals("北京市") || pro.equals("上海市") || pro.equals("重庆市") || pro.equals("天津市")) {
            return pro;
        } else if (pro.indexOf("区") != -1) {
            return pro;
        } else if (pro.indexOf("海外") != -1) {
            return pro;
        } else if (pro.indexOf("省") != -1) {
            return pro;
        } else {
            return pro + "省";
        }
    }

    public static void main(String[] args) {
        System.out.println("hello");
        System.out.println(getDsCode("TAOBAO"));
    }
}
