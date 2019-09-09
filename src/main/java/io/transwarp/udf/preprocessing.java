package io.transwarp.udf;

import com.alibaba.fastjson.JSONObject;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.log4j.Logger;

import java.io.IOException;


public class preprocessing extends UDF {

    public static Logger log = Logger.getLogger(dsxt.class);

    // hbase 表结构 rowley 存储单号，q1存储 flag ，q2存储 消息json串
    public static String[] columns = {"q1","q2"};

    public String evaluate(String jsonStr) {
        JSONObject jo = JSONObject.parseObject(jsonStr);

        // 获取json串中的电商平台,推送时间,单号
        String mailNo = jo.getString("mailNo");
        String ds_name = jo.getString("ecCompanyId");
        String time = jo.getString("sysDate");
        String pc_name= jo.getString("logisticProviderID");

        // 生成时间key
        String dt = jsonUtils.DateTime(time);

        String flag = null;
        try {
            flag = hbaseUtils.getOneRecordByCol("order", mailNo, "f", "q1");
        } catch (IOException e) {
            e.printStackTrace();
        }
        if (!flag.equals("1")) {
            String[] values = {"1", jsonStr};
            try {
                hbaseUtils.addData("order", mailNo, columns, values);
            } catch (IOException e) {
                e.printStackTrace();
            }
            if (ds_name.equals("JBD")){
                String send_pro_code = jo.getString("senProvCode");
                String send_city_code = jo.getString("senCityCode");
                String send_dist_code = jo.getString("senCountyCode");

                String rec_pro_code = "999999";
                String rec_city_code = "999999";
                String rec_dist_code = "999999";

                // 京邦达 code
                String ds_code = "30000";

                // 获取企业code
                String pc_code= jsonUtils.getPcCode(pc_name);

                // 生成最终中间结果字符串
                String ds = jsonUtils.ds(ds_code,send_pro_code,send_city_code,send_dist_code,rec_pro_code,rec_city_code,rec_dist_code);
                String city = jsonUtils.byCity(send_city_code,rec_city_code);
                String dist = jsonUtils.Dist(send_dist_code,rec_dist_code);
                String key_city = jsonUtils.keyCityDataMsg(pc_code,send_city_code,send_pro_code,rec_city_code,rec_pro_code);
                String pc = jsonUtils.pcDataMsg(pc_code,send_pro_code,send_city_code,send_dist_code,rec_pro_code,rec_city_code,rec_dist_code);
                String prov = jsonUtils.Prov(send_pro_code,rec_pro_code,rec_city_code,rec_dist_code);

                // 拼接最终结果json字符串
                String result_json_str = dt + "#" + ds + "#" + "1" + "#" + "224" + "#" + city + "#" + dist + "#" + key_city + "#" + pc + "#" + prov + "#" + "true" + "#" + "1" + "#" + "0" + "#" + "sp";

                // 返回最终结果
                return result_json_str;
            }
            else if (ds_name.equals("TAOBAO")){
                String send_pro = jo.getString("senProvCode");
                String send_city = jo.getString("senCityCode").split(",")[0];
                String send_dist = jo.getString("senCityCode").split(",")[1];
                String rec_pro = jo.getString("recProvCode");
                String rec_city = jo.getString("recCityCode").split(",")[0];
                String rec_dist = jo.getString("recCityCode").split(",")[1];

                String send_pro_code = jsonUtils.getProvinceCode(send_pro);
                String send_city_code = jsonUtils.getCityCode(send_pro,send_city,send_dist);
                String send_dist_code = jsonUtils.getDistCode(send_pro,send_city,send_dist);

                String rec_pro_code = jsonUtils.getProvinceCode(rec_pro);
                String rec_city_code = jsonUtils.getCityCode(rec_pro,rec_city,rec_dist);
                String rec_dist_code = jsonUtils.getDistCode(rec_pro,rec_city,rec_dist);

                // 京邦达 code
                String ds_code = jsonUtils.getDsCode(ds_name);

                // 获取企业code
                String pc_code= jsonUtils.getPcCode(pc_name);

                // 生成最终中间结果字符串
                String ds = jsonUtils.ds(ds_code,send_pro_code,send_city_code,send_dist_code,rec_pro_code,rec_city_code,rec_dist_code);
                String city = jsonUtils.byCity(send_city_code,rec_city_code);
                String dist = jsonUtils.Dist(send_dist_code,rec_dist_code);
                String key_city = jsonUtils.keyCityDataMsg(pc_code,send_city_code,send_pro_code,rec_city_code,rec_pro_code);
                String pc = jsonUtils.pcDataMsg(pc_code,send_pro_code,send_city_code,send_dist_code,rec_pro_code,rec_city_code,rec_dist_code);
                String prov = jsonUtils.Prov(send_pro_code,rec_pro_code,rec_city_code,rec_dist_code);

                // 拼接最终结果json字符串
                String result_json_str = dt + "#" + ds + "#" + "1" + "#" + "224" + "#" + city + "#" + dist + "#" + key_city + "#" + pc + "#" + prov + "#" + "true" + "#" + "1" + "#" + "0" + "#" + "sp";

                // 返回最终结果
                return result_json_str;
            }
        }
        return dt + "##0#0######true#1#1#sp";
    }
}
