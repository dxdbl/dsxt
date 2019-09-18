package io.transwarp.udf;

import com.alibaba.fastjson.JSONObject;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.log4j.Logger;

import java.io.IOException;

public class preprocessing3 extends UDF {
    private static Logger log = Logger.getLogger(preprocessing3.class);
    // hbase 表结构 rowley 存储单号，q1存储 flag ，q2存储 消息json串
    private static String[] columns = {"q1","q2"};
    private static JSONObject jo;
    private static String send_city;
    private static String send_dist;
    private static String rec_city;
    private static String rec_dist;
    private static String mailNo;
    private static String ds_name;
    private static String time;
    private static String pc_name;
    private static String dt;
    private static String flag;
    private static String send_pro_code;
    private static String send_city_code;
    private static String send_dist_code;
    private static String rec_pro_code;
    private static String rec_city_code;
    private static String rec_dist_code;
    private static String ds_code;
    private static String pc_code;
    private static String ds;
    private static String city;
    private static String dist;
    private static String key_city;
    private static String pc;
    private static String prov;
    private static String result_json_str;
    private static String send_pro;
    private static String rec_pro;
    private static String[] values = {"1", "jsonstr"};

    public String evaluate(String jsonStr) {
        jo = JSONObject.parseObject(jsonStr);
        log.info("##################### json对象化成功 ############");

        // 获取json串中的电商平台,推送时间,单号
        mailNo = jo.getString("mailNo");
        log.info("##################### 获取 mailNo 成功 ############");
        ds_name = jo.getString("ecCompanyId");
        log.info("##################### 获取 ecCompanyId 成功 ############");
        time = jo.getString("sysDate");
        log.info("##################### 获取 sysDate 成功 ############");
        pc_name= jo.getString("logisticProviderID"); // pc 快递企业
        log.info("##################### 获取 logisticProviderID 成功 ############");

        // 生成时间key
        dt = jsonUtil.DateTime(time);
        log.info("##################### dt 成功 ############" + dt);

        //flag = null;
        try {
            flag = hbaseUtils.getOneRecordByCol("dsxt.order_online", mailNo, "f", "q1");
            log.info("##################### getOneRecordByCol 成功 ############" );
        } catch (IOException e) {
            e.printStackTrace();
        }
        if (!flag.equals("1")) {
            values[1] = jsonStr;
            try {
                hbaseUtils.addData("dsxt.order_online", mailNo, columns, values);
                log.info("##################### addData 成功 ############" );
            } catch (IOException e) {
                e.printStackTrace();
            }
            if (ds_name.equals("JBD")){
                log.info("##################### JBD判断 成功 ############" );
                send_pro_code = jo.getString("senProvCode");
                log.info("##################### send_pro_code 成功 ############" + send_pro_code);
                send_city_code = jo.getString("senCityCode");
                log.info("##################### send_city_code 成功 ############" + send_city_code);
                send_dist_code = jo.getString("senCountyCode");
                log.info("##################### send_dist_code 成功 ############" + send_dist_code);

                rec_pro_code = "999999";
                rec_city_code = "999999";
                rec_dist_code = "999999";

                // 京邦达 code
                ds_code = "30000";

                // 获取企业code
                pc_code= jsonUtil.getPcCode(pc_name);
                log.info("##################### getPcCode 成功 ############" + pc_code);

                // 生成最终中间结果字符串
                ds = jsonUtil.ds(ds_code,send_pro_code,send_city_code,send_dist_code,rec_pro_code,rec_city_code,rec_dist_code);
                city = jsonUtil.byCity(send_city_code,rec_city_code);
                dist = jsonUtil.Dist(send_dist_code,rec_dist_code);
                key_city = jsonUtil.keyCityDataMsg(pc_code,send_city_code,send_pro_code,rec_city_code,rec_pro_code);
                pc = jsonUtil.pcDataMsg(pc_code,send_pro_code,send_city_code,send_dist_code,rec_pro_code,rec_city_code,rec_dist_code);
                prov = jsonUtil.Prov(send_pro_code,rec_pro_code,rec_city_code,rec_dist_code);

                // 拼接最终结果json字符串
                result_json_str = dt + "#" + ds + "#" + "1" + "#" + "224" + "#" + city + "#" + dist + "#" + key_city + "#" + pc + "#" + prov + "#" + "true" + "#" + "1" + "#" + "0" + "#" + "sp";

                // 返回最终结果
                return result_json_str;
            }
            else if (ds_name.equals("TAOBAO")){

                send_pro = jo.getString("senProvCode");
                rec_pro = jo.getString("recProvCode");

                if (jo.getString("senCityCode").contains(",")){
                    send_city = jo.getString("senCityCode").split(",")[0];
                    send_dist = jo.getString("senCityCode").split(",")[1];
                }
                else{
                    send_city = jo.getString("senCityCode");
                    send_dist = "distisempty";
                }

                if (jo.getString("recCityCode").contains(",")){
                    rec_city = jo.getString("recCityCode").split(",")[0];
                    rec_dist = jo.getString("recCityCode").split(",")[1];
                }
                else{
                    rec_city = jo.getString("recCityCode");
                    rec_dist = "distisempty";
                }

                send_pro_code = jsonUtil.getProvinceCode(send_pro);
                send_city_code = jsonUtil.getCityCode(send_pro,send_city,send_dist);
                send_dist_code = jsonUtil.getDistCode(send_pro,send_city,send_dist);

                rec_pro_code = jsonUtil.getProvinceCode(rec_pro);
                rec_city_code = jsonUtil.getCityCode(rec_pro,rec_city,rec_dist);
                rec_dist_code = jsonUtil.getDistCode(rec_pro,rec_city,rec_dist);

                // 电商企业 code
                ds_code = jsonUtil.getDsCode(ds_name);

                // 获取企业code
                pc_code= jsonUtil.getPcCode(pc_name);

                // 生成最终中间结果字符串
                ds = jsonUtil.ds(ds_code,send_pro_code,send_city_code,send_dist_code,rec_pro_code,rec_city_code,rec_dist_code);
                city = jsonUtil.byCity(send_city_code,rec_city_code);
                dist = jsonUtil.Dist(send_dist_code,rec_dist_code);
                key_city = jsonUtil.keyCityDataMsg(pc_code,send_city_code,send_pro_code,rec_city_code,rec_pro_code);
                pc = jsonUtil.pcDataMsg(pc_code,send_pro_code,send_city_code,send_dist_code,rec_pro_code,rec_city_code,rec_dist_code);
                prov = jsonUtil.Prov(send_pro_code,rec_pro_code,rec_city_code,rec_dist_code);

                // 拼接最终结果json字符串
                result_json_str = dt + "#" + ds + "#" + "1" + "#" + "224" + "#" + city + "#" + dist + "#" + key_city + "#" + pc + "#" + prov + "#" + "true" + "#" + "1" + "#" + "0" + "#" + "sp";

                // 返回最终结果
                return result_json_str;
            }
        }
        return dt + "##0#0######true#1#1#sp";
    }
}
