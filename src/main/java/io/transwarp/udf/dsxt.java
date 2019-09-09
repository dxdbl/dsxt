package io.transwarp.udf;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.log4j.Logger;

import java.io.IOException;


public class dsxt extends UDF {

    public static Logger log = Logger.getLogger(dsxt.class);
    public static String[] columns = {"q1","q2","q3","q4","q5","q6","q7","q8","q9","q10","q11","q12"};
    // 面单号 mailNo,创建时间 orderCreateTime，流出省份 sender_prov，接收省份 receiver_prov
    public String evaluate(String ecCompanyId, String mailNo, String logisticProviderID, String orderType, String serviceType, String txLogisticID, String orderCreateTime, String sender_city, String sender_prov, String receiver_city, String receiver_prov, String pushTime) {

        // 生成时间key
        String dt = jsonUtils.DateTime(pushTime);
        log.info("##########" + dt);

        String flag = null;
        try {
            flag = hbaseUtils.getOneRecordByCol("order",mailNo,"f","q12");
        } catch (IOException e) {
            e.printStackTrace();
        }
        if (!flag.equals("1")){
            String[] values = {ecCompanyId,logisticProviderID,orderType,serviceType,txLogisticID,orderCreateTime,sender_city,sender_prov,receiver_city,receiver_prov,pushTime,"1"};
            try {
                hbaseUtils.addData("order",mailNo,columns,values);
            } catch (IOException e) {
                e.printStackTrace();
            }
            //String insert_sql = "set ngmr.exec.mode=local;insert into dsxt.orders values('" + mailNo +"','" + orderCreateTime + "','" + sender_prov + "','" + receiver_prov +"');";
            //utils.insert(insert_sql);
            // 获取地区代码和快递企业代码
            String send_prov = jsonUtils.formatProvince(sender_prov);
            log.info("############发送省份##########" + send_prov);
            String rec_prov = jsonUtils.formatProvince(receiver_prov);
            log.info("############接收省份##########" + send_prov);
            String send_city = sender_city.split("-")[0];
            log.info("##########1 " + send_city);
            String send_dist = sender_city.split("-")[1];
            log.info("##########2 " + send_dist);
            String rec_city = receiver_city.split("-")[0];
            log.info("##########3 " + rec_city);
            String rec_dist = receiver_city.split("-")[1];
            log.info("##########4 " + rec_dist);
            String send_pro_code = jsonUtils.getProvinceCode(send_prov);
            log.info("##########5 " + send_pro_code);
            String rec_pro_code = jsonUtils.getProvinceCode(rec_prov);
            log.info("##########6 " + rec_pro_code);
            //
            String send_city_code = jsonUtils.getCityCode(send_prov,send_city,send_dist);
            log.info("#########7########" + send_prov + send_city + send_dist);
            log.info("##########" + send_city_code);
            String rec_city_code = jsonUtils.getCityCode(rec_prov,rec_city,rec_dist);
            log.info("##########" + rec_city_code);


            String send_dist_code = jsonUtils.getDistCode(send_prov,send_city,send_dist);
            log.info("##########" + send_dist_code);
            String rec_dist_code = jsonUtils.getDistCode(rec_prov,rec_city,rec_dist);
            log.info("##########" + rec_dist_code);
            //
            String ds_code = jsonUtils.getDsCode(ecCompanyId);

            // 获取企业code
            String pc_code= jsonUtils.getPcCode(logisticProviderID);
            log.info("##############" + pc_code);

            // 生成最终中间结果字符串
            String ds = jsonUtils.ds(ds_code,send_pro_code,send_city_code,send_dist_code,rec_pro_code,rec_city_code,rec_dist_code);
            log.info("##############" + ds);
            String city = jsonUtils.byCity(send_city_code,rec_city_code);
            log.info("##############" + city);
            String dist = jsonUtils.Dist(send_dist_code,rec_dist_code);
            log.info("##############" + dist);
            String key_city = jsonUtils.keyCityDataMsg(pc_code,send_city_code,send_pro_code,rec_city_code,rec_pro_code);
            log.info("##############" + key_city);
            String pc = jsonUtils.pcDataMsg(pc_code,send_pro_code,send_city_code,send_dist_code,rec_pro_code,rec_city_code,rec_dist_code);
            log.info("##############" + pc);
            String prov = jsonUtils.Prov(send_pro_code,rec_pro_code,rec_city_code,rec_dist_code);
            log.info("##############" + prov);

            // 拼接最终结果json字符串
            String result_json_str = dt + "#" + ds + "#" + "1" + "#" + "224" + "#" + city + "#" + dist + "#" + key_city + "#" + pc + "#" + prov + "#" + "true" + "#" + "1" + "#" + "0" + "#" + "sp";
            log.info("###################" + result_json_str);
            // 返回最终结果
            return result_json_str;
        }
        return dt + "##0#0######true#1#1#sp";
    }

    public static void main(String[] args) {

    }
}
