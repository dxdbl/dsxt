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
        String dt = jsonUtil.DateTime(pushTime);

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
            // 获取地区代码和快递企业代码
            String send_prov = jsonUtil.formatProvince(sender_prov);
            String rec_prov = jsonUtil.formatProvince(receiver_prov);
            String send_city = sender_city.split("-")[0];
            String send_dist = sender_city.split("-")[1];
            String rec_city = receiver_city.split("-")[0];
            String rec_dist = receiver_city.split("-")[1];
            String send_pro_code = jsonUtil.getProvinceCode(send_prov);
            String rec_pro_code = jsonUtil.getProvinceCode(rec_prov);
            //
            String send_city_code = jsonUtil.getCityCode(send_prov,send_city,send_dist);
            String rec_city_code = jsonUtil.getCityCode(rec_prov,rec_city,rec_dist);

            String send_dist_code = jsonUtil.getDistCode(send_prov,send_city,send_dist);
            String rec_dist_code = jsonUtil.getDistCode(rec_prov,rec_city,rec_dist);
            String ds_code = jsonUtil.getDsCode(ecCompanyId);

            // 获取企业code
            String pc_code= jsonUtil.getPcCode(logisticProviderID);

            // 生成最终中间结果字符串
            String ds = jsonUtil.ds(ds_code,send_pro_code,send_city_code,send_dist_code,rec_pro_code,rec_city_code,rec_dist_code);
            String city = jsonUtil.byCity(send_city_code,rec_city_code);
            String dist = jsonUtil.Dist(send_dist_code,rec_dist_code);
            String key_city = jsonUtil.keyCityDataMsg(pc_code,send_city_code,send_pro_code,rec_city_code,rec_pro_code);
            String pc = jsonUtil.pcDataMsg(pc_code,send_pro_code,send_city_code,send_dist_code,rec_pro_code,rec_city_code,rec_dist_code);
            String prov = jsonUtil.Prov(send_pro_code,rec_pro_code,rec_city_code,rec_dist_code);

            // 拼接最终结果json字符串
            String result_json_str = dt + "#" + ds + "#" + "1" + "#" + "224" + "#" + city + "#" + dist + "#" + key_city + "#" + pc + "#" + prov + "#" + "true" + "#" + "1" + "#" + "0" + "#" + "sp";
            // 返回最终结果
            return result_json_str;
        }
        return dt + "##0#0######true#1#1#sp";
    }
}
