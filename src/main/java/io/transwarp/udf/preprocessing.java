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
        String ds = jo.getString("ecCompanyId");
        String time = jo.getString("sysDate");

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
        }
        return dt + "##0#0######true#1#1#sp";
    }

    public static void main(String[] args) {
        //System.out.println(evaluate(""));

        JSONObject jo = JSONObject.parseObject("{\"ecCompanyId\":\"JBD\",\"txLogisticID\":\"JBD\",\"logisticProviderID\":\"JBD\",\"mailNo\":\"JD0002136068926\",\"mailType\":\"1\",\"recAreaCode\":\"1\",\"senProvCode\":\"220000\",\"senCityCode\":\"220100\",\"senCountyCode\":\"220182\",\"recCityCode\":\"00\",\"recDatetime\":\"2019-08-31 14:22:11\",\"sysDate\":\"2019-08-31 14:22:33\"}");

        if (jo.getString("ecCompanyId") == null)
        {
            System.out.println("null");
        }
        else{
            System.out.println(jo.getString("ecCompanyId"));
        }
    }
}
