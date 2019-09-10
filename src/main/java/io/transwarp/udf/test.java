package io.transwarp.udf;

import com.alibaba.fastjson.JSONObject;

/**
 * @description:
 * @author: mhf
 * @time: 2019/9/10 17:09
 */
public class test {
    public static void main(String[] args) {

    String jsonStr = "{\"ecCompanyId\":\"JBD\",\"txLogisticID\":\"JBD\",\"logisticProviderID\":\"JBD\",\"mailNo\":\"JD0002136068926\",\"mailType\":\"1\",\"recAreaCode\":\"1\",\"senProvCode\":\"220000\",\"senCityCode\":\"220100\",\"senCountyCode\":\"220182\",\"recCityCode\":\"00\",\"recDatetime\":\"2019-08-31 14:22:11\",\"sysDate\":\"2019-08-31 14:22:33\"}";
    JSONObject jo = JSONObject.parseObject(jsonStr);
        System.out.println("##################### json对象化成功 ############" + jo);

        // 获取json串中的电商平台,推送时间,单号
    String mailNo = jo.getString("mailNo");
        System.out.println("##################### 获取 mailNo 成功 ############" + mailNo);
    String ds_name = jo.getString("ecCompanyId");
        System.out.println("##################### 获取 ecCompanyId 成功 ############" + ds_name);
    String time = jo.getString("sysDate");
        System.out.println("##################### 获取 sysDate 成功 ############" + time);
    String pc_name= jo.getString("logisticProviderID");
        System.out.println("##################### 获取 logisticProviderID 成功 ############" + pc_name);
}
}
