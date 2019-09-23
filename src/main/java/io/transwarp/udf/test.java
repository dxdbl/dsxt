package io.transwarp.udf;

import com.alibaba.fastjson.JSONObject;
import org.apache.hadoop.hive.ql.plan.JoinSchemaDesc;

import java.math.BigDecimal;
import java.util.Calendar;
import java.util.Date;

/**
 * @description:
 * @author: mhf
 * @time: 2019/9/10 17:09
 */
public class test {
    public static void main(String[] args) {

        String a = "{\"ecCompanyId\":\"JBD\",\"txLogisticID\":\"JBD\",\"logisticProviderID\":\"JBD\",\"mailNo\":\"JDVE00291582550\",\"mailType\":\"1\",\"recAreaCode\":\"1\",\"senProvCode\":\"420000\",\"senCityCode\":\"420100\",\"senCountyCode\":\"420106\",\"recCityCode\":\"00\",\"recDatetime\":\"2019-08-31 14:22:15\",\"sysDate\":\"2019-08-31 14:22:33\"}";
        JSONObject jo = JSONObject.parseObject(a);
        String ds_name = jo.getString("ecCompanyId");
        System.out.println(ds_name);

    }

}
