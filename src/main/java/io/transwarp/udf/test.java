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

        String a = "{\n" +
                "    \"ecCompanyId\": \"JBD\",\n" +
                "    \"logisticProviderID\": \"JBD\",\n" +
                "    \"mailNo\": \"JDVG00087119081\",\n" +
                "    \"mailType\": \"1\",\n" +
                "    \"pushTime\": \"2019-09-20 17:29:23\",\n" +
                "    \"recCityCode\": \"00\",\n" +
                "    \"recDatetime\": \"2019-09-20 17:29:23\",\n" +
                "    \"senCityCode\": \"00\",\n" +
                "    \"senProvCode\": \"65000000.0\",\n" +
                "    \"sysDate\": \"2019-09-20 17:27:39\",\n" +
                "    \"txLogisticID\": \"JBD\"\n" +
                "}";
        JSONObject jo = JSONObject.parseObject(a);
        BigDecimal b = jo.getBigDecimal("senProvCode");

        System.out.println( b.stripTrailingZeros().toPlainString());
    }

}
