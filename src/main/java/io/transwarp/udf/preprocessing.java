package io.transwarp.udf;

import com.alibaba.fastjson.JSONObject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.log4j.Logger;

import java.io.IOException;

public class preprocessing extends UDF {
    //private  Logger log = Logger.getLogger(preprocessing3.class);
    // hbase 表结构 rowley 存储单号，q1存储 flag ，q2存储 消息json串
    public static   String[] columns = {"q1","q2"};
    public static   String[] values = {"1", "jsonstr"};
    private  Table table;
    private  Get get;

    public static Configuration conf = null;

    static {
        Configuration HBASE_CONFIG = new Configuration();
        HBASE_CONFIG.set("hbase.zookeeper.quorum", "tdh4,tdh8,tdh13");
        HBASE_CONFIG.set("hbase.master.kerberos.principal", "hbase/_HOST@TDH");
        HBASE_CONFIG.set("hbase.regionserver.kerberos.principal", "hbase/_HOST@TDH");
        HBASE_CONFIG.set("hbase.security.authentication", "kerberos");
        HBASE_CONFIG.set("zookeeper.znode.parent", "/hyperbase1");
        HBASE_CONFIG.set("hadoop.security.authentication", "kerberos");
        conf = HBaseConfiguration.create(HBASE_CONFIG);
        UserGroupInformation.setConfiguration(conf);
        try {
            UserGroupInformation.loginUserFromKeytab("hbase/tdh4@TDH", "/var/hyperbase.keytab");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public String evaluate(String jsonStr) {
        JSONObject jo = JSONObject.parseObject(jsonStr);
        //log.info("##################### json对象化成功 ############");

        // 获取json串中的电商平台,推送时间,单号
        String mailNo = jo.getString("mailNo");
        //log.info("##################### 获取 mailNo 成功 ############");
        String ds_name = jo.getString("ecCompanyId");
        //log.info("##################### 获取 ecCompanyId 成功 ############");
        String time = jo.getString("sysDate");
        //log.info("##################### 获取 sysDate 成功 ############");
        String pc_name= jo.getString("logisticProviderID"); // pc 快递企业
        //log.info("##################### 获取 logisticProviderID 成功 ############");

        // 生成时间key
        String dt = jsonUtil.DateTime(time);
        //log.info("##################### dt 成功 ############" + dt);

        String flag = null;
        try {
            flag = getOneRecordByCol("dsxt.order_online", mailNo, "f", "q1");
            //log.info("##################### getOneRecordByCol 成功 ############" );
        } catch (IOException e) {
            e.printStackTrace();
        }
        if (!flag.equals("1")) {
            values[1] = jsonStr;
            try {
                addData("dsxt.order_online", mailNo, columns, values);
                //log.info("##################### addData 成功 ############" );
            } catch (IOException e) {
                e.printStackTrace();
            }
            if (ds_name.equals("JBD")){
                //log.info("##################### JBD判断 成功 ############" );
                String send_pro_code = jo.getString("senProvCode");
                //log.info("##################### send_pro_code 成功 ############" + send_pro_code);
                String send_city_code = jo.getString("senCityCode");
                //log.info("##################### send_city_code 成功 ############" + send_city_code);
                String send_dist_code = jo.getString("senCountyCode");
                //log.info("##################### send_dist_code 成功 ############" + send_dist_code);

                String rec_pro_code = "999999";
                String rec_city_code = "999999";
                String rec_dist_code = "999999";

                // 京邦达 code
                String ds_code = "30000";

                // 获取企业code
                String pc_code= jsonUtil.getPcCode(pc_name);
                //log.info("##################### getPcCode 成功 ############" + pc_code);

                // 生成最终中间结果字符串
                String ds = jsonUtil.ds(ds_code,send_pro_code,send_city_code,send_dist_code,rec_pro_code,rec_city_code,rec_dist_code);
                String city = jsonUtil.byCity(send_city_code,rec_city_code);
                String dist = jsonUtil.Dist(send_dist_code,rec_dist_code);
                String key_city = jsonUtil.keyCityDataMsg(pc_code,send_city_code,send_pro_code,rec_city_code,rec_pro_code);
                String  pc = jsonUtil.pcDataMsg(pc_code,send_pro_code,send_city_code,send_dist_code,rec_pro_code,rec_city_code,rec_dist_code);
                String  prov = jsonUtil.Prov(send_pro_code,rec_pro_code,rec_city_code,rec_dist_code);

                // 拼接最终结果json字符串
                String result_json_str = dt + "#" + ds + "#" + "1" + "#" + "224" + "#" + city + "#" + dist + "#" + key_city + "#" + pc + "#" + prov + "#" + "true" + "#" + "1" + "#" + "0" + "#" + "sp";

                // 返回最终结果
                return result_json_str;
            }
            else if (ds_name.equals("TAOBAO")){

                String send_pro = jo.getString("senProvCode");
                String rec_pro = jo.getString("recProvCode");
                String send_city = null;
                String send_dist = null;
                String rec_city = null;
                String rec_dist = null;

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

                String send_pro_code = jsonUtil.getProvinceCode(send_pro);
                String send_city_code = jsonUtil.getCityCode(send_pro,send_city,send_dist);
                String send_dist_code = jsonUtil.getDistCode(send_pro,send_city,send_dist);

                String rec_pro_code = jsonUtil.getProvinceCode(rec_pro);
                String  rec_city_code = jsonUtil.getCityCode(rec_pro,rec_city,rec_dist);
                String rec_dist_code = jsonUtil.getDistCode(rec_pro,rec_city,rec_dist);

                // 电商企业 code
                String  ds_code = jsonUtil.getDsCode(ds_name);

                // 获取企业code
                String pc_code= jsonUtil.getPcCode(pc_name);

                // 生成最终中间结果字符串
                String  ds = jsonUtil.ds(ds_code,send_pro_code,send_city_code,send_dist_code,rec_pro_code,rec_city_code,rec_dist_code);
                String city = jsonUtil.byCity(send_city_code,rec_city_code);
                String  dist = jsonUtil.Dist(send_dist_code,rec_dist_code);
                String  key_city = jsonUtil.keyCityDataMsg(pc_code,send_city_code,send_pro_code,rec_city_code,rec_pro_code);
                String  pc = jsonUtil.pcDataMsg(pc_code,send_pro_code,send_city_code,send_dist_code,rec_pro_code,rec_city_code,rec_dist_code);
                String prov = jsonUtil.Prov(send_pro_code,rec_pro_code,rec_city_code,rec_dist_code);

                // 拼接最终结果json字符串
                String result_json_str = dt + "#" + ds + "#" + "1" + "#" + "224" + "#" + city + "#" + dist + "#" + key_city + "#" + pc + "#" + prov + "#" + "true" + "#" + "1" + "#" + "0" + "#" + "sp";

                // 返回最终结果
                return result_json_str;
            }
        }
        return dt + "##0#0######true#1#1#sp";
    }
    private String getOneRecordByCol(String tableName, String rowKey,String familyName, String columnName) throws IOException {
        if (table == null){
            table = new HTable(conf, Bytes.toBytes(tableName));
        }
        Get get = new Get(Bytes.toBytes(rowKey));
        get.addColumn(Bytes.toBytes(familyName), Bytes.toBytes(columnName)); // 获取指定列族和列修饰符对应的列
        Result result = table.get(get);
        try {
            for (KeyValue kv : result.list()) {
                return Bytes.toString(kv.getValue());
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return "error";
    }
    // 一次 put 多个列
    private void addData(String tableName, String rowKey, String[] column, String[] value) throws IOException {
        if (table == null){
            table = new HTable(conf, Bytes.toBytes(tableName));
        }
        Put put = new Put(Bytes.toBytes(rowKey));// 设置rowkey
        for (int j = 0; j < column.length; j++) {
            put.add(Bytes.toBytes("f"), Bytes.toBytes(column[j]), Bytes.toBytes(value[j]));
        }
        table.put(put);
    }
}
