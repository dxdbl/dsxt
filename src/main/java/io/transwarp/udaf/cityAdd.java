package io.transwarp.udaf;

import com.alibaba.fastjson.JSONObject;
import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;

import java.util.HashSet;
import java.util.Set;

/**
 * @description:
 * @author: mhf
 * @time: 2019/8/24 14:55
 */
public class cityAdd extends UDAF {

    public class jsonStr{
        private String str;
    }

    public class cityAddEvaluator implements UDAFEvaluator {

        jsonStr json;

        public cityAddEvaluator(){
            super();
            json = new jsonStr();
            init();
        }

        /**
         2.
         * init函数类似于构造函数，用于UDAF的初始化
         3.
         */
        public void init() {
            json.str = "";
        }
        /*
         * iterate接收传入的参数，并进行内部的轮转。其返回类型为boolean * * @param o * @return
         */
        public boolean iterate(String o) {
            if (o != null && !o.equals("")){
                if (json.str.equals("")){
                    json.str = o;
                }else{
                    json.str = cityAdd(json.str,o);
                }
            }
            return true;
        }

        /**
         * terminatePartial无参数，其为iterate函数遍历结束后，返回轮转数据，
         * * terminatePartial类似于hadoop的Combiner * * @return
         */

        public String terminatePartial() {
            // combiner
            return (json.str.equals("")? "" :json.str);

        }

        /**
         * merge接收terminatePartial的返回结果，进行数据merge操作，其返回类型为boolean * * @param o * @return
         */
        public boolean merge(String json1) {
            if (!json1.equals("") && json1 != null) {
                if (json.str.equals("")){
                    json.str = json1;
                }else {
                    json.str = cityAdd(json.str,json1);
                }
            }
            // TO-DO
            return true;
        }
        /**
         * terminate返回最终的聚集函数结果 * * @return
         */
        public String terminate() {
            return (json.str.equals("") ? "" : json.str);
        }
    }
    public String cityAdd(String jsonStr1, String jsonStr2){
        JSONObject jo1 = JSONObject.parseObject(jsonStr1);
        JSONObject jo2 = JSONObject.parseObject(jsonStr2);

        JSONObject rec = jo1.getJSONObject("city");
        JSONObject rec2 = jo2.getJSONObject("city");
        Set<String> set1 = rec.keySet();//第一个串的city集合
        Set<String> set2 = rec2.keySet();//第二个串的city集合

        Set<String> set3 = new HashSet<String>();
        for (String s : set2) {
            for (String s2 : set1) {
                if (s.equals(s2)) {
                    set3.add(s);

                }
            }
        }
        Set<String> set4 = new HashSet<String>();
        set4.addAll(set2);
        set4.removeAll(set3);

        if (set4.size() != 0) {
            for (String s : set4) {
                rec.put(s, rec2.getJSONObject(s));

            }
        }
        if (set3.size() != 0) {
            for (String s : set3) {
                JSONObject jo = rec.getJSONObject(s);
                jo.put("rec", rec.getJSONObject(s).getInteger("rec") + rec2.getJSONObject(s).getInteger("rec"));
                jo.put("sed", rec.getJSONObject(s).getInteger("sed") + rec2.getJSONObject(s).getInteger("sed"));
                jo.put("sedPrice", rec.getJSONObject(s).getInteger("sedPrice") + rec2.getJSONObject(s).getInteger("sedPrice"));
            }
        }
        return jo1.toString();
    }
}
