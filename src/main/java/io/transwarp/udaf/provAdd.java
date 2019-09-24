package io.transwarp.udaf;

import com.alibaba.fastjson.JSONObject;
import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @description:
 * @author: mhf
 * @time: 2019/8/24 14:55
 */
public class provAdd extends UDAF {

    private static List<String> cd = new ArrayList<String>(){{this.add("city");add("dist");}};

    public  static class jsonStr{
        private String str;
    }

    public  static class provAddEvaluator implements UDAFEvaluator {

        jsonStr json;

        public provAddEvaluator(){
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
                    json.str = provAdd(json.str,o);
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
                    json.str = provAdd(json.str,json1);
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

    // 按省份 json串合并
    public static String provAdd(String j1,String j2){
        JSONObject jo1 = JSONObject.parseObject(j1);
        JSONObject jo2 = JSONObject.parseObject(j2);

        JSONObject jo3 = JSONObject.parseObject(jo1.getString("prov"));
        JSONObject   o32 = JSONObject.parseObject(jo1.getString("prov"));
        JSONObject  jo4 = JSONObject.parseObject(jo2.getString("prov"));

        for (Map.Entry<String, Object> entry1 : o32.entrySet()){
            for (Map.Entry<String, Object> entry2 : jo4.entrySet()){
                if (jo3.containsKey(entry2.getKey())){
                    if (entry1.getKey().equals(entry2.getKey())) {
                        //
                        JSONObject  jo5 = JSONObject.parseObject(entry1.getValue().toString());
                        JSONObject  jo6 = JSONObject.parseObject(entry2.getValue().toString());
                        float sum_rec = Float.parseFloat(jo5.get("rec").toString()) + Float.parseFloat(jo6.get("rec").toString());
                        float sum_sed = Float.parseFloat(jo5.get("sed").toString()) + Float.parseFloat(jo6.get("sed").toString());
                        float  sum_sedPrice = Float.parseFloat(jo5.get("sedPrice").toString()) + Float.parseFloat(jo6.get("sedPrice").toString());
                        jo5.put("rec", sum_rec);
                        jo5.put("sed", sum_sed);
                        jo5.put("sedPrice", sum_sedPrice);
                        jo3.put(entry1.getKey(), jo5);
                        JSONObject o7 = JSONObject.parseObject(jo5.getString("to"));
                        JSONObject o8 = JSONObject.parseObject(jo6.getString("to"));
                        //System.out.println(o7);

                        // 判断 o7和o8是否为空的情况
                        if (!o7.isEmpty() && !o8.isEmpty()) {

                            for (Map.Entry<String, Object> entry3 : o7.entrySet()) {
                                for (Map.Entry<String, Object> entry4 : o8.entrySet()) {
                                    if (!entry3.getKey().equals("city") && !entry3.getKey().equals("dist") && o7.containsKey(entry4.getKey()) && !entry4.getKey().equals("city") && !entry4.getKey().equals("city")) {
                                        if (entry3.getKey().equals(entry4.getKey())) {
                                            float  sum_pro = Integer.parseInt(entry3.getValue().toString()) + Integer.parseInt(entry4.getValue().toString());
                                            //System.out.println(sum_pro);
                                            o7.put(entry3.getKey(), sum_pro);
                                            //System.out.println("o7---" + o7);
                                            jo5.put("to", o7);
                                            //System.out.println("o5---" + o5);
                                        }
                                    } else if (!entry3.getKey().equals("city") && !entry3.getKey().equals("dist") && !o7.containsKey(entry4.getKey()) && !entry4.getKey().equals("city") && !entry4.getKey().equals("city")) {
                                        o7.put(entry4.getKey(), entry4.getValue());
                                        jo5.put(entry3.getKey(), o7);
                                        //System.out.println(o5);
                                    }
                                }
                            }

                            for (int i = 0; i < cd.size(); i++) {
                                JSONObject o9 = JSONObject.parseObject(o7.getString(cd.get(i)));
                                JSONObject o10 = JSONObject.parseObject(o8.getString(cd.get(i)));

                                for (Map.Entry<String, Object> entry5 : o9.entrySet()) {
                                    for (Map.Entry<String, Object> entry6 : o10.entrySet()) {
                                        if (o9.containsKey(entry6.getKey())) {
                                            if (entry5.getKey().equals(entry6.getKey())) {
                                                float sum = Float.parseFloat(entry5.getValue().toString()) + Float.parseFloat(entry6.getValue().toString());
                                                o9.put(entry5.getKey(), sum);
                                                o7.put(cd.get(i), o9);
                                            }
                                        } else {
                                            o9.put(entry6.getKey(), entry6.getValue());
                                            o7.put(cd.get(i), o9);
                                        }
                                    }
                                }
                            }
                        }else if (o7.isEmpty() && !o8.isEmpty()){
                            jo5.put("to",o8);
                        }
                        // todo
                        jo3.put(entry1.getKey(),jo5);
                    }
                }
                else {
                    //System.out.println(entry2.getKey());
                    //System.out.println(entry2.getKey() + entry2.getValue());
                    jo3.put(entry2.getKey(),entry2.getValue());
                }
            }
        }
        jo1.put("prov",jo3);
        return jo1.toString();
    }
}
