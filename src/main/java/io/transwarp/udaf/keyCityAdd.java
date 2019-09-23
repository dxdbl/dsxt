package io.transwarp.udaf;

import com.alibaba.fastjson.JSONObject;
import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @description:
 * @author: mhf
 * @time: 2019/8/24 14:55
 */
public class keyCityAdd extends UDAF {

    private static List<String> lt = new ArrayList<String>(){{this.add("toProv");add("toCity");add("fromCity");add("fromProv");}};;


    public  class jsonStr{
        private String str;
    }

    public  class keyCityAddEvaluator implements UDAFEvaluator {

        jsonStr json;

        public keyCityAddEvaluator(){
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
                    json.str = keyCityAdd(json.str,o);
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
                    json.str = keyCityAdd(json.str,json1);
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

    public String keyCityAdd(String jsonStr1, String jsonStr2){
        JSONObject jo00 = JSONObject.parseObject(jsonStr1);
        JSONObject jo01 = JSONObject.parseObject(jsonStr2);
        JSONObject jo1 = JSONObject.parseObject(jo00.getString("keyCity"));
        JSONObject jo12 = JSONObject.parseObject(jo00.getString("keyCity"));
        JSONObject jo2 = JSONObject.parseObject(jo01.getString("keyCity"));

        for (Map.Entry<String, Object> entry1 : jo12.entrySet()) {
            for (Map.Entry<String, Object> entry2 : jo2.entrySet()){
                if (jo1.containsKey(entry2.getKey())){
                    if (entry1.getKey().equals(entry2.getKey())){
                        //System.out.println("101100 相等 ");
                        // to-do
                        JSONObject jo3 = JSONObject.parseObject(entry1.getValue().toString());
                        JSONObject jo4 = JSONObject.parseObject(entry2.getValue().toString());
                        //System.out.println("jo3 --- " + jo3);
                        //System.out.println("jo4 --- " + jo4);
                        for (int i = 0; i < lt.size(); i++) {
                            JSONObject jo5 = JSONObject.parseObject(jo3.getString(lt.get(i)));
                            JSONObject jo52 = JSONObject.parseObject(jo3.getString(lt.get(i)));
                            JSONObject jo6 = JSONObject.parseObject(jo4.getString(lt.get(i)));
                            //System.out.println("jo5 --- " + jo5);
                            //System.out.println("jo6 --- " + jo6);
                            for (Map.Entry<String, Object> entry3 : jo52.entrySet()){
                                for (Map.Entry<String, Object> entry4 : jo6.entrySet()){
                                    //System.out.println(entry3.getKey());
                                    //System.out.println(entry3.getValue());
                                    if (jo5.containsKey(entry4.getKey())){
                                        if (entry3.getKey().equals(entry4.getKey())){
                                            int sum = Integer.parseInt(entry3.getValue().toString()) + Integer.parseInt(entry4.getValue().toString());
                                            //System.out.println(sum);
                                            jo5.put(entry3.getKey(),sum);
                                            jo3.put(lt.get(i),jo5);
                                            //jo6.remove(entry3.getKey());
                                            //System.out.println("加和后的jo5 --- " + jo5);
                                        }
                                    }else {
                                        jo5.put(entry4.getKey(),entry4.getValue());
                                        jo3.put(lt.get(i),jo5);
                                    }
                                }
                            }
                            //jo4.remove(lt.get(i),jo6);
                            //System.out.println("####### jo3  " + jo3);
                        }
                        jo1.put(entry1.getKey(),jo3);
                        //jo2.remove(entry2.getKey(),jo4);
                        //System.out.println("###### jo3 " + jo3);
                    }

                }
                else {
                    jo1.put(entry2.getKey(),entry2.getValue());
                }
            }
        }
        jo00.put("keyCity",jo1);
        //System.out.println("###### jo00 " + jo00);
        return jo00.toString();
    }
}
