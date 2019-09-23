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
public class dsAdd extends UDAF {

    public static class jsonStr{
        private String str;
    }

    public class dsAddEvaluator implements UDAFEvaluator {

        jsonStr json;

        public dsAddEvaluator(){
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
                    json.str = dsAdd(json.str,o);
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
                    json.str = dsAdd(json.str,json1);
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

    // 电商企业
    private  String dsAdd(String json1,String json2){
        JSONObject jo1 = JSONObject.parseObject(json1);
        JSONObject jo2 = JSONObject.parseObject(json2);

        Set<String> set1 = jo1.getJSONObject("ds").keySet();
        Set<String> set2 = jo2.getJSONObject("ds").keySet();
        Set<String> set3 = new HashSet<String>();//ds相同的集合
        Set<String> set4 = new HashSet<String>();//第一个串中没有的ds值
        for(String i:set1){
            for(String j:set2){
                if(i.equals(j)){
                    set3.add(i);
                }
            }
        }
        set4.addAll(set2);
        set4.removeAll(set3);

        if(set4.size()!=0){ //有不同的ds值
            for(String d:set4){//把没有的ds值put进去
                jo1.getJSONObject("ds").put(d,jo2.getJSONObject("ds").getJSONObject(d));

            }
        }

        if(set3.size()!=0){//有相同的ds值
            for(String d:set3){
                jo1.getJSONObject("ds").getJSONObject(d).put("all",
                        jo1.getJSONObject("ds").getJSONObject(d).getInteger("all")+
                                jo2.getJSONObject("ds").getJSONObject(d).getInteger("all") );
                jo1.getJSONObject("ds").getJSONObject(d).put("allPrice",
                        jo1.getJSONObject("ds").getJSONObject(d).getInteger("allPrice")+
                                jo2.getJSONObject("ds").getJSONObject(d).getInteger("allPrice") );

                Set<String> set5 = jo1.getJSONObject("ds").getJSONObject(d).getJSONObject("prov").keySet();//第一个串中的prov值
                Set<String> set6 = jo2.getJSONObject("ds").getJSONObject(d).getJSONObject("prov").keySet();//第二个串中的prov值
                Set<String> set7 = new HashSet<String>();//两个串相同得到prov
                Set<String> set8 = new HashSet<String>();//第一个串中没有的prov
                for(String i:set5){
                    for(String j:set6){
                        if(i.equals(j)){
                            set7.add(i);
                        }
                    }
                }
                set8.addAll(set6);
                set8.removeAll(set7);
                if(set8.size()!=0){//有不相同的prov值
                    for(String i:set8){
                        jo1.getJSONObject("ds").getJSONObject(d).getJSONObject("prov").put(i,
                                jo2.getJSONObject("ds").getJSONObject(d).getJSONObject("prov").getJSONObject(i));
                    }
                }
                if(set7.size()!=0){
                    for(String p:set7){
                        jo1.getJSONObject("ds").getJSONObject(d).getJSONObject("prov").getJSONObject(p).put("rec",
                                jo1.getJSONObject("ds").getJSONObject(d).getJSONObject("prov").getJSONObject(p).getInteger("rec")+
                                        jo2.getJSONObject("ds").getJSONObject(d).getJSONObject("prov").getJSONObject(p).getInteger("rec"));
                        jo1.getJSONObject("ds").getJSONObject(d).getJSONObject("prov").getJSONObject(p).put("sed",
                                jo1.getJSONObject("ds").getJSONObject(d).getJSONObject("prov").getJSONObject(p).getInteger("sed")+
                                        jo2.getJSONObject("ds").getJSONObject(d).getJSONObject("prov").getJSONObject(p).getInteger("sed"));
                        jo1.getJSONObject("ds").getJSONObject(d).getJSONObject("prov").getJSONObject(p).put("sedPrice",
                                jo1.getJSONObject("ds").getJSONObject(d).getJSONObject("prov").getJSONObject(p).getInteger("sedPrice")+
                                        jo2.getJSONObject("ds").getJSONObject(d).getJSONObject("prov").getJSONObject(p).getInteger("sedPrice"));

                        Set<String> set9 = jo1.getJSONObject("ds").getJSONObject(d).getJSONObject("prov").getJSONObject(p).getJSONObject("city").keySet();
                        Set<String>  set10 = jo2.getJSONObject("ds").getJSONObject(d).getJSONObject("prov").getJSONObject(p).getJSONObject("city").keySet();
                        Set<String>  set11 = new HashSet<String>();//两个串相同的city
                        Set<String>  set12 = new HashSet<String>();//第一个串中没有的city
                        for(String i:set9){
                            for(String j:set10){
                                if(i.equals(j)){
                                    set11.add(i);
                                }
                            }
                        }
                        set12.addAll(set10);
                        set12.removeAll(set11);
                        if(set12.size()!=0){
                            for(String i:set12){
                                jo1.getJSONObject("ds").getJSONObject(d).getJSONObject("prov").getJSONObject(p).getJSONObject("city").put(i,
                                        jo2.getJSONObject("ds").getJSONObject(d).getJSONObject("prov").getJSONObject(p).getJSONObject("city").getJSONObject(i));
                            }
                        }

                        if(set11.size()!=0){
                            for(String c:set11){
                                jo1.getJSONObject("ds").getJSONObject(d).getJSONObject("prov").getJSONObject(p).getJSONObject("city").getJSONObject(c).put("rec",
                                        jo1.getJSONObject("ds").getJSONObject(d).getJSONObject("prov").getJSONObject(p).getJSONObject("city").getJSONObject(c).getInteger("rec")+
                                                jo2.getJSONObject("ds").getJSONObject(d).getJSONObject("prov").getJSONObject(p).getJSONObject("city").getJSONObject(c).getInteger("rec"));
                                jo1.getJSONObject("ds").getJSONObject(d).getJSONObject("prov").getJSONObject(p).getJSONObject("city").getJSONObject(c).put("sed",
                                        jo1.getJSONObject("ds").getJSONObject(d).getJSONObject("prov").getJSONObject(p).getJSONObject("city").getJSONObject(c).getInteger("sed")+
                                                jo2.getJSONObject("ds").getJSONObject(d).getJSONObject("prov").getJSONObject(p).getJSONObject("city").getJSONObject(c).getInteger("sed"));
                                jo1.getJSONObject("ds").getJSONObject(d).getJSONObject("prov").getJSONObject(p).getJSONObject("city").getJSONObject(c).put("sedPrice",
                                        jo1.getJSONObject("ds").getJSONObject(d).getJSONObject("prov").getJSONObject(p).getJSONObject("city").getJSONObject(c).getInteger("sedPrice")+
                                                jo2.getJSONObject("ds").getJSONObject(d).getJSONObject("prov").getJSONObject(p).getJSONObject("city").getJSONObject(c).getInteger("sedPrice"));

                                Set<String>  set13 = jo1.getJSONObject("ds").getJSONObject(d).getJSONObject("prov").getJSONObject(p).getJSONObject("city").getJSONObject(c).getJSONObject("dist").keySet();//第一个串中的dist值
                                Set<String>  set14 = jo2.getJSONObject("ds").getJSONObject(d).getJSONObject("prov").getJSONObject(p).getJSONObject("city").getJSONObject(c).getJSONObject("dist").keySet();//第二个串中的dist值
                                Set<String>  set15 = new HashSet<String>();//两个串相同的dist
                                Set<String>  set16 = new HashSet<String>();//第一个串中没有的dist
                                for(String i:set13){
                                    for(String j:set14){
                                        if(i.equals(j)){
                                            set15.add(i);
                                        }
                                    }
                                }
                                set16.addAll(set14);
                                set16.removeAll(set15);

                                if(set16.size()!=0){
                                    for(String i:set16){
                                        jo1.getJSONObject("ds").getJSONObject(d).getJSONObject("prov").getJSONObject(p).getJSONObject("city").getJSONObject(c).getJSONObject("dist").put(i,
                                                jo2.getJSONObject("ds").getJSONObject(d).getJSONObject("prov").getJSONObject(p).getJSONObject("city").getJSONObject(c).getJSONObject("dist").getJSONObject(i));
                                    }
                                }

                                if(set15.size()!=0){
                                    for(String di:set15){
                                        jo1.getJSONObject("ds").getJSONObject(d).getJSONObject("prov").getJSONObject(p).getJSONObject("city").getJSONObject(c).getJSONObject("dist").getJSONObject(di).put("rec",
                                                jo1.getJSONObject("ds").getJSONObject(d).getJSONObject("prov").getJSONObject(p).getJSONObject("city").getJSONObject(c).getJSONObject("dist").getJSONObject(di).getInteger("rec")+
                                                        jo2.getJSONObject("ds").getJSONObject(d).getJSONObject("prov").getJSONObject(p).getJSONObject("city").getJSONObject(c).getJSONObject("dist").getJSONObject(di).getInteger("rec") );
                                        jo1.getJSONObject("ds").getJSONObject(d).getJSONObject("prov").getJSONObject(p).getJSONObject("city").getJSONObject(c).getJSONObject("dist").getJSONObject(di).put("sed",
                                                jo1.getJSONObject("ds").getJSONObject(d).getJSONObject("prov").getJSONObject(p).getJSONObject("city").getJSONObject(c).getJSONObject("dist").getJSONObject(di).getInteger("sed")+
                                                        jo2.getJSONObject("ds").getJSONObject(d).getJSONObject("prov").getJSONObject(p).getJSONObject("city").getJSONObject(c).getJSONObject("dist").getJSONObject(di).getInteger("sed") );
                                        jo1.getJSONObject("ds").getJSONObject(d).getJSONObject("prov").getJSONObject(p).getJSONObject("city").getJSONObject(c).getJSONObject("dist").getJSONObject(di).put("sedPrice",
                                                jo1.getJSONObject("ds").getJSONObject(d).getJSONObject("prov").getJSONObject(p).getJSONObject("city").getJSONObject(c).getJSONObject("dist").getJSONObject(di).getInteger("sedPrice")+
                                                        jo2.getJSONObject("ds").getJSONObject(d).getJSONObject("prov").getJSONObject(p).getJSONObject("city").getJSONObject(c).getJSONObject("dist").getJSONObject(di).getInteger("sedPrice") );
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        return jo1.toString();
    }
}
