package io.transwarp.udaf;

import com.alibaba.fastjson.JSONObject;

import java.util.*;

/**
 * @description:
 * @author: mhf
 * @time: 2019/8/24 22:35
 */
public class jsonUtils {

    private static JSONObject jo = null;
    private static JSONObject jo00 = null;
    private static JSONObject jo01 = null;
    private static JSONObject jo1 = null;
    private static JSONObject jo12 = null;
    private static JSONObject jo2 = null;
    private static JSONObject jo3 = null;
    private static JSONObject o32 = null;
    private static JSONObject jo4 = null;
    private static JSONObject jo5 = null;
    private static JSONObject jo52 = null;
    private static JSONObject jo6 = null;
    private static JSONObject o7 = null;
    private static JSONObject o8 = null;
    private static JSONObject o9 = null;
    private static JSONObject o10 = null;
    private static JSONObject rec = null;
    private static JSONObject rec2 = null;
    private static Set<String> set1 = null;
    private static Set<String> set2 = null;
    private static Set<String> set3 = null;
    private static Set<String> set4 = null;
    private static Set<String> set5 = null;
    private static Set<String> set6 = null;
    private static Set<String> set7 = null;
    private static Set<String> set8 = null;
    private static Set<String> set9 = null;
    private static Set<String> set10 = null;
    private static Set<String> set11 = null;
    private static Set<String> set12 = null;
    private static Set<String> set13 = null;
    private static Set<String> set14 = null;
    private static Set<String> set15 = null;
    private static Set<String> set16 = null;
    private static List<String> lt = new ArrayList<String>(){{this.add("toProv");add("toCity");add("fromCity");add("fromProv");}};;
    private static List<String> cd = new ArrayList<String>(){{this.add("city");add("dist");}};
    private static float sum_pro;
    private static float sum;
    private static float sum_rec;
    private static float sum_sed;
    private static float sum_sedPrice;
    private static Map.Entry<String, Object> entry1;
    private static Map.Entry<String, Object> entry2;
    private static Map.Entry<String, Object> entry3;
    private static Map.Entry<String, Object> entry4;
    private static Map.Entry<String, Object> entry5;
    private static Map.Entry<String, Object> entry6;




    public static String cityAdd(String jsonStr1, String jsonStr2){
        jo1 = JSONObject.parseObject(jsonStr1);
        jo2 = JSONObject.parseObject(jsonStr2);

        rec = jo1.getJSONObject("city");
        rec2 = jo2.getJSONObject("city");
        set1 = rec.keySet();//第一个串的city集合
        set2 = rec2.keySet();//第二个串的city集合

        set3 = new HashSet<String>();
        for (String s : set2) {
            for (String s2 : set1) {
                if (s.equals(s2)) {
                    set3.add(s);

                }
            }
        }
        set4 = new HashSet<String>();
        set4.addAll(set2);
        set4.removeAll(set3);

        if (set4.size() != 0) {
            for (String s : set4) {
                rec.put(s, rec2.getJSONObject(s));

            }
        }
        if (set3.size() != 0) {
            for (String s : set3) {
                jo = rec.getJSONObject(s);
                jo.put("rec", rec.getJSONObject(s).getInteger("rec") + rec2.getJSONObject(s).getInteger("rec"));
                jo.put("sed", rec.getJSONObject(s).getInteger("sed") + rec2.getJSONObject(s).getInteger("sed"));
                jo.put("sedPrice", rec.getJSONObject(s).getInteger("sedPrice") + rec2.getJSONObject(s).getInteger("sedPrice"));
            }
        }
        return jo1.toString();
    }

    public static String distAdd(String jsonStr1, String jsonStr2){
        jo1 = JSONObject.parseObject(jsonStr1);
        jo2 = JSONObject.parseObject(jsonStr2);

        rec = jo1.getJSONObject("dist");
        rec2 = jo2.getJSONObject("dist");
        set1 = rec.keySet();//第一个串的city集合
        set2 = rec2.keySet();//第二个串的city集合

        set3 = new HashSet<String>();
        for (String s : set2) {
            for (String s2 : set1) {
                if (s.equals(s2)) {
                    set3.add(s);

                }
            }
        }
        set4 = new HashSet<String>();
        set4.addAll(set2);
        set4.removeAll(set3);

        if (set4.size() != 0) {
            for (String s : set4) {
                rec.put(s, rec2.getJSONObject(s));

            }
        }
        if (set3.size() != 0) {
            for (String s : set3) {
                jo = rec.getJSONObject(s);
                jo.put("rec", rec.getJSONObject(s).getInteger("rec") + rec2.getJSONObject(s).getInteger("rec"));
                jo.put("sed", rec.getJSONObject(s).getInteger("sed") + rec2.getJSONObject(s).getInteger("sed"));
                jo.put("sedPrice", rec.getJSONObject(s).getInteger("sedPrice") + rec2.getJSONObject(s).getInteger("sedPrice"));
            }
        }
        return jo1.toString();
    }
    public static String keyCityAdd(String jsonStr1, String jsonStr2){
        jo00 = JSONObject.parseObject(jsonStr1);
        jo01 = JSONObject.parseObject(jsonStr2);
        jo1 = JSONObject.parseObject(jo00.getString("keyCity"));
        jo12 = JSONObject.parseObject(jo00.getString("keyCity"));
        jo2 = JSONObject.parseObject(jo01.getString("keyCity"));

        for (Map.Entry<String, Object> entry1 : jo12.entrySet()) {
            for (Map.Entry<String, Object> entry2 : jo2.entrySet()){
                if (jo1.containsKey(entry2.getKey())){
                    if (entry1.getKey().equals(entry2.getKey())){
                        //System.out.println("101100 相等 ");
                        // to-do
                        jo3 = JSONObject.parseObject(entry1.getValue().toString());
                        jo4 = JSONObject.parseObject(entry2.getValue().toString());
                        //System.out.println("jo3 --- " + jo3);
                        //System.out.println("jo4 --- " + jo4);
                        for (int i = 0; i < lt.size(); i++) {
                            jo5 = JSONObject.parseObject(jo3.getString(lt.get(i)));
                            jo52 = JSONObject.parseObject(jo3.getString(lt.get(i)));
                            jo6 = JSONObject.parseObject(jo4.getString(lt.get(i)));
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
    public static String pcAdd(String json1,String json2){
        jo1 = JSONObject.parseObject(json1);
        jo2 = JSONObject.parseObject(json2);

        set1 = jo1.getJSONObject("pc").keySet();
        set2 = jo2.getJSONObject("pc").keySet();
        set3 = new HashSet<String>();//pc相同的集合
        set4 = new HashSet<String>();//pc相同的集合
        for(String s:set2){
            for(String s2:set1){
                if(s.equals(s2)){
                    set3.add(s);
                }
            }
        }
        set4.addAll(set2);
        set4.removeAll(set3);

        if(set4.size()!=0){ //有不同的pc值
            for(String s:set4){//把没有的pc值put进去
                jo1.getJSONObject("pc").put(s,jo2.getJSONObject("pc").getJSONObject(s));

            }
        }

        if(set3.size()!=0){
            for(String s:set3){
                jo1.getJSONObject("pc").getJSONObject(s).put("all",
                        jo1.getJSONObject("pc").getJSONObject(s).getInteger("all")+
                                jo2.getJSONObject("pc").getJSONObject(s).getInteger("all"));
                jo1.getJSONObject("pc").getJSONObject(s).put("allPrice",
                        jo1.getJSONObject("pc").getJSONObject(s).getInteger("allPrice")+
                                jo2.getJSONObject("pc").getJSONObject(s).getInteger("allPrice"));

                set5 = jo1.getJSONObject("pc").getJSONObject(s).getJSONObject("prov").keySet();//第一个串中的prov值
                set6 = jo2.getJSONObject("pc").getJSONObject(s).getJSONObject("prov").keySet();//第二个串中的prov值
                set7 = new HashSet<String>();//两个串相同得到prov
                set8 = new HashSet<String>();//第一个串中没有的prov
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
                        jo1.getJSONObject("pc").getJSONObject(s).getJSONObject("prov").put(i,
                                jo2.getJSONObject("pc").getJSONObject(s).getJSONObject("prov").getJSONObject(i));
                    }
                }
                if(set7.size()!=0){//有相同的prov值
                    for(String p:set7){
                        jo1.getJSONObject("pc").getJSONObject(s).getJSONObject("prov").getJSONObject(p).put("rec",
                                jo1.getJSONObject("pc").getJSONObject(s).getJSONObject("prov").getJSONObject(p).getInteger("rec")+
                                        jo2.getJSONObject("pc").getJSONObject(s).getJSONObject("prov").getJSONObject(p).getInteger("rec"));
                        jo1.getJSONObject("pc").getJSONObject(s).getJSONObject("prov").getJSONObject(p).put("sed",
                                jo1.getJSONObject("pc").getJSONObject(s).getJSONObject("prov").getJSONObject(p).getInteger("sed")+
                                        jo2.getJSONObject("pc").getJSONObject(s).getJSONObject("prov").getJSONObject(p).getInteger("sed"));
                        jo1.getJSONObject("pc").getJSONObject(s).getJSONObject("prov").getJSONObject(p).put("sedPrice",
                                jo1.getJSONObject("pc").getJSONObject(s).getJSONObject("prov").getJSONObject(p).getInteger("sedPrice")+
                                        jo2.getJSONObject("pc").getJSONObject(s).getJSONObject("prov").getJSONObject(p).getInteger("sedPrice"));

                        set9 = jo1.getJSONObject("pc").getJSONObject(s).getJSONObject("prov").getJSONObject(p).getJSONObject("city").keySet();
                        set10 = jo2.getJSONObject("pc").getJSONObject(s).getJSONObject("prov").getJSONObject(p).getJSONObject("city").keySet();
                        set11 = new HashSet<String>();//两个串相同的city
                        set12 = new HashSet<String>();//第一个串中没有的city
                        for(String i:set9){
                            for(String j:set10){
                                if(i.equals(j)){
                                    set11.add(i);
                                }
                            }
                        }
                        if(set12.size()!=0){
                            for(String i:set12){
                                jo1.getJSONObject("pc").getJSONObject(s).getJSONObject("prov").getJSONObject(p).getJSONObject("city").put(i,
                                        jo2.getJSONObject("pc").getJSONObject(s).getJSONObject("prov").getJSONObject(p).getJSONObject("city").getJSONObject(i)
                                );
                            }
                        }

                        if(set11.size()!=0){
                            for(String c:set11){
                                jo1.getJSONObject("pc").getJSONObject(s).getJSONObject("prov").getJSONObject(p).getJSONObject("city").getJSONObject(c).put(
                                        "rec",jo1.getJSONObject("pc").getJSONObject(s).getJSONObject("prov").getJSONObject(p).getJSONObject("city").getJSONObject(c).getInteger("rec")
                                                +jo2.getJSONObject("pc").getJSONObject(s).getJSONObject("prov").getJSONObject(p).getJSONObject("city").getJSONObject(c).getInteger("rec")
                                );
                                jo1.getJSONObject("pc").getJSONObject(s).getJSONObject("prov").getJSONObject(p).getJSONObject("city").getJSONObject(c).put(
                                        "sed",jo1.getJSONObject("pc").getJSONObject(s).getJSONObject("prov").getJSONObject(p).getJSONObject("city").getJSONObject(c).getInteger("sed")
                                                +jo2.getJSONObject("pc").getJSONObject(s).getJSONObject("prov").getJSONObject(p).getJSONObject("city").getJSONObject(c).getInteger("sed")
                                );
                                jo1.getJSONObject("pc").getJSONObject(s).getJSONObject("prov").getJSONObject(p).getJSONObject("city").getJSONObject(c).put(
                                        "sedPrice",jo1.getJSONObject("pc").getJSONObject(s).getJSONObject("prov").getJSONObject(p).getJSONObject("city").getJSONObject(c).getInteger("sedPrice")
                                                +jo2.getJSONObject("pc").getJSONObject(s).getJSONObject("prov").getJSONObject(p).getJSONObject("city").getJSONObject(c).getInteger("sedPrice")
                                );

                                set13 = jo1.getJSONObject("pc").getJSONObject(s).getJSONObject("prov").getJSONObject(p).getJSONObject("city").getJSONObject(c).getJSONObject("dist").keySet();//第一个串中的dist值
                                set14 = jo2.getJSONObject("pc").getJSONObject(s).getJSONObject("prov").getJSONObject(p).getJSONObject("city").getJSONObject(c).getJSONObject("dist").keySet();//第二个串中的dist值
                                set15 = new HashSet<String>();//两个串相同的dist
                                set16 = new HashSet<String>();//第一个串中没有的dist
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
                                        jo1.getJSONObject("pc").getJSONObject(s).getJSONObject("prov").getJSONObject(p).getJSONObject("city").getJSONObject(c).getJSONObject("dist").put(i,
                                                jo2.getJSONObject("pc").getJSONObject(s).getJSONObject("prov").getJSONObject(p).getJSONObject("city").getJSONObject(c).getJSONObject("dist").getJSONObject(i));
                                    }
                                }

                                if(set15.size()!=0){
                                    for(String d:set15){
                                        jo1.getJSONObject("pc").getJSONObject(s).getJSONObject("prov").getJSONObject(p).getJSONObject("city").getJSONObject(c).getJSONObject("dist").getJSONObject(d).put("rec",
                                                jo1.getJSONObject("pc").getJSONObject(s).getJSONObject("prov").getJSONObject(p).getJSONObject("city").getJSONObject(c).getJSONObject("dist").getJSONObject(d).getInteger("rec")+
                                                        jo2.getJSONObject("pc").getJSONObject(s).getJSONObject("prov").getJSONObject(p).getJSONObject("city").getJSONObject(c).getJSONObject("dist").getJSONObject(d).getInteger("rec") );
                                        jo1.getJSONObject("pc").getJSONObject(s).getJSONObject("prov").getJSONObject(p).getJSONObject("city").getJSONObject(c).getJSONObject("dist").getJSONObject(d).put("sed",
                                                jo1.getJSONObject("pc").getJSONObject(s).getJSONObject("prov").getJSONObject(p).getJSONObject("city").getJSONObject(c).getJSONObject("dist").getJSONObject(d).getInteger("sed")+
                                                        jo2.getJSONObject("pc").getJSONObject(s).getJSONObject("prov").getJSONObject(p).getJSONObject("city").getJSONObject(c).getJSONObject("dist").getJSONObject(d).getInteger("sed") );
                                        jo1.getJSONObject("pc").getJSONObject(s).getJSONObject("prov").getJSONObject(p).getJSONObject("city").getJSONObject(c).getJSONObject("dist").getJSONObject(d).put("sedPrice",
                                                jo1.getJSONObject("pc").getJSONObject(s).getJSONObject("prov").getJSONObject(p).getJSONObject("city").getJSONObject(c).getJSONObject("dist").getJSONObject(d).getInteger("sedPrice")+
                                                        jo2.getJSONObject("pc").getJSONObject(s).getJSONObject("prov").getJSONObject(p).getJSONObject("city").getJSONObject(c).getJSONObject("dist").getJSONObject(d).getInteger("sedPrice") );
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
    // 电商企业
    public static String dsAdd(String json1,String json2){
        jo1 = JSONObject.parseObject(json1);
        jo2 = JSONObject.parseObject(json2);

        set1 = jo1.getJSONObject("ds").keySet();
        set2 = jo2.getJSONObject("ds").keySet();
        set3 = new HashSet<String>();//ds相同的集合
        set4 = new HashSet<String>();//第一个串中没有的ds值
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

                set5 = jo1.getJSONObject("ds").getJSONObject(d).getJSONObject("prov").keySet();//第一个串中的prov值
                set6 = jo2.getJSONObject("ds").getJSONObject(d).getJSONObject("prov").keySet();//第二个串中的prov值
                set7 = new HashSet<String>();//两个串相同得到prov
                set8 = new HashSet<String>();//第一个串中没有的prov
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

                        set9 = jo1.getJSONObject("ds").getJSONObject(d).getJSONObject("prov").getJSONObject(p).getJSONObject("city").keySet();
                        set10 = jo2.getJSONObject("ds").getJSONObject(d).getJSONObject("prov").getJSONObject(p).getJSONObject("city").keySet();
                        set11 = new HashSet<String>();//两个串相同的city
                        set12 = new HashSet<String>();//第一个串中没有的city
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

                                set13 = jo1.getJSONObject("ds").getJSONObject(d).getJSONObject("prov").getJSONObject(p).getJSONObject("city").getJSONObject(c).getJSONObject("dist").keySet();//第一个串中的dist值
                                set14 = jo2.getJSONObject("ds").getJSONObject(d).getJSONObject("prov").getJSONObject(p).getJSONObject("city").getJSONObject(c).getJSONObject("dist").keySet();//第二个串中的dist值
                                set15 = new HashSet<String>();//两个串相同的dist
                                set16 = new HashSet<String>();//第一个串中没有的dist
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
    // 按省份 json串合并
    public static String provAdd(String j1,String j2){
        jo1 = JSONObject.parseObject(j1);
        jo2 = JSONObject.parseObject(j2);

        jo3 = JSONObject.parseObject(jo1.getString("prov"));
        o32 = JSONObject.parseObject(jo1.getString("prov"));
        jo4 = JSONObject.parseObject(jo2.getString("prov"));

        for (Map.Entry<String, Object> entry1 : o32.entrySet()){
            for (Map.Entry<String, Object> entry2 : jo4.entrySet()){
                if (jo3.containsKey(entry2.getKey())){
                    if (entry1.getKey().equals(entry2.getKey())) {
                        //
                        jo5 = JSONObject.parseObject(entry1.getValue().toString());
                        jo6 = JSONObject.parseObject(entry2.getValue().toString());
                        sum_rec = Float.parseFloat(jo5.get("rec").toString()) + Float.parseFloat(jo6.get("rec").toString());
                        sum_sed = Float.parseFloat(jo5.get("sed").toString()) + Float.parseFloat(jo6.get("sed").toString());
                        sum_sedPrice = Float.parseFloat(jo5.get("sedPrice").toString()) + Float.parseFloat(jo6.get("sedPrice").toString());
                        jo5.put("rec", sum_rec);
                        jo5.put("sed", sum_sed);
                        jo5.put("sedPrice", sum_sedPrice);
                        jo3.put(entry1.getKey(), jo5);
                        o7 = JSONObject.parseObject(jo5.getString("to"));
                        o8 = JSONObject.parseObject(jo6.getString("to"));
                        //System.out.println(o7);

                        // 判断 o7和o8是否为空的情况
                        if (!o7.isEmpty() && !o8.isEmpty()) {

                            for (Map.Entry<String, Object> entry3 : o7.entrySet()) {
                                for (Map.Entry<String, Object> entry4 : o8.entrySet()) {
                                    if (!entry3.getKey().equals("city") && !entry3.getKey().equals("dist") && o7.containsKey(entry4.getKey()) && !entry4.getKey().equals("city") && !entry4.getKey().equals("city")) {
                                        if (entry3.getKey().equals(entry4.getKey())) {
                                            sum_pro = Integer.parseInt(entry3.getValue().toString()) + Integer.parseInt(entry4.getValue().toString());
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
                                o9 = JSONObject.parseObject(o7.getString(cd.get(i)));
                                o10 = JSONObject.parseObject(o8.getString(cd.get(i)));

                                for (Map.Entry<String, Object> entry5 : o9.entrySet()) {
                                    for (Map.Entry<String, Object> entry6 : o10.entrySet()) {
                                        if (o9.containsKey(entry6.getKey())) {
                                            if (entry5.getKey().equals(entry6.getKey())) {
                                                sum = Float.parseFloat(entry5.getValue().toString()) + Float.parseFloat(entry6.getValue().toString());
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
