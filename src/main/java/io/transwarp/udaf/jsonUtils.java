package io.transwarp.udaf;

import com.alibaba.fastjson.JSONObject;

import java.util.*;

/**
 * @description:
 * @author: mhf
 * @time: 2019/8/24 22:35
 */
public class jsonUtils {
    static String str1 = "{\"city\":{\"10011\":{\"rec\":1,\"sed\":1,\"sedPrice\":224}}}";
    static String str2 = "{\"city\":{\"10011\":{\"rec\":1,\"sed\":1,\"sedPrice\":224}}}";

    public static String addJSON(String json1,String json2){
        JSONObject j1 = JSONObject.parseObject(json1);
        JSONObject j2 = JSONObject.parseObject(json2);
        return "error";
    }
    public static String mergeCityJson(String json1,String json2){
        JSONObject j1 = JSONObject.parseObject(json1);
        JSONObject j2 = JSONObject.parseObject(json2);


        //用Iterator迭代器遍历取值，建议用反射机制解析到封装好的对象中

        for (Map.Entry<String, Object> entry : j1.entrySet()) {
            System.out.println(entry.getKey() + ":" + entry.getValue());
        }
        return "error";
    }
    public static String pcJsonAdd(String json1,String json2){
        /**
         * @description:
         * @param json1
         * @param json2
         * @return: java.lang.String
         * @author: mhf
         * @time: 2019/8/27 16:26
         */
        JSONObject jo1 = JSONObject.parseObject(json1);
        JSONObject jo2 = JSONObject.parseObject(json2);

        JSONObject pc2 = jo2.getJSONObject("pc");

        Set<String> set2 = pc2.keySet();//第二个串的pc集合

        JSONObject pc = jo1.getJSONObject("pc");

        Set<String> set3 = new HashSet<String>();//pc相同的集合

        for(String s:set2){
            for(String s2:pc.keySet()){
                if(s.equals(s2)){
                    set3.add(s);
                }
            }
        }
        Set<String> set4 = new HashSet<String>();//第一个串中没有的pc值
        set4.addAll(set2);
        set4.removeAll(set3);


        if(set4.size()!=0){
            for(String s:set4){//把没有的pc值put进去
                jo1.getJSONObject("pc").put(s,pc2.getJSONObject(s));

            }
        }
        if(set3.size()!=0){//pc值相同的加起来
            for(String s:set3){
                JSONObject jo = pc.getJSONObject(s);
                jo1.getJSONObject("pc").getJSONObject(s).put("all",pc.getJSONObject(s).getInteger("all")+pc2.getJSONObject(s).getInteger("all"));
                jo1.getJSONObject("pc").getJSONObject(s).put("allPrice",pc.getJSONObject(s).getInteger("allPrice")+pc2.getJSONObject(s).getInteger("allPrice"));
//                System.out.println(jo);
                Set<String> set5 = pc.getJSONObject(s).getJSONObject("prov").keySet();//第一个串中的prov值
                Set<String> set6 = pc2.getJSONObject(s).getJSONObject("prov").keySet();//第二个串中的prov值
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

                if(set7.size()!=0){
                    for(String i:set7){
                        jo1.getJSONObject("pc").getJSONObject(s).getJSONObject("prov").getJSONObject(i).put("rec",
                                pc.getJSONObject(s).getJSONObject("prov").getJSONObject(i).getInteger("rec")+
                                        pc2.getJSONObject(s).getJSONObject("prov").getJSONObject(i).getInteger("rec"));
                        jo1.getJSONObject("pc").getJSONObject(s).getJSONObject("prov").getJSONObject(i).put("sed",
                                pc.getJSONObject(s).getJSONObject("prov").getJSONObject(i).getInteger("sed")+
                                        pc2.getJSONObject(s).getJSONObject("prov").getJSONObject(i).getInteger("sed"));
                        jo1.getJSONObject("pc").getJSONObject(s).getJSONObject("prov").getJSONObject(i).put("sedPrice",
                                pc.getJSONObject(s).getJSONObject("prov").getJSONObject(i).getInteger("sedPrice")+
                                        pc2.getJSONObject(s).getJSONObject("prov").getJSONObject(i).getInteger("sedPrice"));
                    }
                }

                if(set8.size()!=0){
                    for(String i:set8){
                        jo1.getJSONObject("pc").getJSONObject(s).getJSONObject("prov").put(i,
                                pc2.getJSONObject(s).getJSONObject("prov").getJSONObject(i));
                    }
                }


            }

        }
       return jo1.toString();
    }
    public static String cityAdd(String jsonStr1, String jsonStr2){
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

    public static String distAdd(String jsonStr1, String jsonStr2){
        JSONObject jo1 = JSONObject.parseObject(jsonStr1);
        JSONObject jo2 = JSONObject.parseObject(jsonStr2);

        JSONObject rec = jo1.getJSONObject("dist");
        JSONObject rec2 = jo2.getJSONObject("dist");
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
    public static String keyCityAdd(String jsonStr1, String jsonStr2){
        List<String> lt = new ArrayList<String>();
        lt.add("toProv");
        lt.add("toCity");
        lt.add("fromCity");
        lt.add("fromProv");
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
    public static String pcAdd(String json1,String json2){
        JSONObject jo1 = JSONObject.parseObject(json1);
        JSONObject jo2 = JSONObject.parseObject(json2);

        Set<String> set1 = jo1.getJSONObject("pc").keySet();
        Set<String> set2 = jo2.getJSONObject("pc").keySet();
        Set<String> set3 = new HashSet<String>();//pc相同的集合
        Set<String> set4 = new HashSet<String>();//pc相同的集合
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

                Set<String> set5 = jo1.getJSONObject("pc").getJSONObject(s).getJSONObject("prov").keySet();//第一个串中的prov值
                Set<String> set6 = jo2.getJSONObject("pc").getJSONObject(s).getJSONObject("prov").keySet();//第二个串中的prov值
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

                        Set<String> set9 = jo1.getJSONObject("pc").getJSONObject(s).getJSONObject("prov").getJSONObject(p).getJSONObject("city").keySet();
                        Set<String> set10 = jo2.getJSONObject("pc").getJSONObject(s).getJSONObject("prov").getJSONObject(p).getJSONObject("city").keySet();
                        Set<String> set11 = new HashSet<String>();//两个串相同的city
                        Set<String> set12 = new HashSet<String>();//第一个串中没有的city
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

                                Set<String> set13 = jo1.getJSONObject("pc").getJSONObject(s).getJSONObject("prov").getJSONObject(p).getJSONObject("city").getJSONObject(c).getJSONObject("dist").keySet();//第一个串中的dist值
                                Set<String> set14 = jo2.getJSONObject("pc").getJSONObject(s).getJSONObject("prov").getJSONObject(p).getJSONObject("city").getJSONObject(c).getJSONObject("dist").keySet();//第二个串中的dist值
                                Set<String> set15 = new HashSet<String>();//两个串相同的dist
                                Set<String> set16 = new HashSet<String>();//第一个串中没有的dist
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
                        Set<String> set10 = jo2.getJSONObject("ds").getJSONObject(d).getJSONObject("prov").getJSONObject(p).getJSONObject("city").keySet();
                        Set<String> set11 = new HashSet<String>();//两个串相同的city
                        Set<String> set12 = new HashSet<String>();//第一个串中没有的city
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

                                Set<String> set13 = jo1.getJSONObject("ds").getJSONObject(d).getJSONObject("prov").getJSONObject(p).getJSONObject("city").getJSONObject(c).getJSONObject("dist").keySet();//第一个串中的dist值
                                Set<String> set14 = jo2.getJSONObject("ds").getJSONObject(d).getJSONObject("prov").getJSONObject(p).getJSONObject("city").getJSONObject(c).getJSONObject("dist").keySet();//第二个串中的dist值
                                Set<String> set15 = new HashSet<String>();//两个串相同的dist
                                Set<String> set16 = new HashSet<String>();//第一个串中没有的dist
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
        List<String> cd = new ArrayList<String>();
        cd.add("city");
        cd.add("dist");

        JSONObject o1 = JSONObject.parseObject(j1);
        JSONObject o2 = JSONObject.parseObject(j2);

        JSONObject o3 = JSONObject.parseObject(o1.getString("prov"));
        JSONObject o32 = JSONObject.parseObject(o1.getString("prov"));
        JSONObject o4 = JSONObject.parseObject(o2.getString("prov"));

        for (Map.Entry<String, Object> entry1 : o32.entrySet()){
            for (Map.Entry<String, Object> entry2 : o4.entrySet()){
                if (o3.containsKey(entry2.getKey())){
                    if (entry1.getKey().equals(entry2.getKey())) {
                        //
                        JSONObject o5 = JSONObject.parseObject(entry1.getValue().toString());
                        JSONObject o6 = JSONObject.parseObject(entry2.getValue().toString());
                        int sum_rec = Integer.parseInt(o5.get("rec").toString()) + Integer.parseInt(o6.get("rec").toString());
                        int sum_sed = Integer.parseInt(o5.get("sed").toString()) + Integer.parseInt(o6.get("sed").toString());
                        int sum_sedPrice = Integer.parseInt(o5.get("sedPrice").toString()) + Integer.parseInt(o6.get("sedPrice").toString());
                        o5.put("rec", sum_rec);
                        o5.put("sed", sum_sed);
                        o5.put("sedPrice", sum_sedPrice);
                        o3.put(entry1.getKey(), o5);
                        JSONObject o7 = JSONObject.parseObject(o5.getString("to"));
                        JSONObject o8 = JSONObject.parseObject(o6.getString("to"));
                        //System.out.println(o7);

                        // 判断 o7和o8是否为空的情况
                        if (!o7.isEmpty() && !o8.isEmpty()) {

                            for (Map.Entry<String, Object> entry3 : o7.entrySet()) {
                                for (Map.Entry<String, Object> entry4 : o8.entrySet()) {
                                    if (!entry3.getKey().equals("city") && !entry3.getKey().equals("dist") && o7.containsKey(entry4.getKey()) && !entry4.getKey().equals("city") && !entry4.getKey().equals("city")) {
                                        if (entry3.getKey().equals(entry4.getKey())) {
                                            int sum_pro = Integer.parseInt(entry3.getValue().toString()) + Integer.parseInt(entry4.getValue().toString());
                                            //System.out.println(sum_pro);
                                            o7.put(entry3.getKey(), sum_pro);
                                            //System.out.println("o7---" + o7);
                                            o5.put("to", o7);
                                            //System.out.println("o5---" + o5);
                                        }
                                    } else if (!entry3.getKey().equals("city") && !entry3.getKey().equals("dist") && !o7.containsKey(entry4.getKey()) && !entry4.getKey().equals("city") && !entry4.getKey().equals("city")) {
                                        o7.put(entry4.getKey(), entry4.getValue());
                                        o5.put(entry3.getKey(), o7);
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
                                                int sum = Integer.parseInt(entry5.getValue().toString()) + Integer.parseInt(entry6.getValue().toString());
                                                //System.out.println(sum);
                                                o9.put(entry5.getKey(), sum);
                                                o7.put(cd.get(i), o9);
                                            }
                                        } else {
                                            o9.put(entry6.getKey(), entry6.getValue());
                                            //System.out.println("o9***" + o9);
                                            o7.put(cd.get(i), o9);
                                        }
                                    }
                                }

                            }

                        }else if (o7.isEmpty() && !o8.isEmpty()){
                            o5.put("to",o8);
                        }
                        // todo
                        o3.put(entry1.getKey(),o5);
                    }

                }
                else {
                    //System.out.println(entry2.getKey());
                    //System.out.println(entry2.getKey() + entry2.getValue());
                    o3.put(entry2.getKey(),entry2.getValue());
                }
            }
        }
        o1.put("prov",o3);
        return o1.toString();
    }

}
