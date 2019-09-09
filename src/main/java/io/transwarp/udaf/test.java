package io.transwarp.udaf;

import com.alibaba.fastjson.JSONObject;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @description:根据json文件获取相关值
 * @author: mhf
 * @time: 2019/8/20 8:54
 */
public class test {
    private static String json1 = "{\"keyCity\":{\"110101\":{\"fromCity\":{\"110112\":3,\"120114\":8,\"320200\":2,\"330700\":1,\"440100\":1},\"fromProv\":{\"999999\":3,\"120000\":8,\"320000\":2,\"330000\":1,\"440000\":1},\"toCity\":{\"330100\":1,\"360100\":1,\"370100\":1,\"371400\":1,\"410100\":1,\"440100\":1,\"440300\":2,\"610500\":1,\"999999\":6},\"toProv\":{\"310000\":6,\"330000\":1,\"360000\":1,\"370000\":2,\"410000\":1,\"440000\":3,\"610000\":1}},\"110102\":{\"fromCity\":{\"110112\":5,\"320700\":1,\"320800\":2,\"341800\":1,\"440300\":1,\"441900\":1,\"511300\":1,\"999999\":1},\"fromProv\":{\"110000\":6,\"320000\":3,\"340000\":1,\"440000\":2,\"510000\":1},\"toCity\":{},\"toProv\":{}},\"110105\":{\"fromCity\":{\"110112\":2,\"120114\":6},\"fromProv\":{\"110000\":2,\"120000\":6},\"toCity\":{\"999999\":560},\"toProv\":{\"999999\":560}}}}";
    private static String json2 = "{\"keyCity\":{\"110101\":{\"fromCity\":{\"888888\":3,\"120114\":8,\"320200\":2,\"330700\":1,\"440100\":1},\"fromProv\":{\"110000\":3,\"120000\":8,\"320000\":2,\"330000\":1,\"440000\":1},\"toCity\":{\"330100\":1,\"360100\":1,\"370100\":1,\"371400\":1,\"410100\":1,\"440100\":1,\"440300\":2,\"610500\":1,\"999999\":6},\"toProv\":{\"310000\":6,\"330000\":1,\"360000\":1,\"370000\":2,\"410000\":1,\"440000\":3,\"610000\":1}},\"110102\":{\"fromCity\":{\"110112\":5,\"320700\":1,\"320800\":2,\"341800\":1,\"440300\":1,\"441900\":1,\"511300\":1,\"999999\":1},\"fromProv\":{\"110000\":6,\"320000\":3,\"340000\":1,\"440000\":2,\"510000\":1},\"toCity\":{},\"toProv\":{}},\"110105\":{\"fromCity\":{\"110112\":2,\"120114\":6},\"fromProv\":{\"110000\":2,\"120000\":6},\"toCity\":{\"999999\":560},\"toProv\":{\"999999\":560}}}}";
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
                        System.out.println(o7);

                        // 判断 o7和o8是否为空的情况
                        if (!o7.isEmpty() && !o8.isEmpty()) {

                        for (Map.Entry<String, Object> entry3 : o7.entrySet()) {
                            for (Map.Entry<String, Object> entry4 : o8.entrySet()) {
                                if (!entry3.getKey().equals("city") && !entry3.getKey().equals("dist") && o7.containsKey(entry4.getKey()) && !entry4.getKey().equals("city") && !entry4.getKey().equals("city")) {
                                    if (entry3.getKey().equals(entry4.getKey())) {
                                        int sum_pro = Integer.parseInt(entry3.getValue().toString()) + Integer.parseInt(entry4.getValue().toString());
                                        System.out.println(sum_pro);
                                        o7.put(entry3.getKey(), sum_pro);
                                        System.out.println("o7---" + o7);
                                        o5.put("to", o7);
                                        System.out.println("o5---" + o5);
                                    }
                                } else if (!entry3.getKey().equals("city") && !entry3.getKey().equals("dist") && !o7.containsKey(entry4.getKey()) && !entry4.getKey().equals("city") && !entry4.getKey().equals("city")) {
                                    o7.put(entry4.getKey(), entry4.getValue());
                                    o5.put(entry3.getKey(), o7);
                                    System.out.println(o5);
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
                                            System.out.println(sum);
                                            o9.put(entry5.getKey(), sum);
                                            o7.put(cd.get(i), o9);
                                        }
                                    } else {
                                        o9.put(entry6.getKey(), entry6.getValue());
                                        System.out.println("o9***" + o9);
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
                    System.out.println(entry2.getKey());
                    System.out.println(entry2.getKey() + entry2.getValue());
                    o3.put(entry2.getKey(),entry2.getValue());
                }
            }
        }
        o1.put("prov",o3);
        return o1.toString();
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

    public static void main(String[] args) {
        System.out.println(keyCityAdd(json1,json2));
    }
}
