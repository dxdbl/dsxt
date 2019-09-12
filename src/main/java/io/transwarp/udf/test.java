package io.transwarp.udf;

import com.alibaba.fastjson.JSONObject;
import jodd.typeconverter.Convert;
import org.apache.commons.math3.util.Decimal64;

import java.util.HashSet;
import java.util.Set;

/**
 * @description:
 * @author: mhf
 * @time: 2019/9/10 17:09
 */
public class test {
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
    public static String ds(String ds_code,String s_p,String s_c,String s_d,String r_p,String r_c,String r_d) {
        String ss ="";
        if(s_p == r_p && s_c != r_c && s_d != r_d) {
            ss = "{\"ds\":{\""+ds_code+"\":{\"all\":1,\"allPrice\":224,\"prov\":{\""+r_p+"\":{\"city\":{\""+r_c+"\":{\"dist\":{\""+r_d+"\":{\"rec\":1,\"sed\":0,\"sedPrice\":0}},\"rec\":1,\"sed\":0,\"sedPrice\":0},\""+s_c+"\":{\"dist\":{\""+s_d+"\":{\"rec\":0,\"sed\":1,\"sedPrice\":224}},\"rec\":0,\"sed\":1,\"sedPrice\":224}},\"rec\":1,\"sed\":1,\"sedPrice\":224}}}}}";
        }else if(s_p == r_p && s_c == r_c && s_d != r_d){
            ss = "{\"ds\":{\""+ds_code+"\":{\"all\":1,\"allPrice\":224,\"prov\":{\""+r_p+"\":{\"city\":{\""+r_c+"\":{\"dist\":{\""+r_d+"\":{\"rec\":1,\"sed\":0,\"sedPrice\":0},\""+s_d+"\":{\"rec\":0,\"sed\":1,\"sedPrice\":224}},\"rec\":1,\"sed\":1,\"sedPrice\":224}},\"rec\":1,\"sed\":1,\"sedPrice\":224}}}}}";

        }else if(s_p == r_p && s_c == r_c && s_d == r_d) {
            ss = "{\"ds\":{\""+ds_code+"\":{\"all\":1,\"allPrice\":224,\"prov\":{\""+r_p+"\":{\"city\":{\""+r_c+"\":{\"dist\":{\""+r_d+"\":{\"rec\":1,\"sed\":1,\"sedPrice\":224}},\"rec\":1,\"sed\":1,\"sedPrice\":224}},\"rec\":1,\"sed\":1,\"sedPrice\":224}}}}}";
        }else {
            ss = "{\"ds\":{\""+ds_code+"\":{\"all\":1,\"allPrice\":224,\"prov\":{\""+s_p+"\":{\"rec\":0,\"sed\":1,\"sedPrice\":224,\"city\":{\""+s_c+"\":{\"rec\":0,\"sed\":1,\"sedPrice\":224,\"dist\":{\""+s_d+"\":{\"rec\":0,\"sed\":1,\"sedPrice\":224}}}}},\""+r_p+"\":{\"rec\":1,\"sed\":0,\"sedPrice\":0,\"city\":{\""+r_c+"\":{\"rec\":1,\"sed\":0,\"sedPrice\":0,\"dist\":{\""+r_d+"\":{\"rec\":1,\"sed\":0,\"sedPrice\":0}}}}}}}}}";
        }
        return ss;
    }
    public static void main(String[] args) {

        String a = "99999999999";
        System.out.println(Float.parseFloat(a));
    }
}
