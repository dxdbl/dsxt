package io.transwarp.udf;

import com.alibaba.fastjson.JSONObject;

import java.util.Calendar;
import java.util.Date;

/**
 * @description:
 * @author: mhf
 * @time: 2019/9/10 17:09
 */
public class test {
    public static void main(String[] args) {

        System.out.println(DateTime("2019-09-18 18:19:01"));
    }
    public  static String DateTime(String date) {
        Date now = new Date();
        int hour = now.getHours();
        if (hour < 10){
            return (date.split(" ")[0].replace("-", "") + "0" + hour);
        }else{
            return (date.split(" ")[0].replace("-", "") + hour);
        }

    }
}
