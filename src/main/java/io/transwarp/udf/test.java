package io.transwarp.udf;

import com.alibaba.fastjson.JSONObject;

/**
 * @description:
 * @author: mhf
 * @time: 2019/9/10 17:09
 */
public class test {
    public static void main(String[] args) {

        String a = "{\"num\":1.005E+7}";
        JSONObject jo = JSONObject.parseObject(a);
        float b = jo.getFloat("num");
        float c = b + 2;
        System.out.println(c);
    }
}
