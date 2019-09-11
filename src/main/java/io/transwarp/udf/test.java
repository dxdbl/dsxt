package io.transwarp.udf;

/**
 * @description:
 * @author: mhf
 * @time: 2019/9/10 17:09
 */
public class test {
    public static void main(String[] args) {

        String test = "东莞市,";
        if (test.contains(",")){
            System.out.println("含有逗号!");
        }
        else{
            System.out.println("不包含逗号！");
        }
    }
}
