package io.transwarp.udf;

import java.io.IOException;

public class test {

    public static void main(String[] args) {
        try {
            System.out.println(hbaseUtils.getOneRecordByCol("distcode","1","f","q1"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
