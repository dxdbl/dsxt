package io.transwarp.utils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;

/**
 * @description:
 * @author: mhf
 * @time: 2019/9/18 17:24
 */
public class WirteIntoHbase {

    public static void main(String[] args) {
        readTxtFile("E:\\0000-Transwarp\\0004-国家邮政局\\应用测试\\电商协同-中科软\\分省数据逻辑\\NameToValue.json");
    }
    //效率高
    public static void readTxtFile(String filePath) {
        try {
            File file = new File(filePath);
            if (file.isFile() && file.exists()) {
                InputStreamReader isr = new InputStreamReader(new FileInputStream(file), "utf-8");
                BufferedReader br = new BufferedReader(isr);
                String lineTxt = null;
                while ((lineTxt = br.readLine()) != null) {
                    System.out.println(lineTxt);
                }
                br.close();
            } else {
                System.out.println("文件不存在!");
            }
        } catch (Exception e) {
            System.out.println("文件读取错误!");
        }
    }
}
