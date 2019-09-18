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
    }
    //效率高
    public static String readTxtFile(String filePath,String tableName) {
        String json = "";
        try {
            File file = new File(filePath);
            if (file.isFile() && file.exists()) {
                InputStreamReader isr = new InputStreamReader(new FileInputStream(file), "utf-8");
                BufferedReader br = new BufferedReader(isr);
                String lineTxt = null;
                while ((lineTxt = br.readLine()) != null) {
                    //System.out.println(lineTxt);
                    json = json + lineTxt;
                }
                br.close();
            } else {
                return "文件不存在!";
            }
        } catch (Exception e) {
            return "文件读取错误!";
        }
        return json;
    }
}
