package io.transwarp.utils;
import io.transwarp.udf.hbaseUtils;

import java.io.*;

/**
 * @description:
 * @author: mhf
 * @time: 2019/9/18 17:24
 */
public class WirteIntoHbase {

    // 写入相应hbase表 参数1 : 文件绝对路径 参数2 : hbase 表名
    public static void main(String[] args) {

        String content = readTxtFile(args[0]);
        try {
            hbaseUtils.addRecord(args[1],"1","f","q1",content);
            System.out.println("write into " + args[1] + "success!");
        } catch (Exception e) {
            e.printStackTrace();
        }
        try {
            String txt = hbaseUtils.getOneRecordByCol(args[1],"1","f","q1");
            System.out.println("read from hbase table is : \n" + txt);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    //效率高
    public static String readTxtFile(String filePath) {
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
