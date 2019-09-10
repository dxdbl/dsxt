package io.transwarp.udaf;

import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;
import org.apache.log4j.Logger;

/**
 * @description:
 * @author: mhf
 * @time: 2019/8/24 14:55
 */
public class pcAdd extends UDAF {
    public static class jsonStr{
        private String str;
    }

    public static class pcAddEvaluator implements UDAFEvaluator {

        jsonStr json;

        public pcAddEvaluator(){
            super();
            json = new jsonStr();
            init();
        }

        /**
         2.
         * init函数类似于构造函数，用于UDAF的初始化
         3.
         */
        public void init() {
            json.str = "";
        }
        /*
         * iterate接收传入的参数，并进行内部的轮转。其返回类型为boolean * * @param o * @return
         */
        public boolean iterate(String o) {
            if (o != null && !o.equals("")){
                if (json.str.equals("")){
                    json.str = o;
                }else{
                    json.str = jsonUtils.pcAdd(json.str,o);
                }
            }
            return true;
        }

        /**
         * terminatePartial无参数，其为iterate函数遍历结束后，返回轮转数据，
         * * terminatePartial类似于hadoop的Combiner * * @return
         */

        public String terminatePartial() {
            // combiner
            return (json.str.equals("")? "" :json.str);

        }

        /**
         * merge接收terminatePartial的返回结果，进行数据merge操作，其返回类型为boolean * * @param o * @return
         */
        public boolean merge(String json1) {
            if (!json1.equals("") && json1 != null) {
                if (json.str.equals("")){
                    json.str = json1;
                }else {
                    json.str = jsonUtils.pcAdd(json.str,json1);
                }
            }
            // TO-DO
            return true;
        }
        /**
         * terminate返回最终的聚集函数结果 * * @return
         */
        public String terminate() {
            return (json.str.equals("") ? "" : json.str);
        }
    }

}
