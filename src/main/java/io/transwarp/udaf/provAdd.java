package io.transwarp.udaf;

import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;
import org.apache.log4j.Logger;

/**
 * @description:
 * @author: mhf
 * @time: 2019/8/24 14:55
 */
public class provAdd extends UDAF {

    public static Logger log = Logger.getLogger(pcAdd.class);

    public static class jsonStr{
        private String str;
    }

    public static class provAddEvaluator implements UDAFEvaluator {

        jsonStr json;

        public provAddEvaluator(){
            super();
            json = new jsonStr();
            init();
            log.info("################# init #################");
        }

        /**
         2.
         * init函数类似于构造函数，用于UDAF的初始化
         3.
         */
        public void init() {
            json.str = "";
            log.info("################ init json str #####" + json.str);
        }
        /*
         * iterate接收传入的参数，并进行内部的轮转。其返回类型为boolean * * @param o * @return
         */
        public boolean iterate(String o) {
            log.info("################ iterate开始 #####" + json.str);
            if (o != null && !o.equals("")){
                if (json.str.equals("")){
                    json.str = o;
                }else{
                    json.str = jsonUtils.provAdd(json.str,o);
                }
                log.info("################ iterate内部 #####" + json.str);
            }
            return true;
        }

        /**
         * terminatePartial无参数，其为iterate函数遍历结束后，返回轮转数据，
         * * terminatePartial类似于hadoop的Combiner * * @return
         */

        public String terminatePartial() {
            // combiner
            log.info("################ terminatePartial #####" + json.str);
            return (json.str.equals("")? "" :json.str);

        }

        /**
         * merge接收terminatePartial的返回结果，进行数据merge操作，其返回类型为boolean * * @param o * @return
         */
        public boolean merge(String json1) {
            log.info("################ merge #####" + json.str);
            if (!json1.equals("") && json1 != null) {
                if (json.str.equals("")){
                    json.str = json1;
                }else {
                    json.str = jsonUtils.provAdd(json.str,json1);
                }
            }
            // TO-DO
            return true;
        }
        /**
         * terminate返回最终的聚集函数结果 * * @return
         */
        public String terminate() {
            log.info("################ terminate #####" + json.str);
            return (json.str.equals("") ? "" : json.str);
        }
    }

}
