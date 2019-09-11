## DSXT
### 单纯保存代码

##### 下面是slipstream中的创建stream语句(不是最终版本..)

```sql  

-- 创建 hbase 表存储非重复单号数据并且用于去重  
drop table order_online;  
CREATE  TABLE  order_online(   
     mailNo STRING  --rowkey 单号   
    ,flag string  -- 默认 '1' 判断是否重复   
    ,jsonstr string 
)   
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler';   

-- 创建 input stream 对接 kafka 的 topic dsxt_online    
DROP STREAM order_input;    
CREATE STREAM order_input ( 
    jsonstr string  
    )   
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' LINES TERMINATED BY  '\n'  
TBLPROPERTIES("topic"="dsxt_online","kafka.zookeeper"="tdh4:2181,tdh8:2181,tdh13:2181",     "kafka.broker.list"="tdh4:9092,tdh8:9092,tdh13:9092");  

-- 创建 stream 对接 UDF 处理之后生成的 json 串 (对应kafka的一个 topic ) 
DROP STREAM order_middle;   
CREATE STREAM order_middle AS   
select preprocessing(jsonstr) as order from order_input;  

-- 创建 stream 将数据按照 # 分隔符进行分列  
-- 创建 input stream用于数据聚合  
DROP STREAM order_aggregate;  
CREATE STREAM order_aggregate AS select
     agg[0] as id  
    ,agg[1] as ds  
    ,agg[2] as countryAllCnt -- 非重复 + 1   
    ,agg[3] as countryAllPrice -- 非重复 224 
    ,agg[4] AS city
    ,agg[5] as dist
    ,agg[6] as keycity
    ,agg[7] as pc
    ,agg[8] as prov
    ,agg[9] as s -- 默认 'true'  
    ,agg[10] as secondSpeed -- 消费速度 默认 1  
    ,agg[11] as flag -- 是否重复标志  
    ,agg[12] as sp  -- 预留 未来业务需要  
    from (select split(order,'#') as agg from  order_middle);  

-- 创建 窗口 用于数据聚合  
DROP STREAM order_aggregate_window;  
CREATE STREAM order_aggregate_window as select  
     id  
    ,ds  
    ,countryAllCnt   -- 非重复 + 1  
    ,countryAllPrice  -- 非重复 224  
    ,city  
    ,dist  
    ,keycity  
    ,pc  
    ,prov  
    ,s  -- 默认 'true'  
    ,secondSpeed   -- 消费速度 默认 1  
    ,flag  -- 是否重复标志  
    ,sp  
    from order_aggregate streamwindow w1 as (INTERVAL '5' SECOND);  


-- 创建流 input stream 用于存储聚合后的结果(单分区 topic)  
DROP STREAM order_after_aggregate;  
CREATE STREAM order_after_aggregate (  
     id string  
    ,ds string  
    ,countryAllCnt String   
    ,countryAllPrice string     
    ,city string    
    ,dist string    
    ,keycity string  
    ,pc string  
    ,prov string  
    ,s string  -- 默认为 'true'  
    ,secondSpeed string  -- 消费速度   
    ,flag string -- 重复数量值  
    ,sp string  -- 默认 'sp'  
    )  
ROW FORMAT DELIMITED FIELDS TERMINATED BY '#' LINES TERMINATED BY  '\n'  
TBLPROPERTIES("topic"="dsxt_aggregate","kafka.zookeeper"="tdh4:2181,tdh8:2181,tdh13:2181",  "kafka.broker.list"="tdh4:9092,tdh8:9092,tdh13:9092");

-- 创建 streamjob 将聚合结果写入 aggregate  
drop streamjob topic_data_aggregate;  
create streamjob topic_data_aggregate as  
("insert into order_after_aggregate select  
     id  
    ,dsadd(ds)  
    ,sum(countryAllCnt)  
    ,sum(countryAllPrice)  
    ,cityAdd(city)  
    ,distAdd(dist)  
    ,keycityadd(keycity)  
    ,pcadd(pc)  
    ,provadd(prov)  
    ,'true'  
    ,sum(secondSpeed)/5   -- 消费速度 除数是窗口长度  
    ,sum(flag)  -- 重复数量值  
    ,'sp'  
     from  order_aggregate_window GROUP BY id");  

-- 读取 aggregate 数据, 将聚合后json加到redis中  
--- 1. 取出对应key的json串  
--- 2. 将从redis取出的json串和聚合后的串按照相同的key相加  
--- 3.将相加结果update回redis)  

```
```java
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

```