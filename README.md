## DSXT
### 单纯保存代码

##### 下面是slipstream中的创建stream语句(不一定是最终版本..)

```sql  
------------------    
------------------  数据预处理阶段(UDF函数 preprocessing)   
------------------   

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
ROW FORMAT DELIMITED  
FIELDS TERMINATED BY '\001'   
LINES TERMINATED BY '\n'  
TBLPROPERTIES("topic"="dsxt_online","kafka.zookeeper"="tdh4:2181,tdh8:2181,tdh13:2181",     "kafka.broker.list"="tdh4:9092,tdh8:9092,tdh13:9092");  

-- 创建 preprocessing 处理后的中间数据 topic  
drop stream after_preprocessing;  
create stream after_preprocessing (  
    after_preprocessing string  
    )   
ROW FORMAT DELIMITED  
FIELDS TERMINATED BY '\001'   
LINES TERMINATED BY  '\n'  
TBLPROPERTIES("topic"="after_preprocessing","kafka.zookeeper"="tdh4:2181,tdh8:2181,tdh13:2181",     "kafka.broker.list"="tdh4:9092,tdh8:9092,tdh13:9092");  

-- 创建 preprocessing  streamjob  
drop streamjob preprocessing;  
create streamjob preprocessing as  
("insert into after_preprocessing select preprocessing(jsonstr) from order_input")  
     jobproperties("morphling.job.checkpoint.interval"="3600000",  --checkpoint间隔,单位(毫秒)  
                    "morphling.job.enable.checkpoint"="true",  --定义该任务是否启用HA  
                    "morphling.task.max.failures"="1", --任务失败重试次数  
                    "stream.number.receivers"="2");  -- receivers 个数设置(开启checkpoint失效)  

-- 启动 streamjob preprocessing  
stop streamjob  preprocessing;  
start streamjob  preprocessing;  
------------------  
------------------  数据预处理阶段结束位置   
------------------  

------------------   
------------------  数据聚合阶段开始(UDAF处理)   
------------------    
-- 创建 stream 将数据按照 # 分隔符进行分列  
-- 创建 input stream 用于数据聚合  
DROP STREAM aggregate_input;  
CREATE STREAM aggregate_input (  
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
) ROW FORMAT DELIMITED  
FIELDS TERMINATED BY '#'   
LINES TERMINATED BY  '\n'  
TBLPROPERTIES("topic"="after_preprocessing","kafka.zookeeper"="tdh4:2181,tdh8:2181,tdh13:2181",     "kafka.broker.list"="tdh4:9092,tdh8:9092,tdh13:9092");  

-- 创建 window 窗口  
DROP STREAM aggregate_window;  
CREATE STREAM aggregate_window AS select  
id,ds,countryAllCnt,countryAllPrice,city,dist,keycity,pc,prov,s,secondSpeed,flag,sp  
from aggregate_input STREAMWINDOW w1 AS(length '10' second slide '10' second);  

-- 创建流 input stream 用于存储聚合后的结果(单分区 topic)  
DROP STREAM after_aggregate;  
CREATE STREAM after_aggregate (  
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
drop streamjob data_aggregate;  
create streamjob data_aggregate as  
("insert into after_aggregate select  
     id  
    ,dsadd(ds)  as ds  
    ,sum(countryAllCnt)  as countryAllCnt  
    ,sum(countryAllPrice)  as countryAllPrice  
    ,cityAdd(city)  as city  
    ,distAdd(dist)  as dist  
    ,keycityadd(keycity)  as keycity  
    ,pcadd(pc)  as pc  
    ,provadd(prov)  as prov  
    ,'true'  as s
    ,sum(secondSpeed)/10  as secondSpeed -- 消费速度(除数是窗口长度)  
    ,sum(flag) as flag  -- 重复单号数量  
    ,'sp'  as sp  
     from  aggregate_window GROUP BY id")  
     jobproperties("morphling.job.checkpoint.interval"="3600000",  --checkpoint间隔,单位(毫秒)  
                    "morphling.job.enable.checkpoint"="true",  --定义该任务是否启用HA  
                    "morphling.task.max.failures"="1", --任务失败重试次数  
                    "stream.number.receivers"="2");  -- receivers 个数设置(开启checkpoint失效)  

-- 启动 streamjob data_aggregate  
stop streamjob data_aggregate;  
start streamjob data_aggregate;  
----------------------------  
----------------------------  数据聚合阶段结束位置   
----------------------------  

----------------------------   
----------------------------  更新Redis数据阶段开始(UDF处理)   
----------------------------  
-- 读取 aggregate 数据, 将聚合后json加到redis中  
--- 1. 取出对应key的json串  
--- 2. 将从redis取出的json串和聚合后的串按照相同的key相加  
--- 3.将相加结果update回redis)  

-- 创建存储处理结果的 topic  
DROP STREAM dsxt_out;    
CREATE STREAM dsxt_out (  
     uuid string  
    ,msg string  
    ,time string  
    )   
ROW FORMAT DELIMITED FIELDS TERMINATED BY '#' LINES TERMINATED BY  '\n'  
TBLPROPERTIES("topic"="dsxt_out","kafka.zookeeper"="tdh4:2181,tdh8:2181,tdh13:2181",     "kafka.broker.list"="tdh4:9092,tdh8:9092,tdh13:9092");  

-- 创建 streamjob 
drop streamjob update_redis;  
create streamjob update_redis as  
("insert into dsxt_out select  
     uuid()  
    ,updateRedis(id,ds,countryAllCnt,countryAllPrice  
    ,city,dist,keycity,pc,prov,  
    s,secondSpeed,flag,sp) ,unix_timestamp()  
from  after_aggregate")  
jobproperties("morphling.job.checkpoint.interval"="3600000",  --checkpoint间隔,单位(毫秒)  
    "morphling.job.enable.checkpoint"="true",  --定义该任务是否启用HA  
    "morphling.task.max.failures"="1", --任务失败重试次数  
    "stream.number.receivers"="1");    -- receivers 个数设置(开启checkpoint失效)  

-- 启动流任务   
stop streamjob update_redis;  
start streamjob update_redis ;  
----------------------------   
----------------------------  更新Redis数据阶段结束位置  
----------------------------  

```
