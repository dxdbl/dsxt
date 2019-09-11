package io.transwarp.udaf;

import redis.clients.jedis.Jedis;

/**
 * @description:
 * @author: mhf
 * @time: 2019/8/30 17:37
 */
public class redisUtils {

    public static Jedis jedis;

    public static void main(String[] args) {
        try {
            if (jedis == null) {
                jedis = new Jedis("10.11.220.15", 6379, 10000);
            }

            System.out.println(jedis.hget("JSON_STORE","cachedD2019083117"));

        } catch (Exception e) {
            e.printStackTrace();
        }
        if (jedis != null) {
            jedis.close();

        }
    }
    public static String readRedis(String key,String field){
        if (jedis == null){
            jedis = new Jedis("10.11.220.15", 6379, 10000);
        }
        String json_str = jedis.hget(key,field);
        return json_str;
    }
    public static void putRedis(String key,String field,String value){
        if (jedis == null){
            jedis = new Jedis("10.11.220.15", 6379, 10000);
        }
        jedis.hset(key,field,value);
    }
}
