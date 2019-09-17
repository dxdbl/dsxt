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
        System.out.println(hget(args[0]));
    }
    public static String hget(String field){
        try {
            if (jedis == null) {
                jedis = new Jedis("10.11.220.15", 6379, 10000);
            }
            return jedis.hget("JSON_STORE","cachedD2019083117");

        } catch (Exception e) {
            e.printStackTrace();
        }
        return "error";
    }
}
