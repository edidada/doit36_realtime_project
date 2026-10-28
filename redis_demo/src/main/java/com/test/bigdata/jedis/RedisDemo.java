package com.test.bigdata.jedis;

import redis.clients.jedis.Jedis;

import java.io.*;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RedisDemo {

    public static void main(String[] args) throws IOException, ClassNotFoundException {

        // 构造客户端对象
        Jedis jedis = new Jedis(System.getenv().getOrDefault("REDIS_HOST", "doitedu"),
                Integer.parseInt(System.getenv().getOrDefault("REDIS_PORT", "6379")));
        String password = System.getenv().getOrDefault("REDIS_PASSWORD", "");
        if (!password.isEmpty()) jedis.auth(password);
        jedis.select(Integer.parseInt(System.getenv().getOrDefault("REDIS_DATABASE", "0")));


        // 调用api发送请求，获取结果

        // 测试客户端能否联通
        String ping = jedis.ping();
        System.out.println(ping);


        /**
         * 1. 普通 key-value结构操作
         */
        // 插入一条  字符串key-value
        jedis.set("p001","吴亦凡同款摄像机");
        // 取值
        String p001 = jedis.get("p001");
        System.out.println(p001);

        // 删掉一条数据
        jedis.del("p001");
        String res = jedis.get("p001");
        System.out.println(res);


        // 用普通 key-value结构，放一个java对象
        Stu stu = new Stu(1, "猛哥", "18866888866", true, 38888.8);

        ByteArrayOutputStream arrayOutputStream = new ByteArrayOutputStream();
        ObjectOutputStream objOut = new ObjectOutputStream(arrayOutputStream);
        // 序列化
        objOut.writeObject(stu);

        byte[] objBytes = arrayOutputStream.toByteArray();

        jedis.set("1".getBytes(),objBytes);

        // 从redis将对象重新取出
        byte[] bytes = jedis.get("1".getBytes());
        // 反序列化
        ObjectInputStream objInput = new ObjectInputStream(new ByteArrayInputStream(bytes));
        Stu o = (Stu) objInput.readObject();
        System.out.println(o);


        /**
         * 2. List结构操作
         */
        jedis.lpush("taoge","刘亦菲","张天爱","猛哥");
        jedis.lpush("猛哥","欧阳锋","涛哥","星宿老怪","东方不败");

        // 往已存在的list中添加元素
        jedis.lpush("taoge","刘大灯");


        // 取出taoge的所有女朋友
        List<String> taoge = jedis.lrange("taoge", 0, -1);
        System.out.println(taoge);

        //取出猛哥的第 1个男朋友
        List<String> mengge = jedis.lrange("猛哥", 1, 1);
        System.out.println(mengge);


        /**
         * hash结构操作
         * 把一个班级的学生装在一个hash结构中
         */
        Map<String, String> students = new HashMap<>();
        students.put(1+"","猛哥");
        students.put(2+"","坤哥");
        students.put(3+"","甜姐");

        jedis.hset("doit36",4+"","帅哥");
        jedis.hmset("doit36",students);

        Map<String, String> doit36 = jedis.hgetAll("doit36");
        System.out.println(doit36);


    }

}
