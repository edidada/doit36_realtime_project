package cn.doitedu.rtdw.utils;

import com.alibaba.fastjson.JSON;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;

import java.io.IOException;
import java.util.HashMap;

public class HttpRequestDemo {

    public static void main(String[] args) throws IOException {

        try (CloseableHttpClient client = HttpClientBuilder.create().build()) {

            HttpPost post = new HttpPost(RuntimeConfig.value("SIMWORDS_URL", "http://doitedu:8081/api/post/simwords"));
            post.addHeader("Content-type","application/json; charset=utf-8");
            post.addHeader("Accept","application/json");

            HashMap<String, String> mp = new HashMap<>();
            mp.put("origin","移动固态硬盘");

            post.setEntity(new StringEntity(JSON.toJSONString(mp),"utf-8"));

            // 发送请求
            HttpResponse response = client.execute(post);

            HttpEntity entity = response.getEntity();
            System.out.println(EntityUtils.toString(entity));


        }

    }


}
