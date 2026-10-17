package cn.doitedu;

import cn.doitedu.rtdw.utils.RuntimeConfig;
import org.apache.http.HttpResponse;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClients;
import org.apache.http.util.EntityUtils;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class Http {

    public static void main(String[] args) throws ExecutionException, InterruptedException, IOException {
        CloseableHttpAsyncClient client;
        Future<HttpResponse> future;
        HttpPost post;

        RequestConfig cfg = RequestConfig.custom()
                .setConnectTimeout(2000)
                .setSocketTimeout(1000)
                .build();

        client = HttpAsyncClients.custom()
                .setMaxConnTotal(5)
                .setDefaultRequestConfig(cfg)
                .build();

        try {
            client.start();
            // HttpGet get = new HttpGet();

            post = new HttpPost(RuntimeConfig.value("SIMWORDS_URL", "http://doitedu:8081/api/post/simwords"));
            post.addHeader("Content-type","application/json; charset=utf-8");
            post.addHeader("Accept", "application/json");

            post.setEntity(new StringEntity("{\"origin\":\""+"黑咖啡"+"\"}", StandardCharsets.UTF_8));
            future = client.execute(post, null);
            System.out.println(EntityUtils.toString(future.get().getEntity()));



            post.setEntity(new StringEntity("{\"origin\":\""+"无糖苦咖啡"+"\"}", StandardCharsets.UTF_8));
            future = client.execute(post, null);
            System.out.println(EntityUtils.toString(future.get().getEntity()));

        } finally {
            client.close();
        }

    }

}
