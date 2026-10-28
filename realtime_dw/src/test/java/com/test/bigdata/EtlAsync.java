package com.test.bigdata;

import com.test.bigdata.rtdw.utils.RuntimeConfig;
import com.test.bigdata.rtdw.beans.EventBean;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.async.AsyncFunction;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.flink.streaming.util.retryable.AsyncRetryStrategies;
import org.apache.flink.streaming.util.retryable.RetryPredicates;
import org.apache.http.HttpResponse;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClients;
import org.apache.http.util.EntityUtils;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class EtlAsync {
    public static void main(String[] args) throws Exception {

        // 构造flink编程入口
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 一些必须参数设置
        env.setStateBackend(new HashMapStateBackend());
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointStorage(System.getProperty("flink.checkpoints.dir",
                new java.io.File(System.getProperty("java.io.tmpdir"), "doit36-checkpoints").toURI().toString()));

        DataStreamSource<String> ds = env.socketTextStream(RuntimeConfig.value("SOCKET_HOST", "doitedu"), RuntimeConfig.port("SOCKET_PORT", 9090));

        AsyncFunction<String,String> func = new RichAsyncFunction<String, String>() {
            CloseableHttpAsyncClient client;
            Future<HttpResponse> future;
            HttpPost post;
            @Override
            public void open(Configuration parameters) throws Exception {

                RequestConfig cfg = RequestConfig.custom()
                        .setConnectTimeout(2000)
                        .setSocketTimeout(1000)
                        .build();

                client = HttpAsyncClients.custom()
                        .setMaxConnTotal(5)
                        .setDefaultRequestConfig(cfg)
                        .build();

                client.start();
                // HttpGet get = new HttpGet();

                post = new HttpPost(RuntimeConfig.value("SIMWORDS_URL", "http://doitedu:8081/api/post/simwords"));
                post.addHeader("Content-type","application/json; charset=utf-8");
                post.addHeader("Accept", "application/json");

            }

            @Override
            public void close() throws Exception {
                if (client != null) client.close();
            }

            @Override
            public void asyncInvoke(String input, ResultFuture<String> resultFuture) throws Exception {

                // 请求体参数
                post.setEntity(new StringEntity("{\"origin\":\""+input+"\"}", StandardCharsets.UTF_8));

                future = client.execute(post, null);


                CompletableFuture.supplyAsync(() -> {
                    try {
                        HttpResponse httpResponse = future.get();
                        String res = EntityUtils.toString(httpResponse.getEntity());
                        return res;
                    } catch (Exception e) {
                        return null;
                    }
                }).thenAccept(s -> {
                    resultFuture.complete(Collections.singletonList(s));
                });
            }
        };


        AsyncRetryStrategies.FixedDelayRetryStrategy strategy = new AsyncRetryStrategies.FixedDelayRetryStrategyBuilder<EventBean>(3, 2000)
                .ifException(RetryPredicates.HAS_EXCEPTION_PREDICATE)
                .ifResult(RetryPredicates.EMPTY_RESULT_PREDICATE)
                .build();

        SingleOutputStreamOperator<String> resStream = AsyncDataStream.unorderedWaitWithRetry(ds, func, 2000,
                TimeUnit.MILLISECONDS, strategy);

        resStream.print();

        env.execute();

    }

}
