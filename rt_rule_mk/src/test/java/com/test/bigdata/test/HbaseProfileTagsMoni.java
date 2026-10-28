package com.test.bigdata.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

public class HbaseProfileTagsMoni {

    public static void main(String[] args) throws IOException {

        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "doitedu:2181");
        Connection hbaseConn = ConnectionFactory.createConnection(conf);
        Table tagTable = hbaseConn.getTable(TableName.valueOf("user_profile_tags"));

        Put put = new Put(Bytes.toBytes(20));
        put.addColumn("f".getBytes(),"tg01".getBytes(),Bytes.toBytes(5));
        put.addColumn("f".getBytes(),"tg08".getBytes(),Bytes.toBytes(28));


        Put put2 = new Put(Bytes.toBytes(21));
        put2.addColumn("f".getBytes(),"tg01".getBytes(),Bytes.toBytes(3));
        put2.addColumn("f".getBytes(),"tg08".getBytes(),Bytes.toBytes(28));

        tagTable.put(Arrays.asList(put,put2));


        tagTable.close();
        hbaseConn.close();

    }
}
