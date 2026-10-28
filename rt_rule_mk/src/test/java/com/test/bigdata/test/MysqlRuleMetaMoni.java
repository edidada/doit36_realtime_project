package com.test.bigdata.test;

import org.apache.commons.io.FileUtils;
import org.junit.Test;
import org.roaringbitmap.RoaringBitmap;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.sql.*;

public class MysqlRuleMetaMoni {

    /**
     * 模拟插入  规则模型4的  规则定义
     * @throws SQLException
     * @throws IOException
     */
    @Test
    public void insertModel4Rule() throws SQLException, IOException {

        Connection conn = DriverManager.getConnection("jdbc:mysql://doitedu:3306/rtmk", "root", "root");

        PreparedStatement stmt = conn.prepareStatement(
                "insert into rule_meta_doit36 (model_id,param_json,static_crowd,status,publisher,create_time,update_time) values (?,?,?,?,?,?,?)");
        stmt.setInt(1,4);

        String json = FileUtils.readFileToString(new File("model4_test.json"), "utf-8");
        stmt.setString(2,json);

        RoaringBitmap bitmap = RoaringBitmap.bitmapOf(20, 21, 22);
        ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
        DataOutputStream dataOut = new DataOutputStream(byteOut);
        bitmap.serialize(dataOut);
        byte[] crowdBytes = byteOut.toByteArray();

        stmt.setBytes(3,crowdBytes);

        stmt.setInt(4,1);
        stmt.setString(5,"deep as sea");

        stmt.setTimestamp(6,new Timestamp(System.currentTimeMillis()));
        stmt.setTimestamp(7,new Timestamp(System.currentTimeMillis()));

        stmt.execute();

        stmt.close();
        conn.close();

    }


    /**
     * 模拟插入  规则模型1的 规则定义
     * @throws SQLException
     * @throws IOException
     */
    @Test
    public void insertModel1Rule() throws SQLException, IOException {

        Connection conn = DriverManager.getConnection("jdbc:mysql://doitedu:3306/rtmk", "root", "root");

        PreparedStatement stmt = conn.prepareStatement(
                "insert into rule_meta_doit36 (model_id,param_json,static_crowd,status,publisher,create_time,update_time) values (?,?,?,?,?,?,?)");
        stmt.setInt(1,1);

        String json = FileUtils.readFileToString(new File("D:\\dev_works\\doit36_realtime_project\\model1_test.json"), "utf-8");
        stmt.setString(2,json);

        RoaringBitmap bitmap = RoaringBitmap.bitmapOf();
        ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
        DataOutputStream dataOut = new DataOutputStream(byteOut);
        bitmap.serialize(dataOut);
        byte[] crowdBytes = byteOut.toByteArray();

        stmt.setBytes(3,crowdBytes);

        stmt.setInt(4,1);
        stmt.setString(5,"deep as sea");

        stmt.setTimestamp(6,new Timestamp(System.currentTimeMillis()));
        stmt.setTimestamp(7,new Timestamp(System.currentTimeMillis()));

        stmt.execute();

        stmt.close();
        conn.close();

    }


    @Test
    public void insertModel5Rule() throws SQLException, IOException {

        Connection conn = DriverManager.getConnection("jdbc:mysql://doitedu:3306/rtmk", "root", "root");

        PreparedStatement stmt = conn.prepareStatement(
                "insert into rule_meta_doit36 (model_id,model_calculator_code,param_json,static_crowd,status,publisher,create_time,update_time) values (?,?,?,?,?,?,?,?)");
        stmt.setInt(1,5);

        String code = FileUtils.readFileToString(new File("d:/a.groovy"), "utf-8");
        stmt.setString(2,code);

        String json = FileUtils.readFileToString(new File("D:\\dev_works\\doit36_realtime_project\\model5_test2.json"), "utf-8");
        stmt.setString(3,json);

        RoaringBitmap bitmap = RoaringBitmap.bitmapOf();
        ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
        DataOutputStream dataOut = new DataOutputStream(byteOut);
        bitmap.serialize(dataOut);
        byte[] crowdBytes = byteOut.toByteArray();

        stmt.setBytes(4,crowdBytes);

        stmt.setInt(5,1);
        stmt.setString(6,"deep as sea");

        stmt.setTimestamp(7,new Timestamp(System.currentTimeMillis()));
        stmt.setTimestamp(8,new Timestamp(System.currentTimeMillis()));

        stmt.execute();

        stmt.close();
        conn.close();

    }


}
