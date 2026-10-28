package com.test.bigdata.rtdw.utils;

import org.apache.flink.table.functions.ScalarFunction;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

// 自定义函数，按小时，10分，30分，60分钟 截断取整
public class TimeRound extends ScalarFunction {
    public static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    public String eval(String dateTime,int intervalMinutes) throws ParseException {
        Date date = sdf.parse(dateTime);
        long ts = date.getTime();

        long factor = intervalMinutes*60*1000;
        long trunc = (ts/factor)*factor;

        return sdf.format(new Date(trunc));
    }

}
