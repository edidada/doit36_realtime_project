package cn.doitedu.rtdw.beans;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class EventBean {
    // 用户id
    int user_id;
    // 设备型号
    String device_type;
    // 发布渠道
    String release_channel;
    // 会话id
    String session_id;
    // 页面类型
    String page_type;
    // 页面url
    String page_url;
    // 事件ID
    String event_id;
    // 事件时间
    long event_time;

    // 事件属性
    Map<String, String> properties;

    // 切割会话id
    String session_cut_id;

    // 页面访问起始时间
    long page_start_time;

    // 页面访问结束时间
    long page_end_time;
}