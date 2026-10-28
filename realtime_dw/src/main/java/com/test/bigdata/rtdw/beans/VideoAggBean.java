package com.test.bigdata.rtdw.beans;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.sql.Timestamp;
import java.time.LocalDateTime;


@NoArgsConstructor
@AllArgsConstructor
@Data
public  class VideoAggBean {
    long user_id;
    String event_id;
    long event_time;
    String release_channel;
    String device_type;
    long video_id;
    String video_play_id;
    String video_type;
    String video_name;
    String video_album;
    String video_author;
    long video_timelong;
    LocalDateTime create_time;
    Long play_start_time;
    Long play_end_time;
}