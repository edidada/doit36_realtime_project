package com.test.bigdata.rtmk.beans;

import lombok.*;

import java.util.Map;

@Getter
@Setter
@Data
@NoArgsConstructor
@AllArgsConstructor
public class EventBean {

    private int user_id;
    private String event_id;
    private long event_time;
    private Map<String,String> properties;

}
