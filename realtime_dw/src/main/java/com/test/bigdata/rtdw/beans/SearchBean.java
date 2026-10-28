package com.test.bigdata.rtdw.beans;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class SearchBean {

    private long user_id;
    private long event_time;
    // 代表这次搜索生命周期的发起时间
    private long search_time;

    private String search_id;
    private String keyword;
    private String split_word;
    private String similar_word;


    private int search_return_cnt;
    private int search_click_cnt;


}
