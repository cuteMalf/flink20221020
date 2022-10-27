package com.malf.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class AdsClickLog {
    //用户ID、广告ID、省份、城市和时间戳
    private Long userId;
    private Long adId;
    private String province;
    private String city;
    private Long timestamp;
}
