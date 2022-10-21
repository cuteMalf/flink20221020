package com.malf.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * WaterSensor:水位传感器
 * id:传感器编号
 * ts:时间戳
 * vc:水位
 */

@Data
@AllArgsConstructor
@NoArgsConstructor
public class WaterSensor {
    private String id;
    private Long ts;
    private Integer vc;
}
