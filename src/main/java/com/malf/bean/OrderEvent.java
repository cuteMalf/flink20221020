package com.malf.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
/**
 * 订单数据
 */
public class OrderEvent {
    private Long orderId;
    private String eventType;
    //交易码
    private String txId;
    private Long eventTime;
}
