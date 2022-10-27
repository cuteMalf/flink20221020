package com.malf.bean;


import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
/**
 * 交易数据
 */
public class TxEvent {
    private String txId;
    private String payChannel;
    private Long eventTime;
}
