package com.malf.dayfive;

import com.malf.bean.OrderEvent;
import com.malf.bean.TxEvent;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;

/**
 * 实现两条流的对账功能
 * 模拟订单流和交易流的对账功能
 *
 */
public class FlinkRealTimeReconciliation {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
//        environment.setParallelism(1);
        SingleOutputStreamOperator<OrderEvent> orderEventStream = environment.readTextFile("input/OrderLog.csv").map((MapFunction<String, OrderEvent>) value -> {
            String[] strings = value.split(",");
            return new OrderEvent(Long.parseLong(strings[0]), strings[1], strings[2], Long.parseLong(strings[3]));
        });

        SingleOutputStreamOperator<TxEvent> txEventStream = environment.readTextFile("input/ReceiptLog.csv").map((MapFunction<String, TxEvent>) value -> {
            String[] strings = value.split(",");
            return new TxEvent(strings[0], strings[1], Long.parseLong(strings[2]));
        });

        ConnectedStreams<OrderEvent, TxEvent> connectedStreams = orderEventStream.connect(txEventStream);
        connectedStreams
                .keyBy("txId","txId")
                .process(new CoProcessFunction<OrderEvent, TxEvent, String>() {
                    private HashMap<String, OrderEvent> orderMap = new HashMap<>();
                    private HashMap<String, TxEvent> txMap = new HashMap<>();
                    @Override
                    public void processElement1(OrderEvent value, CoProcessFunction<OrderEvent, TxEvent, String>.Context ctx, Collector<String> out) {
                        if (txMap.containsKey(value.getTxId())){
                            out.collect(value.getTxId()+" 对账成功");
                            txMap.remove(value.getTxId());
                        }else {
                            orderMap.put(value.getTxId(),value);
                        }
                    }

                    @Override
                    public void processElement2(TxEvent value, CoProcessFunction<OrderEvent, TxEvent, String>.Context ctx, Collector<String> out) {
                        if (orderMap.containsKey(value.getTxId())){
                            out.collect(value.getTxId()+" 对账成功");
                            orderMap.remove(value.getTxId());
                        }else {
                            txMap.put(value.getTxId(),value);
                        }
                    }
                })
                .print();

        environment.execute();
    }
}
