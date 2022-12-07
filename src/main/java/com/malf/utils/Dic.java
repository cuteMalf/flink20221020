package com.malf.utils;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class Dic {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        environment
                .readTextFile("input/zhiye.txt")
                .flatMap(new RichFlatMapFunction<String, String>() {

                    private Connection mysqlConnection;

                    @Override
                    public void open(Configuration parameters) {
                        mysqlConnection = JDBCUtil.getMysqlConnection();
                    }

                    @Override
                    public void flatMap(String value, Collector<String> out) throws SQLException {
                        StringBuilder insertSql = new StringBuilder();
                        String[] strings = value.split(",");
                        String dicName="职业分类与代码";
                        String dicCode="zyflydm";
                        String itemCodeType="数字";
                        insertSql
                                .append("INSERT INTO ph_eeanalysis.yx_dic VALUES (null, now(), now(), 'phkj', 'phkj', '")
                                .append(dicName)
                                .append("', '")
                                .append(dicCode)
                                .append("', '")
                                .append(strings[1])
                                .append("', '")
                                .append(strings[0])
                                .append("', '")
                                .append(itemCodeType)
                                .append("',null")
                                .append(",null")
                                .append(",null")
                                .append(",null")
                                .append(",'1'")
                                .append(");");
                        PreparedStatement preparedStatement = mysqlConnection.prepareStatement(insertSql.toString());
                        preparedStatement.executeUpdate();
                        out.collect(insertSql.toString());

                    }

                    @Override
                    public void close() throws Exception {
                        JDBCUtil.close(mysqlConnection);
                    }
                })
                .print();





//                .flatMap((FlatMapFunction<String, String>) (value, out) -> {
//            StringBuilder insertSql = new StringBuilder();
//            String[] strings = value.split(",");
//            String dicName="职业分类与代码";
//            String dicCode="zyflydm";
//            insertSql
//                    .append("INSERT INTO ph_eeanalysis.yx_dic VALUES (null, now(), now(), 'phkj', 'phkj', '")
//                    .append(dicName)
//                    .append("', '")
//                    .append(dicCode)
//                    .append("', '")
//                    .append(strings[1])
//                    .append("', '")
//                    .append(strings[0])
//                    .append("', '")
//                    .append("数字'")
//                    .append(",null")
//                    .append(",null")
//                    .append(",null")
//                    .append(",null")
//                    .append(",'1'")
//                    .append(");");
//            out.collect(insertSql.toString());
//        })
//                .returns(Types.STRING)
//                .print();

        environment.execute();
    }
}
