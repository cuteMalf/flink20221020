package com.malf.utils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class JDBCUtil {
    public static Connection getMysqlConnection()  {

        return getJDBCConnection("com.mysql.cj.jdbc.Driver", "jdbc:mysql://10.118.21.43:3306/ph_eeanalysis?useSSL=false&characterEncoding=UTF-8&serverTimezone=Asia/Shanghai","root","Pinghang@123");
    }

    private static Connection getJDBCConnection(String driver, String url, String user, String password) {

        try {
            Class.forName(driver);
        } catch (ClassNotFoundException e) {
            new RuntimeException("你提供的驱动类不存在-->"+driver);
        }
        Connection connection =null;


        try {
            connection = DriverManager.getConnection(url, user, password);
        } catch (SQLException e) {
            e.printStackTrace();
            new RuntimeException("url,user,password.这三者可能有错误!");
        }


        return connection;
    }

    public static void close(Connection connection) throws SQLException {
        if (connection != null && !connection.isClosed()) {
            connection.close();

        }
    }

}
