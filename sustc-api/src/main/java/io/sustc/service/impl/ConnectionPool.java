package io.sustc.service.impl;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import java.sql.Connection;
import java.sql.SQLException;

public class ConnectionPool {
    private static HikariDataSource dataSource;

    static {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl("jdbc:postgresql://localhost:5432/postgres?characterEncoding=UTF-8");
        config.setUsername("test");
        config.setPassword("Jianuo123?");
        // 设置连接池其他属性，如最大连接数、最小连接数等
        config.setMaximumPoolSize(20);
        dataSource = new HikariDataSource(config);
    }
    public static Connection getConnection() throws SQLException {
        return dataSource.getConnection();
    }
    public static void releaseConnection(Connection connection) {
        if (connection != null) {
            try {
                connection.close(); // 将连接返回连接池
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }
}

