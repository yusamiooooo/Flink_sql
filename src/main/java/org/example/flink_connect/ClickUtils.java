package org.example.flink_connect;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class ClickUtils {
    private static Connection connection;

    public static Connection getConnection() throws ClassNotFoundException, SQLException {
//Class.froName装载一个类并且对其进行实例化
        Class.forName("ru.yandex.clickhouse.ClickHouseDriver");
        String url = "jdbc:clickhouse://192.168.142.130:8123/default";
        connection = DriverManager.getConnection(url);
        return connection;
    }

    public void close() throws SQLException {
        connection.close();
    }
}
