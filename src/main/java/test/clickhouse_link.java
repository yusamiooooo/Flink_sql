package test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class clickhouse_link {

    public  Connection link(String link_url,String username,String pd) throws SQLException {

// 加载 ClickHouse 的 JDBC 驱动
        try {
            Class.forName("ru.yandex.clickhouse.ClickHouseDriver");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }

// 创建连接

        Connection conn = DriverManager.getConnection(link_url,username,pd);




        return conn;
    }

    public  void insert_update(String tql) throws SQLException {

// 加载 ClickHouse 的 JDBC 驱动
        try {
            Class.forName("ru.yandex.clickhouse.ClickHouseDriver");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }

// 创建连接

        Connection conn = DriverManager.getConnection("jdbc:clickhouse://192.168.14.130:8123/test", "default", "1234");

        // 创建 Statement 对象
        Statement stmt = conn.createStatement();

        stmt.executeUpdate(tql);





    }
    public  void select_ck(String tql) throws SQLException {

// 加载 ClickHouse 的 JDBC 驱动
        try {
            Class.forName("ru.yandex.clickhouse.ClickHouseDriver");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }

// 创建连接

        Connection conn = DriverManager.getConnection("jdbc:clickhouse://192.168.14.130:8123/test", "default", "1234");


        // 创建 Statement 对象（执行环境对象）
        Statement stmt = conn.createStatement();

        
        stmt.executeQuery(tql);





    }

}
