package test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class Ck_data {
    public static void main(String[] args) {
        System.out.println("nihao");
    }
    public void test() throws SQLException {



        String insert_sq="INSERT INTO dest_table SELECT * FROM src_table";


        String select_sq2=" SELECT * FROM src_table";

        clickhouse_link clickhouse_link = new clickhouse_link();

        //执行插入sql语
        clickhouse_link.insert_update(insert_sq);

        //执行查询sql语
        clickhouse_link.select_ck(select_sq2);



    }
}
