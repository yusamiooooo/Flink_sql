package org.example.flink_jdbc;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.*;
import org.apache.flink.table.catalog.hive.HiveCatalog;

public class flink_hive {
    public static void main(String[] args) {

        //  创建 Flink 流处理环境
        StreamExecutionEnvironment tenv = StreamExecutionEnvironment.getExecutionEnvironment();
        //  开发用构建流程序
        tenv.setParallelism(2);
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .inStreamingMode()

                .build();

        TableEnvironment tableEnv = TableEnvironment.create(settings);


        // 配置 HiveCatalog
        String name = "myhive";
        String database = "test";

        // 指向包含 hive-site.xml 目录的 URI
        String hiveConfDir = "src\\main\\resources";


        HiveCatalog hive = new HiveCatalog(name, database, hiveConfDir);
        tableEnv.registerCatalog("myhive", hive);
        // 将HiveCatalog设置为 当前Catalog
        tableEnv.useCatalog(name);

//        table.print();

        tableEnv.getConfig().setSqlDialect(SqlDialect.HIVE);
        tableEnv.useDatabase("test");
        String insertSQL = "select * from test_table;";
       tableEnv.executeSql(insertSQL).print();

    }
}
