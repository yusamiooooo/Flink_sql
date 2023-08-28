package org.example.flink_jdbc;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.type.TypeReference;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.runtime.generated.HashFunction;
import org.apache.flink.types.Row;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Map;

import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.JSON_VALUE;

public class flink_test {

    public static void main(String[] args) {
        Logger.getRootLogger().setLevel(Level.INFO);

        //  Configuration configuration = new Configuration();
        //  configuration.setString("rest.port","9091"); //指定 Flink Web UI 端口为9091
        //  StreamExecutionEnvironment tenv = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);


        //  创建 Flink 流处理环境
        StreamExecutionEnvironment tenv = StreamExecutionEnvironment.getExecutionEnvironment();
        //  开发用构建流程序
        //  设置水位线 1秒
        //   tenv.getConfig().setAutoWatermarkInterval(1000);
        tenv.setParallelism(2);
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .inStreamingMode()

                .build();

        TableEnvironment tableEnv = TableEnvironment.create(settings);

        //    tableEnv.getConfig().getConfiguration().setString("table.exec.emit.strategy", "append");
        //注册自定义函数
        tableEnv.createTemporaryFunction("splitfunction", splitfunction.class);
        //调用自定义函数
        //     tableEnv.sqlQuery("select word,length from str,lateral table(splitfunction,字段名)");

//加载clickhouse表
//
//                TableResult tableResult10 = tableEnv.executeSql(
//                " CREATE TABLE zz_test(" +
//                        "process  string, " +
//                        "ts  string, " +
//                        "order2    string, " +
//                        "rt as TO_TIMESTAMP(FROM_UNIXTIME(UNIX_TIMESTAMP('ts'), 'yyyy-MM-dd HH:mm:ss'))," +
//                        "WATERMARK FOR `rt` AS `rt` - INTERVAL '5' SECOND" +
//                        ")with(" +
//                        "  'connector' = 'kafka'," +
//                        " 'properties.bootstrap.servers' = '172.16.116.123:9092', " +
//                        " 'topic' = 'aiotest', " +
//                        " 'value.format' = 'json', " +
//                        //设置从最新的偏移量开始读取
//                        " 'scan.startup.mode' = 'latest-offset' " +
//                        ")");


        TableResult tableResult10 = tableEnv.executeSql(
                " CREATE TABLE zz_test(" +
                        "process  string,\n " +
                        "order2   string \n" +
                        ")with(" +
                        "  'connector' = 'kafka'," +
                        " 'properties.bootstrap.servers' = '172.16.116.123:9092', " +
                        " 'topic' = 'aiotest', " +
                        " 'value.format' = 'json', " +
                        //设置从最新的偏移量开始读取
                        " 'scan.startup.mode' = 'latest-offset' " +
                        ")");



        TableResult tableResult1 = tableEnv.executeSql(
                " CREATE TABLE test(" +
                        "process  string,\n " +
                        "state    ROW(skilllist ARRAY<ROW<userid STRING,skilllevel STRING,skill STRING>>) \n" +
                        ")with(" +
                        "  'connector' = 'kafka'," +
                        " 'properties.bootstrap.servers' = '172.16.116.123:9092', " +
                        " 'topic' = 'aitest', " +
                        " 'value.format' = 'json', " +
                        //设置从最新的偏移量开始读取
                        " 'scan.startup.mode' = 'latest-offset' " +
                        ")");


//        TableResult sourceTable = tableEnv.executeSql("CREATE TABLE city (" +
//                "  id INT,\n" +
//                "  province_id  INT,\n" +
//                "  city_name  STRING,\n" +
//                "  description  STRING,\n" +
//                "  PRIMARY KEY (id) NOT ENFORCED\n"+
//                ") WITH (\n" +
//                "'connector' = 'mysql-cdc',\n" +
//                "'hostname' = '172.16.100.158',\n" +
//                "'port' = '3306',\n" +
//                "'username' = 'dbadm',\n" +
//                "'password' = 'dbR3d18',\n" +
//                "'database-name' = 'test',\n" +
//                "'server-time-zone' = 'UTC',\n"+
//                " 'table-name' = 'city'\n" +
//                ")");

        TableResult sourceTablesink = tableEnv.executeSql(
                " CREATE TABLE testaio(" +
                        "id  INT, " +
                        "name  STRING " +
                        ")with(" +
                        "  'connector' = 'kafka'," +
                        " 'properties.bootstrap.servers' = '172.16.116.124:9092', " +
                        " 'topic' = 'testaio', " +
                        " 'scan.startup.mode' = 'earliest-offset'," +
                        "'format' = 'debezium-json'" +
                        ")");


//        TableResult tableResult11 = tableEnv.executeSql(
//                " CREATE TABLE dim_pe(" +
//                        "id  int, " +
//                        "name  string " +
//                        ")with(" +
//                        " 'connector' = 'jdbc'," +
//                        " 'url' ='jdbc:mysql://localhost:3306/test', " +
//                        "'table-name' = 'dim_pe', "+
//                        "'username' = 'root'," +
//                        "'password' = '123456', " +
//                        "'lookup.cache.max-rows' = '-1'," +
//                        "'lookup.cache.ttl' = '10s', " +
//                        "'lookup.max-retries' = '3'" +
//                        ")"
//        );
//
//        tableEnv.executeSql("create table test2(\n" +
//                "`process` STRING,\n" +
//                "`order2` STRING\n" +
//                ")WITH(\n" +
//                "'connector' = 'jdbc',\n" +
//                "'url' = 'jdbc:clickhouse://192.168.14.130:8123/test',\n" +
//                " 'username' = 'default',\n"+
//                " 'password' = '',\n"+
//                "'table-name' = 'test2'\n" +
//                ")"
//        );


        tableEnv.executeSql("select word,length,state,state.skilllist[1] as skilllist,state.skilllist[2].skill  as userid from test,lateral table(splitfunction(process))").print();



    }

    //创建自定义表函数  创建完需要注册
    @FunctionHint(output = @DataTypeHint("Row<word STRING,length INT>"))
    public static class splitfunction extends TableFunction {

        public void eval(String str) {
            //处理完需要用collect方法输出
            for (String world : str.split(" ")) {
                collect(Row.of(world, world.length()));
            }
        }
    }

    // 创建自定义标量函数
    @FunctionHint(output = @DataTypeHint("ROW<*>"))
    public static class JsonToTable extends ScalarFunction {
        public Row eval(String json_str) {

            ObjectMapper mapper = new ObjectMapper();
            try {
                // 将 JSON 字符串转换为 Map 对象
                Map<String, Object> map = mapper.readValue(json_str, new TypeReference<Map<String, Object>>() {
                });
                // 使用 org.apache.flink.types.Row.of 方法创建 Row 对象
                return Row.of(map);
            } catch (IOException e) {
                // 如果解析失败，返回空值
                return null;
            }
        }
    }

}