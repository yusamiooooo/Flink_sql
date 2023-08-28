package org.example;


import org.apache.flink.api.common.ExecutionMode;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;


public class Flink_sql {


    public static void main(String[] args) throws Exception{

        System.out.println("开始执行");



        StreamExecutionEnvironment tenv = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .inStreamingMode()

                .build();

        TableEnvironment tableEnv = TableEnvironment.create(settings);
        //设置重启策略
        tenv.getConfig().setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.seconds(10)));

        TableResult tableResult = tableEnv.executeSql(
                " CREATE TABLE z_test(" +
                        " systemid   string, " +
                        " domainname string " +
                        ")with(" +
                        "  'connector' = 'print'" +
                        ")");

//        TableResult tableResult10 = tableEnv.executeSql(
//                " CREATE TABLE zz_test(" +
//                        "  systemId  string, " +
//                        "  userId    string, " +
//                        "userUuid    string, " +
//                        "deviceType  string, " +
//                        "  imei      string, " +
//                        "seqId   string, " +
//                        "ip        string, " +
//                        " url    string, " +
//                        "browser    string, " +
//                        "eventName       string, " +
//                        "eventResult   string, " +
//                        "eventStartTime    TIMESTAMP, " +
//                        "eventEndTime       TIMESTAMP, " +
//                        "extraData  string, " +
//                        "isBounce    string, " +
//                        "cId    string " +
//                        ")with(" +
//                        "  'connector' = 'kafka'," +
//                        " 'properties.bootstrap.servers' = '172.16.116.123:9092,172.16.116.124:9092,172.16.116.125:9092', " +
//                        " 'topic' = 'test_clickhouse', " +
//                        " 'scan.startup.mode' = 'latest-offset' " +
//                        ")");
//        System.out.println("第一步建表");

/*
        TableResult result = tableEnv.executeSql("CREATE TABLE realtime_raw_table(" +
                " systemid  string, " +
                " userid  string, " +
                " useruuid  string, " +
                " devicetype  string, " +
                " imei  string, " +
                " seqid  string, " +
                " ip string, " +
                " url  string, " +
                " browser  string, " +
                " eventStartTime  TIMESTAMP, " +
                " eventEndTime  TIMESTAMP, " +
                " extradata  string, " +
                " isbounce  string, " +
                " cid string " +
                ")with(" +
                " 'type' = 'jdbc'," +
                " 'default-database' = 'mydatabase', " +
                " 'table-name' = 'realtime_raw_table', " +
                " 'username' = 'root', " +
                " 'password' = '123456', " +
                " 'driver' = 'org.mariadb.jdbc.Driver' " +

                ")");
*/

/*
        //将outputTable中的数据写入到Kafka的output_kafka主题
        TableResult outputTable = tableEnv.executeSql(
                "CREATE TABLE test(\n" +
                        "  systemId   string, " +
                        "  userId   string, " +
                        "  useruuid   string, " +
                        "  deviceType   string, " +
                        "  imei   string, " +
                        "  seqid   string, " +
                        "  ip  string, " +
                        "  url   string, " +
                        "  browser   string, " +
                        "  eventName   string, " +
                        "  eventResult   string, " +
                        "  eventStartTime   string, " +
                        "  eventEndTime   string, " +
                        "  extradata   string, " +
                        "  isbounce   string, " +
                        "  cid  string " +
                        ") WITH (\n" +
                        "  'connector' = 'jdbc',\n" +
                        "  'driver' =  'com.mysql.cj.jdbc.Driver',\n" +
                        "  'url' = 'jdbc:mysql://localhost:3306/world',\n" +
                        "  'table-name' = 'test',\n" +
                        "  'username' = 'root',\n" +
                        "  'password' = '123456'\n" +
                        ")"
        );
        //


 */
   //     System.out.println("mysql建表完成");

        TableResult test = tableEnv.executeSql(
                "CREATE  TABLE test2 (\n" +
                        "process    int,\n " +
                        "order2      String\n " +
                        ") WITH (\n" +
                        "  'connector' = 'jdbc',\n" +
                        "  'url' = 'jdbc:clickhouse://192.168.14.130:8123/test',\n" +
                        " 'username' = 'default',\n"+
                        " 'password' = '',\n"+
                        " 'table-name' = 'test2'\n" +
                        ")"
        );

        TableResult test2 = tableEnv.executeSql(
                "CREATE TABLE test1 (\n" +
                        "process    int,\n " +
                        "order2      String\n " +
                        ") WITH (\n" +
                        "  'connector' = 'clickhouse',\n" +
                        "  'url' = 'clickhouse://192.168.14.130:8123',\n" +
                        " 'username' = 'default',\n"+
                        " 'password' = '',\n"+
                        " 'database-name' = 'test',\n"+
                        " 'table-name' = 'test1'\n" +
                        ")"
        );
        System.out.println("clickhouse建表完成");




        System.out.println("第二步建表");

      // Table Energy_Property2 = tableEnv.sqlQuery("select * from Energy_Property");



      //  tableEnv.createTemporaryView("ResultTable", Energy_Property2);
   //   zz_test.executeInsert("test").print();

       System.out.println("开始sql查询");



       // tableEnv.executeSql("select * from test");


        System.out.println("开始执行插入");



        //
        System.out.println("完成");


    }
}
