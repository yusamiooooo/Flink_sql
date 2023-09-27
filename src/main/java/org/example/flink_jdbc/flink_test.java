package org.example.flink_jdbc;



import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.type.TypeReference;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.runtime.generated.HashFunction;
import org.apache.flink.types.Row;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Map;

import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.JSON_ARRAY;
import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.JSON_VALUE;

public class flink_test {

    public static void main(String[] args) {


        //  Configuration configuration = new Configuration();
        //  configuration.setString("rest.port","9091"); //指定 Flink Web UI 端口为9091
        //   StreamExecutionEnvironment tenv = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);


        //  创建 Flink 流处理环境
        StreamExecutionEnvironment tenv = StreamExecutionEnvironment.getExecutionEnvironment();
        //  开发用构建流程序
        tenv.setParallelism(2);
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .inStreamingMode()

                .build();

        TableEnvironment tableEnv = TableEnvironment.create(settings);



        //    tableEnv.getConfig().getConfiguration().setString("table.exec.emit.strategy", "append");
        //注册自定义函数
      //  tableEnv.createTemporaryFunction("MySum", MySum.class);
        tableEnv.createTemporaryFunction("response_json_to_row", response_json_to_row.class);
        tableEnv.createTemporaryFunction("RequestJsonToRow", RequestJsonToRow.class);



        //调用自定义函数
        //    tableEnv.sqlQuery("select word,length from str,lateral table(splitfunction,字段名)");

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

//
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
////
////


        TableResult pg_dim_bash_device = tableEnv.executeSql(
                "    CREATE TABLE pg_dim_bash_device (" +
                        "  `id` BIGINT NOT NULL," +
                "  `device_code` VARCHAR(2147483647)," +
                "  `project_id` BIGINT," +
                " `is_delete` BOOLEAN," +
                " `create_time` TIMESTAMP(6)," +
                " `update_time` TIMESTAMP(6)," +
                " `create_user` BIGINT," +
                " `update_user` BIGINT," +
                " `token` VARCHAR(2147483647)," +
                        "PRIMARY KEY (`id`) NOT ENFORCED" +
                        ")" +
                        " WITH (" +
                " 'connector' = 'postgres-cdc'," +
                "'database-name' = 'svf_bash'," +
                " 'debezium.slot.name' = 'bash_device'," +
                " 'decoding.plugin.name' = 'pgoutput'," +
                " 'hostname' = '172.16.100.159'," +
                " 'password' = 'postgres'," +
                " 'port' = '5432'," +
                " 'schema-name' = 'public'," +
                "'table-name' = 'bash_device'," +
                "'username' = 'postgres'" +
                        ")");

        TableResult ods_bash_event_record_realtime = tableEnv.executeSql(
                " CREATE TABLE ods_bash_event_record_realtime (" +
                        "`system_type` VARCHAR(2147483647)," +
                        "`project_id` VARCHAR(2147483647)," +
                        "`project_name` VARCHAR(2147483647)," +
                        "`service_id` VARCHAR(2147483647)," +
                        "`event_id` VARCHAR(2147483647)," +
                        "`product_name` VARCHAR(2147483647)," +
                        "`device_code` VARCHAR(2147483647)," +
                        "`model_version` VARCHAR(2147483647)," +
                        "`optical_side_id` VARCHAR(2147483647)," +
                        "`image_id` VARCHAR(2147483647)," +
                        "`image_name` VARCHAR(2147483647)," +
                        "`task_id` VARCHAR(2147483647)," +
                        "`part_no` VARCHAR(2147483647)," +
                        "`image_expiration` VARCHAR(2147483647)," +
                        "`image_avg_process_time` VARCHAR(2147483647)," +
                        "`request` VARCHAR(2147483647)," +
                        "`state` VARCHAR(2147483647)," +
                        "`response` VARCHAR(2147483647)," +
                        "`start_time` VARCHAR(2147483647)," +
                        "`end_time` VARCHAR(2147483647)," +
                        "`duration` VARCHAR(2147483647)," +
                        "`shift_id` VARCHAR(2147483647)" +
                        ")" +

                        " WITH (" +
                        " 'connector' = 'kafka'," +
                        " 'format' = 'json'," +
                        "  'properties.bootstrap.servers' = '172.16.116.123:9092,172.16.116.124:9092, 172.16.116.125:9092'," +
                        "  'scan.startup.mode' = 'latest-offset'," +
                        "  'topic' = 'ods_bash_event_record_realtime'" +
                        ")");


        TableResult dwd_bash_grpc_event_record_realtime = tableEnv.executeSql(
                "CREATE TABLE dwd_bash_grpc_event_record_realtime (\n" +
  "`system_type` VARCHAR(2147483647) NOT NULL,\n" +
  "`project_id` VARCHAR(2147483647) NOT NULL,\n" +
                "`project_name` VARCHAR(2147483647),\n" +
                "`service_id` VARCHAR(2147483647) NOT NULL,\n" +
                "`event_id` VARCHAR(2147483647) NOT NULL,\n" +
                "`product_code` VARCHAR(2147483647) NOT NULL,\n" +
                "`task_code` VARCHAR(2147483647) NOT NULL,\n" +
                "`workpiece_code` VARCHAR(2147483647) NOT NULL,\n" +
                "`device_code` VARCHAR(2147483647) NOT NULL,\n" +
                "`model_version` VARCHAR(2147483647),\n" +
                "`optical_side_id` VARCHAR(2147483647) NOT NULL,\n" +
                "`image_id` VARCHAR(2147483647) NOT NULL,\n" +
                "`image_name` VARCHAR(2147483647),\n" +
                "`image_expiration` VARCHAR(2147483647),\n" +
                "`image_avg_process_time` VARCHAR(2147483647),\n" +
                "`image_width` VARCHAR(2147483647),\n" +
                "`image_height` VARCHAR(2147483647),\n" +
                "`image_size` VARCHAR(2147483647),\n" +
                "`detection_results_class_name` VARCHAR(2147483647),\n" +
                "`detection_result_x_min` VARCHAR(2147483647),\n" +
                "`detection_result_y_min` VARCHAR(2147483647),\n" +
                "`detection_result_bb_width` VARCHAR(2147483647),\n" +
                "`detection_result_bb_height` VARCHAR(2147483647),\n" +
                "`detection_area` VARCHAR(2147483647),\n" +
                "`detection_area_type` VARCHAR(2147483647),\n" +
                "`detection_comment` VARCHAR(2147483647),\n" +
                "`model_result` VARCHAR(2147483647),\n" +
                "`image_time` VARCHAR(2147483647),\n" +
                "`request_time` VARCHAR(2147483647),\n" +
                "`detection_defect_x` VARCHAR(2147483647),\n" +
                "`detection_defect_y` VARCHAR(2147483647),\n" +
                "`detection_defect_width` VARCHAR(2147483647),\n" +
                "`detection_defect_height` VARCHAR(2147483647),\n" +
                "`detection_defect_label` VARCHAR(2147483647),\n" +
                "`detection_defect_defect_feature` VARCHAR(2147483647),\n" +
                "`response_index` VARCHAR(2147483647),\n" +
                "`detection_defects_temp_x` VARCHAR(2147483647),\n" +
                "`detection_defects_temp_y` VARCHAR(2147483647),\n" +
                "`detection_defects_temp_width` VARCHAR(2147483647),\n" +
                "`detection_defects_temp_height` VARCHAR(2147483647),\n" +
                "`detection_defects_temp_label` VARCHAR(2147483647),\n" +
                "`detection_defects_temp_defect_feature` VARCHAR(2147483647),\n" +
                "`detection_defects_map_bb_height` VARCHAR(2147483647),\n" +
                "`detection_defects_map_bb_width` VARCHAR(2147483647),\n" +
                "`detection_defects_map_brightness` VARCHAR(2147483647),\n" +
                "`detection_defects_map_class_name` VARCHAR(2147483647),\n" +
                "`detection_defects_map_class_type` VARCHAR(2147483647),\n" +
                "`detection_defects_map_contrast` VARCHAR(2147483647),\n" +
                "`detection_defects_map_datetime` VARCHAR(2147483647),\n" +
                "`detection_defects_map_defect_feature` VARCHAR(2147483647),\n" +
                "`detection_defects_map_detection_value` VARCHAR(2147483647),\n" +
                "`detection_defects_map_gradients` VARCHAR(2147483647),\n" +
                "`detection_defects_map_if_model` VARCHAR(2147483647),\n" +
                "`detection_defects_map_label` VARCHAR(2147483647),\n" +
                "`detection_defects_map_length` VARCHAR(2147483647),\n" +
                "`detection_defects_map_max20brightness` VARCHAR(2147483647),\n" +
                "`detection_defects_map_min20brightness` VARCHAR(2147483647),\n" +
                "`detection_defects_map_pixel_area` VARCHAR(2147483647),\n" +
                "`detection_defects_map_points_x` VARCHAR(2147483647),\n" +
                "`detection_defects_map_points_y` VARCHAR(2147483647),\n" +
                "`detection_defects_map_score` VARCHAR(2147483647),\n" +
                "`detection_defects_map_shape_type` VARCHAR(2147483647),\n" +
                " `detection_defects_map_width` VARCHAR(2147483647),\n" +
                "`detection_defects_map_xmin` VARCHAR(2147483647),\n" +
                "`detection_defects_map_ymin` VARCHAR(2147483647),\n" +
                "`is_crop` VARCHAR(2147483647),\n" +
                "`internal_is_crop` VARCHAR(2147483647),\n" +
                "`part_no` VARCHAR(2147483647),\n" +
                "`proto_version` VARCHAR(2147483647),\n" +
                "`stream_length` VARCHAR(2147483647),\n" +
                "`est_proc_time` VARCHAR(2147483647),\n" +
                "`product_config_key` VARCHAR(2147483647),\n" +
                "`product_max_time` VARCHAR(2147483647),\n" +
                "`product_switch` VARCHAR(2147483647),\n" +
                "`seat_max_time` VARCHAR(2147483647),\n" +
                "`product_detection_type` VARCHAR(2147483647),\n" +
                "`product_handle_type` VARCHAR(2147483647),\n" +
                "`review_defect` VARCHAR(2147483647),\n" +
                "`result` VARCHAR(2147483647),\n" +
                "`start_time` VARCHAR(2147483647),\n" +
                "`end_time` VARCHAR(2147483647),\n" +
                "`shift_id` VARCHAR(2147483647) NOT NULL,\n" +
                "`shift_name` VARCHAR(2147483647),\n" +
                "`duration` VARCHAR(2147483647)\n" +
                        ")\n" +
                        "WITH (\n" +
                "'connector' = 'kafka',\n" +
                 "'format' = 'debezium-json',\n" +
                 "'debezium-json.ignore-parse-errors'='true', \n" +
                "'properties.bootstrap.servers' = '172.16.116.123:9092,172.16.116.124:9092, 172.16.116.125:9092',\n" +
                "'scan.startup.mode' = 'earliest-offset',\n" +
                "'topic' = 'dwd_bash_grpc_event_record_realtime'\n" +
        ")");

        TableResult dim_bash_work_shift = tableEnv.executeSql(
                " CREATE TABLE dim_bash_work_shift (\n" +
                        "`id` VARCHAR(2147483647) NOT NULL,\n" +
                " `shift_name` VARCHAR(2147483647),\n" +
                " `start_time` VARCHAR(2147483647),\n" +
                " `end_time` VARCHAR(2147483647),\n" +
                " `is_delete` VARCHAR(2147483647),\n" +
                " `create_time` VARCHAR(2147483647),\n" +
                " `update_time` VARCHAR(2147483647),\n" +
                " `create_user` VARCHAR(2147483647),\n" +
                " `update_user` VARCHAR(2147483647),\n" +
                " `sort_no` VARCHAR(2147483647),\n" +
                " `is_add_one` VARCHAR(2147483647)\n" +
                        ")\n" +
                "WITH (\n" +
                "   'connector' = 'kafka',\n" +
                "   'format' = 'debezium-json',\n" +
                        "'debezium-json.ignore-parse-errors'='true', \n" +
                        "  'properties.bootstrap.servers' = '172.16.116.123:9092,172.16.116.124:9092, 172.16.116.125:9092',\n" +
                " 'scan.startup.mode' = 'latest-offset',\n" +
                "  'topic' = 'dim_bash_work_shift'\n" +
                        ")");
        TableResult ck_dwd_bash_grpc_event_record_realtime  = tableEnv.executeSql(
                " CREATE TABLE ck_dwd_bash_grpc_event_record_realtime (\n" +
                "`system_type` VARCHAR(2147483647) NOT NULL,\n" +
                        "`project_id` VARCHAR(2147483647) NOT NULL,\n" +
                        "`project_name` VARCHAR(2147483647),\n" +
                        "`service_id` VARCHAR(2147483647) NOT NULL,\n" +
                        "`event_id` VARCHAR(2147483647) NOT NULL,\n" +
                        "`product_code` VARCHAR(2147483647) NOT NULL,\n" +
                        "`task_code` VARCHAR(2147483647) NOT NULL,\n" +
                        "`workpiece_code` VARCHAR(2147483647) NOT NULL,\n" +
                        "`device_code` VARCHAR(2147483647) NOT NULL,\n" +
                        "`model_version` VARCHAR(2147483647),\n" +
                        "`optical_side_id` VARCHAR(2147483647) NOT NULL,\n" +
                        "`image_id` VARCHAR(2147483647) NOT NULL,\n" +
                        "`image_name` VARCHAR(2147483647),\n" +
                        "`image_expiration` VARCHAR(2147483647),\n" +
                        "`image_avg_process_time` VARCHAR(2147483647),\n" +
                        "`image_width` VARCHAR(2147483647),\n" +
                        "`image_height` VARCHAR(2147483647),\n" +
                        "`image_size` VARCHAR(2147483647),\n" +
                        "`detection_results_class_name` VARCHAR(2147483647),\n" +
                        "`detection_result_x_min` VARCHAR(2147483647),\n" +
                        "`detection_result_y_min` VARCHAR(2147483647),\n" +
                        "`detection_result_bb_width` VARCHAR(2147483647),\n" +
                        "`detection_result_bb_height` VARCHAR(2147483647),\n" +
                        "`detection_area` VARCHAR(2147483647),\n" +
                        "`detection_area_type` VARCHAR(2147483647),\n" +
                        "`detection_comment` VARCHAR(2147483647),\n" +
                        "`model_result` VARCHAR(2147483647),\n" +
                        "`image_time` VARCHAR(2147483647),\n" +
                        "`request_time` VARCHAR(2147483647),\n" +
                        "`detection_defect_x` VARCHAR(2147483647),\n" +
                        "`detection_defect_y` VARCHAR(2147483647),\n" +
                        "`detection_defect_width` VARCHAR(2147483647),\n" +
                        "`detection_defect_height` VARCHAR(2147483647),\n" +
                        "`detection_defect_label` VARCHAR(2147483647),\n" +
                        "`detection_defect_defect_feature` VARCHAR(2147483647),\n" +
                        "`response_index` VARCHAR(2147483647),\n" +
                        "`detection_defects_temp_x` VARCHAR(2147483647),\n" +
                        "`detection_defects_temp_y` VARCHAR(2147483647),\n" +
                        "`detection_defects_temp_width` VARCHAR(2147483647),\n" +
                        "`detection_defects_temp_height` VARCHAR(2147483647),\n" +
                        "`detection_defects_temp_label` VARCHAR(2147483647),\n" +
                        "`detection_defects_temp_defect_feature` VARCHAR(2147483647),\n" +
                        "`detection_defects_map_bb_height` VARCHAR(2147483647),\n" +
                        "`detection_defects_map_bb_width` VARCHAR(2147483647),\n" +
                        "`detection_defects_map_brightness` VARCHAR(2147483647),\n" +
                        "`detection_defects_map_class_name` VARCHAR(2147483647),\n" +
                        "`detection_defects_map_class_type` VARCHAR(2147483647),\n" +
                        "`detection_defects_map_contrast` VARCHAR(2147483647),\n" +
                        "`detection_defects_map_datetime` VARCHAR(2147483647),\n" +
                        "`detection_defects_map_defect_feature` VARCHAR(2147483647),\n" +
                        "`detection_defects_map_detection_value` VARCHAR(2147483647),\n" +
                        "`detection_defects_map_gradients` VARCHAR(2147483647),\n" +
                        "`detection_defects_map_if_model` VARCHAR(2147483647),\n" +
                        "`detection_defects_map_label` VARCHAR(2147483647),\n" +
                        "`detection_defects_map_length` VARCHAR(2147483647),\n" +
                        "`detection_defects_map_max20brightness` VARCHAR(2147483647),\n" +
                        "`detection_defects_map_min20brightness` VARCHAR(2147483647),\n" +
                        "`detection_defects_map_pixel_area` VARCHAR(2147483647),\n" +
                        "`detection_defects_map_points_x` VARCHAR(2147483647),\n" +
                        "`detection_defects_map_points_y` VARCHAR(2147483647),\n" +
                        "`detection_defects_map_score` VARCHAR(2147483647),\n" +
                        "`detection_defects_map_shape_type` VARCHAR(2147483647),\n" +
                        " `detection_defects_map_width` VARCHAR(2147483647),\n" +
                        "`detection_defects_map_xmin` VARCHAR(2147483647),\n" +
                        "`detection_defects_map_ymin` VARCHAR(2147483647),\n" +
                        "`is_crop` VARCHAR(2147483647),\n" +
                        "`internal_is_crop` VARCHAR(2147483647),\n" +
                        "`part_no` VARCHAR(2147483647),\n" +
                        "`proto_version` VARCHAR(2147483647),\n" +
                        "`stream_length` VARCHAR(2147483647),\n" +
                        "`est_proc_time` VARCHAR(2147483647),\n" +
                        "`product_config_key` VARCHAR(2147483647),\n" +
                        "`product_max_time` VARCHAR(2147483647),\n" +
                        "`product_switch` VARCHAR(2147483647),\n" +
                        "`seat_max_time` VARCHAR(2147483647),\n" +
                        "`product_detection_type` VARCHAR(2147483647),\n" +
                        "`product_handle_type` VARCHAR(2147483647),\n" +
                        "`review_defect` VARCHAR(2147483647),\n" +
                        "`result` VARCHAR(2147483647),\n" +
                        "`start_time` VARCHAR(2147483647),\n" +
                        "`end_time` VARCHAR(2147483647),\n" +
                        "`shift_id` VARCHAR(2147483647) NOT NULL,\n" +
                        "`shift_name` VARCHAR(2147483647),\n" +
                        "`duration` VARCHAR(2147483647),\n" +
                        "   PRIMARY KEY (`system_type`, `project_id`, `service_id`, `event_id`, `product_code`, `task_code`, `device_code`, `model_version`, `optical_side_id`, `image_id`, `shift_id`, `end_time`, `start_time`) NOT ENFORCED\n" +
                ")\n" +

        "  WITH (\n" +
                "'connector' = 'clickhouse',\n" +
                "  'database-name' = 'bash_data_warehouse',\n" +
                "  'password' = 'Clickhouse158',\n" +
                " 'table-name' = 'dwd_bash_grpc_event_record_realtime',\n" +
                " 'url' = 'clickhouse://172.16.100.158:9123',\n" +
                " 'username' = 'default'\n" +
                        ")");



//               tableEnv.executeSql("create table cm(\n" +
//                "`cm` STRING,\n" +
//                 "`account` STRING,\n" +
//                "`password` STRING,\n" +
//                 "`mobile` STRING,\n" +
//                "`email` STRING\n" +
//                ")WITH(\n" +
//                "'connector' = 'jdbc',\n" +
//                "'url' = 'jdbc:hive2://172.16.116.124:10000/test',\n" +
//                " 'username' = 'hive',\n"+
//                " 'password' = 'hive',\n"+
//                "'table-name' = 'cm'\n" +
//               ")"
//        );
//



        //   tableEnv.executeSql("select word,cast(state as STRING) as state,cast(state.skilllist[1] as STRING) as skilllist,cast(state.skilllist[2].skill as STRING)  as userid from test,lateral table(splitfunction(process))").print();
       // tableEnv.executeSql("select * from test;").print();

//tableEnv.executeSql("SELECT t1.window_start   from(\n" +
//       "SELECT * FROM TABLE(TUMBLE(TABLE test, DESCRIPTOR(rt), INTERVAL '10' SECOND))\n" +
//       ") as t1\n" +
//        "left join (\n" +
//       "   SELECT * FROM TABLE(TUMBLE(TABLE test2, DESCRIPTOR(rt), INTERVAL '10' SECOND))\n" +
//       ") as t2\n" +
//        "ON t1.process = t2.process\n" +
//       "AND t1.window_start = t2.window_start \n" +
//       "AND t1.window_end = t2.window_end;").print();
////
//           tableEnv.executeSql("\n" +
//                   "select * from test as t1 join test2 as t2 on t1.process=t2.process\n" +
//                   "and t1.rt BETWEEN t2.rt -INTERVAL '10' SECOND and t2.rt;").print();
//
//

//        TableResult test3=  tableEnv.executeSql("CREATE CATALOG hive_catalog WITH (\n" +
//                "'type' = 'hive',\n" +
//                "'default-database' = 'test',\n" +
//                "'hive-conf-dir' = 'src\\main\\resources'\n" +
//        ")"
//);
//


        TableResult pg_dim_bash_work_shift = tableEnv.executeSql(
                " CREATE TABLE pg_dim_bash_work_shift (\n" +
                " `id` BIGINT NOT NULL,\n" +
                " `shift_name` VARCHAR(2147483647),\n" +
                " `start_time` TIME(0),\n" +
                " `end_time` TIME(0),\n" +
                " `is_delete` BOOLEAN,\n" +
                " `create_time` TIMESTAMP(6),\n" +
                " `update_time` TIMESTAMP(6),\n" +
                " `create_user` BIGINT,\n" +
                " `update_user` BIGINT,\n" +
                " `sort_no` SMALLINT,\n" +
                " `is_add_one` SMALLINT,\n" +
                " PRIMARY KEY (`id`) NOT ENFORCED\n" +
")\n" +
        "  WITH (\n" +
        "  'connector' = 'postgres-cdc',\n" +
        "  'database-name' = 'svf_bash',\n" +
        "   'debezium.slot.name' = 'bash_shift',\n" +
        "    'decoding.plugin.name' = 'pgoutput',\n" +
        "    'hostname' = '172.16.100.159',\n" +
        "    'password' = 'postgres',\n" +
        "     'port' = '5432',\n" +
                        "'schema-name' = 'public',\n" +
        "     'table-name' = 'bash_shift',\n" +
        "    'username' = 'postgres'\n" +
                        ")"
        );
//        tableEnv.executeSql( "CREATE TABLE ods_bash_check_log_realtime ("
//                        +" `id` BIGINT,\n "
//                        +" `device_no` STRING,\n"
//                        +" `optical_side_id` STRING,\n"
//                        +" `uuid` STRING,\n"
//                        +" `image_name` STRING,\n"
//                        +" `result` STRING,\n"
//                        +" `err_msg` STRING,\n"
//                        +" `log_time` STRING,\n"
//                        +" `project_id` STRING,\n"
//                        +" `create_time` TIMESTAMP(3),\n"
//                        +" `is_delete` BOOLEAN,\n"
//                        +" PRIMARY KEY (`id`) NOT ENFORCED \n"
//                        +     ") "
//                        + "WITH ("
//                        +   "'connector' = 'jdbc',"
//                        +   "'password' = 'postgres',"
//                        +   "'table-name' = 'public.bash_check_log',"
//                        +   "'url' = 'jdbc:postgresql://172.16.100.159:5432/svf_bash',"
//                        +   "'username' = 'postgres'"
//                        +   ")"
//        );
//

//        tableEnv.executeSql("\n" +
//                "insert into dwd_bash_grpc_event_record_realtime\n" +
//                "select distinct\n" +
//                "COALESCE(cast(system_type as string),' '),\n" +
//                "COALESCE(cast(project_id as string),' '),\n" +
//                "COALESCE(cast(project_name  as string),' '),\n" +
//                "COALESCE(cast(service_id  as string),' '),\n" +
//                "COALESCE(cast(event_id as string),' '),\n" +
//                "COALESCE(cast(product_name as string),' ') ,\n" +
//                "COALESCE(cast(task_id as string),' ') as task_code,\n" +
//                "COALESCE(cast(part_no as string),' ') as workpiece_code,\n" +
//                "COALESCE(cast(device_code as string),' ') ,\n" +
//                "COALESCE(cast(model_version  as string),' '),\n" +
//                "COALESCE(cast(optical_side_id as string),' '),\n" +
//                "COALESCE(cast(image_id as string),' '),\n" +
//                "COALESCE(cast(image_name as string),' '),\n" +
//                "COALESCE(cast(image_expiration as string),' '),\n" +
//                "COALESCE(cast(image_avg_process_time as string),' '),\n" +
//                "COALESCE(cast(imageWidth as string),' ') as image_width ,\n" +
//                "COALESCE(cast(imageHeight as string),' ') as image_height ,\n" +
//                "COALESCE(cast(imageSize  as string),' ')as image_size ,\n" +
//                "COALESCE(cast(detectionResults_class_name  as string),' '),\n" +
//                "COALESCE(cast(detectionResults_xmin  as string),' '),\n" +
//                "COALESCE(cast(detectionResults_ymin  as string),' '),\n" +
//                "COALESCE(cast(detectionResults_bb_width as string),' ') ,\n" +
//                "COALESCE(cast(detectionResults_bb_height  as string),' '),\n" +
//                "COALESCE(cast(detectionArea  as string),' '),\n" +
//                "COALESCE(cast(detectionAreaType  as string),' '),\n" +
//                "COALESCE(cast(detectionComment  as string),' '),\n" +
//                "COALESCE(cast(modelResult as string),' ') ,\n" +
//                "COALESCE(cast(imageTime  as string),' '),\n" +
//                "COALESCE(cast(requestTime  as string),' '),\n" +
//                "COALESCE(cast(detectionDefect_x  as string),' '),\n" +
//                "COALESCE(cast(detectionDefect_y  as string),' '),\n" +
//                "COALESCE(cast(detectionDefect_width  as string),' '),\n" +
//                "COALESCE(cast(detectionDefect_height  as string),' '),\n" +
//                "COALESCE(cast(detectionDefect_label  as string),' '),\n" +
//                "COALESCE(cast(detectionDefect_defectFeature  as string),' '),\n" +
//                "COALESCE(cast(index  as string),' ')as response_index ,\n" +
//                "COALESCE(cast(detectionDefectsTemp_x  as string),' '),\n" +
//                "COALESCE(cast(detectionDefectsTemp_y  as string),' '),\n" +
//                "COALESCE(cast(detectionDefectsTemp_width as string),' ') ,\n" +
//                "COALESCE(cast(detectionDefectsTemp_height as string),' ') ,\n" +
//                "COALESCE(cast(detectionDefectsTemp_label  as string),' '),\n" +
//                "COALESCE(cast(detectionDefectsTemp_defectFeature  as string),' '),\n" +
//                "COALESCE(cast(detectionDefectsMap_bb_height  as string),' '),\n" +
//                "COALESCE(cast(detectionDefectsMap_bb_width  as string),' '),\n" +
//                "COALESCE(cast(detectionDefectsMap_brightness  as string),' '),\n" +
//                "COALESCE(cast(detectionDefectsMap_class_name  as string),' '),\n" +
//                "COALESCE(cast(detectionDefectsMap_class_type  as string),' '),\n" +
//                "COALESCE(cast(detectionDefectsMap_contrast  as string),' '),\n" +
//                "COALESCE(cast(detectionDefectsMap_datetime  as string),' '),\n" +
//                "COALESCE(cast(detectionDefectsMap_defect_feature  as string),' '),\n" +
//                "COALESCE(cast(detectionDefectsMap_detection_value as string),' ') ,\n" +
//                "COALESCE(cast(detectionDefectsMap_gradients  as string),' '),\n" +
//                "COALESCE(cast(detectionDefectsMap_if_model  as string),' '),\n" +
//                "COALESCE(cast(detectionDefectsMap_label as string),' ') ,\n" +
//                "COALESCE(cast(detectionDefectsMap_length  as string),' '),\n" +
//                "COALESCE(cast(detectionDefectsMap_max20brightness  as string),' '),\n" +
//                "COALESCE(cast(detectionDefectsMap_min20brightness as string) ,' '),\n" +
//                "COALESCE(cast(detectionDefectsMap_pixel_area as string),' ') ,\n" +
//                "COALESCE(cast(detectionDefectsMap_points_x as string),' ') ,\n" +
//                "COALESCE(cast(detectionDefectsMap_points_y  as string),' '),\n" +
//                "COALESCE(cast(detectionDefectsMap_score  as string),' '),\n" +
//                "COALESCE(cast(detectionDefectsMap_shape_type  as string),' '),\n" +
//                "COALESCE(cast(detectionDefectsMap_width as string),' ') ,\n" +
//                "COALESCE(cast(detectionDefectsMap_xmin  as string),' '),\n" +
//                "COALESCE(cast(detectionDefectsMap_ymin  as string),' '),\n" +
//                "COALESCE(cast(response_isCrop  as string),' ')as is_crop ,\n" +
//                "COALESCE(cast(response_internalIsCrop as string),' ') as  internal_is_crop ,\n" +
//                "COALESCE(cast(response_partNo as string),' ') as part_no ,\n" +
//                "COALESCE(cast(response_protoVersion as string),' ') as proto_version ,\n" +
//                "COALESCE(cast(response_streamLength as string),' ') as stream_length ,\n" +
//                "COALESCE(cast(response_estProcTime  as string),' ')as est_proc_time ,\n" +
//                "COALESCE(cast(response_productConfigKey as string),' ') as product_config_key ,\n" +
//                "COALESCE(cast(response_productMaxTime as string),' ') as product_max_time ,\n" +
//                "COALESCE(cast(response_productSwitch as string),' ') as product_switch ,\n" +
//                "COALESCE(cast(response_seatMaxTime  as string),' ')as seat_max_time ,\n" +
//                "COALESCE(cast(response_productDetectionType as string) ,' ')as product_detection_type ,\n" +
//                "COALESCE(cast(response_productHandleType as string) ,' ')as product_handle_type ,\n" +
//                "COALESCE(cast(response_reviewDefect  as string),' ') as review_defect ,\n" +
//                "COALESCE(cast(response_result as string) ,' '),\n" +
//                "COALESCE(cast(start_time as string),' '),\n" +
//                "COALESCE(cast(end_time as string),' '),\n" +
//                "COALESCE(cast(shift_id as string),' '),\n" +
//                "COALESCE(cast(shift_name as string),' '),\n" +
//                "COALESCE(cast(duration as string),' ')\n" +
//                "from (\n" +
//                "select\n" +
//                "  t1.`system_type` ,\n" +
//                "  t1.`project_id` ,\n" +
//                "  t1.`project_name` ,\n" +
//                "  t1.`service_id` ,\n" +
//                "  t1.`event_id` ,\n" +
//                "  t1.`product_name` ,\n" +
//                "  t1.`device_code` ,\n" +
//                "  t1.`model_version` ,\n" +
//                "  t1.`optical_side_id` ,\n" +
//                "  t1.`image_id` ,\n" +
//                "  t1.`image_name` ,\n" +
//                "  t1.`task_id` ,\n" +
//                "  t1.`part_no` ,\n" +
//                "  t1.`image_expiration` ,\n" +
//                "  t1.`image_avg_process_time` ,\n" +
//                "  t1.`request` ,\n" +
//                "  t1.`state` ,\n" +
//                "  t1.`response` ,\n" +
//                "  t1.`start_time`,\n" +
//                "  t1.`end_time`,\n" +
//                "  t1.`duration` ,\n" +
//                "  t1.`shift_id`,\n" +
//                "  t2.shift_name\n" +
//                " from\n" +
//                "ods_bash_event_record_realtime as t1\n" +
//                "left join dim_bash_work_shift as t2\n" +
//                "on t1.shift_id=t2.id\n" +
//                ")\n" +
//                ",lateral table(RequestJsonToRow(request))\n" +
//                ",lateral table(response_json_to_row(response))\n" +
//                ";").print();
        System.out.println("查询完成");
        tableEnv.executeSql("\n" +
"insert into ck_dwd_bash_grpc_event_record_realtime select * from dwd_bash_grpc_event_record_realtime;").print();

//
//
//    }
//        tableEnv.executeSql("\n" +
//                "SELECT distinct t1.process,class_name \n" +
//                "  from(\n" +
//                "SELECT \n" +
//                "process,class_name \n" +
//                "FROM \n" +
//                "TABLE(TUMBLE(TABLE test, DESCRIPTOR(rt), INTERVAL '10' SECOND)),lateral table(json_array_to_row_function(request.detectionResults))\n" +
//                ") as t1\n" +
//                "left join (\n" +
//                "SELECT \n" +
//                "* \n" +
//                "FROM \n" +
//                "test2\n" +
//                ") as t2\n" +
//                "ON t1.process = t2.process\n" +
//                ";").print();


    }


    //创建自定义表函数  创建完需要注册
    @FunctionHint(output = @DataTypeHint("Row<word STRING,length INT>"))
    public static class splitfunction extends TableFunction {

        public void eval(String str) {
            //处理完需要用collect方法输出
            for (String world : str.split(" ")) {
                // 使用 org.apache.flink.types.Row.of 方法创建 Row 对象
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



    // 定义一个中间结果类，用于存储累加的和
    public static class SumAccumulator {
        public Integer sum = 0; // 初始值为0

    }

    // 定义一个自定义聚合函数类，继承 AggregateFunction<Long, SumAccumulator>
    public static class MySum extends AggregateFunction<Integer, SumAccumulator> {

        // 从中间结果对象中获取最终的结果值
        @Override
        public Integer getValue(SumAccumulator acc) {
            return acc.sum; // 返回累加的和
        }

        // 创建一个初始的中间结果对象
        @Override
        public SumAccumulator createAccumulator() {
            return new SumAccumulator(); // 返回一个新的 SumAccumulator 对象
        }
        // 根据输入的数据更新中间结果对象
        public void accumulate(SumAccumulator acc, Integer value) {
            acc.sum += value; // 累加输入的值到 sum 字段上
        }
    }


    //创建自定义表函数  创建完需要注册
    @FunctionHint(output = @DataTypeHint("Row<class_name STRING,xmin DOUBLE,ymin DOUBLE,bb_width DOUBLE,bb_height DOUBLE,score INT,defect_feature STRING>"))
    public static class json_array_to_row_function extends TableFunction {

        public void eval(String json2) {

            JSONArray jsonarray = JSON.parseArray(json2);


            for (int i = 0; i < jsonarray.size(); i++) {
                JSONObject jsonObject = jsonarray.getJSONObject(i);
                String class_name = jsonObject.getString("class_name");
                Double xmin = jsonObject.getDouble("xmin");
                Double ymin = jsonObject.getDouble("ymin");
                Double bb_width = jsonObject.getDouble("bb_width");
                Double bb_height = jsonObject.getDouble("bb_height");
                Integer score = jsonObject.getInteger("score");
                String defect_feature = jsonObject.getString("defect_feature");
                collect(Row.of(class_name, xmin, ymin, bb_width, bb_height, score, defect_feature));
            }

        }
    }


    @FunctionHint(output = @DataTypeHint("" +
            "Row<detectionDefect_x Int," +
            "detectionDefect_y Int," +
            "detectionDefect_width Int," +
            "detectionDefect_height Int," +
            "detectionDefect_label String," +
            "detectionDefect_defectFeature String," +
            "index Int," +
            "response_points_x Int," +
            "response_points_y Int," +
            "response_systemType Int," +
            "response_uuid String," +
            "response_productName String," +
            "response_modelVersion String," +
            "response_imageName String," +
            "response_imageWidth Int," +
            "response_imageHeight Int," +
            "response_isCrop Int," +
            "response_internalIsCrop Int," +
            "response_detectionAreaType Int," +
            "response_taskId String," +
            "response_partNo String," +
            "response_protoVersion String," +
            "response_streamLength Int," +
            "response_estProcTime Int," +
            "response_system_id String," +
            "response_project_id String," +
            "response_project_name String," +
            "response_device_no String," +
            "response_productConfigKey String," +
            "response_productMaxTime Int," +
            "response_productSwitch Int," +
            "response_seatMaxTime Int," +
            "response_productDetectionType Int," +
            "response_productHandleType Int," +
            "response_reviewDefect String," +
            "response_result String," +
            "response_bashSampleType String," +
            "response_modelSampleType String," +
            "response_sortingSampleType String," +
            "response_sortingSampleTypeDetail String," +
            "response_merge_type String," +
            "detectionDefectsTemp_x Int," +
            "detectionDefectsTemp_y Int," +
            "detectionDefectsTemp_width Int," +
            "detectionDefectsTemp_height Int," +
            "detectionDefectsTemp_label String," +
            "detectionDefectsTemp_defectFeature String," +
            "detectionDefectsMap_bb_height Int," +
            "detectionDefectsMap_bb_width Int," +
            "detectionDefectsMap_brightness Int," +
            "detectionDefectsMap_class_name String," +
            "detectionDefectsMap_class_type String," +
            "detectionDefectsMap_contrast Int," +
            "detectionDefectsMap_datetime String," +
            "detectionDefectsMap_defect_feature String," +
            "detectionDefectsMap_detection_value Int," +
            "detectionDefectsMap_gradients Int," +
            "detectionDefectsMap_if_model Boolean," +
            "detectionDefectsMap_label String," +
            "detectionDefectsMap_length Int," +
            "detectionDefectsMap_max20brightness Int," +
            "detectionDefectsMap_min20brightness Int," +
            "detectionDefectsMap_pixel_area Int," +
            "detectionDefectsMap_score Int," +
            "detectionDefectsMap_shape_type String," +
            "detectionDefectsMap_width Int," +
            "detectionDefectsMap_xmin Int," +
            "detectionDefectsMap_ymin Int," +
            "shapes_label String," +
            "shapes_disputeResult String," +
            "shapes_disputeProcessTime String," +
            "shapes_score Double," +
            "shapes_length Double," +
            "shapes_pixel_area Double," +
            "shapes_if_model Boolean," +
            "shapes_model_info_model_name String," +
            "shapes_model_info_model_id String," +
            "shapes_model_info_model_date String," +
            "shapes_model_info_model_author String," +
            "shapes_labeller_info_name String," +
            "shapes_labeller_info_date String," +
            "shapes_shape_type String," +
            "shapes_labelType Int, " +
            "shapes_width Int," +
            "shapes_group_id String," +
            "shapes_radius String," +
            "shapes_tag String," +
            "detectionDefectsMap_points_x  Int," +
            "detectionDefectsMap_points_y  Int," +
            "shapes_points_x  Int," +
            "shapes_points_y  Int" +
            ">"))
    public static class response_json_to_row extends TableFunction {
        public void eval(String json2) throws JsonProcessingException {

            ObjectMapper mapper = new ObjectMapper();

            JsonNode node = mapper.readTree(json2);
            //数量少了可能会报错
            if (json2 == null) {

                collect(Row.of(null, null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null

                ));
            }


            try {
                if (node.isArray()) {

                    JSONArray jsonarray = JSON.parseArray(json2);

                    for (int i = 0; i < jsonarray.size(); i++) {
                        JSONObject jsonObject = jsonarray.getJSONObject(i);
                        JSONObject detectionDefect = (JSONObject) jsonObject.getOrDefault("detectionDefect", null);
                        Integer response_detectionDefect_x = (Integer) detectionDefect.getOrDefault("x", null);
                        Integer response_detectionDefect_y = (Integer) detectionDefect.getOrDefault("y", null);
                        Integer response_detectionDefect_width = (Integer) detectionDefect.getOrDefault("width", null);
                        Integer response_detectionDefect_height = (Integer) detectionDefect.getOrDefault("height", null);
                        String response_detectionDefect_label = (String) detectionDefect.getOrDefault("label", null);
                        String response_detectionDefect_defectFeature = (String) detectionDefect.getOrDefault("defectFeature", null);
                        Integer response_index = (Integer) jsonObject.getOrDefault("index", null);


                        JSONArray points = (JSONArray) jsonObject.getOrDefault("points", new JSONArray().fluentAdd(new JSONObject()));


                        for (int p = 0; p < Math.max(points.size(), jsonarray.size()); p++) {
                            JSONObject pointsObject = i < points.size() ? (JSONObject) points.get(i) : new JSONObject();
                            Integer response_points_x = (Integer) pointsObject.getOrDefault("x", null);
                            Integer response_points_y = (Integer) pointsObject.getOrDefault("y", null);
                            collect(Row.of(response_detectionDefect_x,
                                    response_detectionDefect_y,
                                    response_detectionDefect_width,
                                    response_detectionDefect_height,
                                    response_detectionDefect_label,
                                    response_detectionDefect_defectFeature,
                                    response_index,
                                    response_points_x,
                                    response_points_y,
                                    null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null

                            ));


                        }

                    }
                }

            } catch (ArrayIndexOutOfBoundsException e) {
                System.out.println("数组越界异常：" + e.getMessage());
            }

            if (node.isObject()) {
                ;
                JSONObject responseobj = JSON.parseObject(json2);

                Integer response_systemType = (Integer) responseobj.getOrDefault("systemType", null);
                String response_uuid = (String) responseobj.getOrDefault("uuid", null);
                String response_productName = (String) responseobj.getOrDefault("productName", null);
                String response_modelVersion = (String) responseobj.getOrDefault("modelVersion", null);
                String response_imageName = (String) responseobj.getOrDefault("imageName", null);
                Integer response_imageWidth = (Integer) responseobj.getOrDefault("imageWidth", null);
                Integer response_imageHeight = (Integer) responseobj.getOrDefault("imageHeight", null);
                JSONArray detectionDefects = (JSONArray) responseobj.getOrDefault("detectionDefects", new JSONArray().fluentAdd(new JSONObject()));


                JSONArray detectionDefectsMap = (JSONArray) responseobj.getOrDefault("detectionDefectsMap", new JSONArray().fluentAdd(new JSONObject()));

                JSONArray detectionDefectsTemp = (JSONArray) responseobj.getOrDefault("detectionDefectsTemp", new JSONArray().fluentAdd(new JSONObject()));
                Integer response_isCrop = (Integer) responseobj.getOrDefault("isCrop", null);

                Integer response_internalIsCrop = (Integer) responseobj.getOrDefault("internalIsCrop", null);

                Integer response_detectionAreaType = (Integer) responseobj.getOrDefault("detectionAreaType", null);

                String response_taskId = (String) responseobj.getOrDefault("taskId", null);

                String response_partNo = (String) responseobj.getOrDefault("partNo", null);

                String response_protoVersion = (String) responseobj.getOrDefault("protoVersion", null);

                Integer response_streamLength = (Integer) responseobj.getOrDefault("streamLength", null);

                Integer response_estProcTime = (Integer) responseobj.getOrDefault("estProcTime", null);

                String response_system_id = (String) responseobj.getOrDefault("system_id", null);

                String response_project_id = (String) responseobj.getOrDefault("project_id", null);

                String response_project_name = (String) responseobj.getOrDefault("project_name", null);

                String response_device_no = (String) responseobj.getOrDefault("device_no", null);

                String response_productConfigKey = (String) responseobj.getOrDefault("productConfigKey", null);

                Integer response_productMaxTime = (Integer) responseobj.getOrDefault("productMaxTime", null);

                Integer response_productSwitch = (Integer) responseobj.getOrDefault("productSwitch", null);

                Integer response_seatMaxTime = (Integer) responseobj.getOrDefault("seatMaxTime", null);

                Integer response_productDetectionType = (Integer) responseobj.getOrDefault("productDetectionType", null);

                Integer response_productHandleType = (Integer) responseobj.getOrDefault("productHandleType", null);

                String response_reviewDefect = (String) responseobj.getOrDefault("reviewDefect", null);

                String response_result = (String) responseobj.getOrDefault("result", null);

                String response_bashSampleType = (String) responseobj.getOrDefault("bashSampleType", null);

                String response_modelSampleType = (String) responseobj.getOrDefault("modelSampleType", null);

                String response_sortingSampleType = (String) responseobj.getOrDefault("sortingSampleType", null);

                String response_sortingSampleTypeDetail = (String) responseobj.getOrDefault("sortingSampleTypeDetail", null);

                String response_merge_type = (String) responseobj.getOrDefault("merge_type", null);


                JSONArray shapes = (JSONArray) responseobj.getOrDefault("shapes", new JSONArray().fluentAdd(new JSONObject()));
                for (int i = 0; i < Math.max(Math.max(Math.max(detectionDefects.size(), detectionDefectsMap.size()), shapes.size()), detectionDefectsTemp.size()); i++) {

                    JSONObject detectionDefectobj = i < detectionDefects.size() ? (JSONObject) detectionDefects.get(i) : new JSONObject();

                    JSONObject detectionDefectsMapobj = i < detectionDefectsMap.size() ? (JSONObject) detectionDefectsMap.get(i) : new JSONObject();

                    JSONObject detectionDefectsTempobj = i < detectionDefectsTemp.size() ? (JSONObject) detectionDefectsTemp.get(i) : new JSONObject();


                    JSONObject shapesobj = i < shapes.size() ? (JSONObject) shapes.get(i) : new JSONObject();

                    Integer detectionDefect_x = (Integer) detectionDefectobj.getOrDefault("x", null);

                    Integer detectionDefect_y = (Integer) detectionDefectobj.getOrDefault("y", null);

                    Integer detectionDefect_width = (Integer) detectionDefectobj.getOrDefault("width", null);

                    Integer detectionDefect_height = (Integer) detectionDefectobj.getOrDefault("height", null);

                    String detectionDefect_label = (String) detectionDefectobj.getOrDefault("label", null);

                    String detectionDefect_defectFeature = (String) detectionDefectobj.getOrDefault("defectFeature", null);

                    Integer detectionDefectsTemp_x = (Integer) detectionDefectsTempobj.getOrDefault("x", null);

                    Integer detectionDefectsTemp_y = (Integer) detectionDefectsTempobj.getOrDefault("y", null);

                    Integer detectionDefectsTemp_width = (Integer) detectionDefectsTempobj.getOrDefault("width", null);

                    Integer detectionDefectsTemp_height = (Integer) detectionDefectsTempobj.getOrDefault("height", null);

                    String detectionDefectsTemp_label = (String) detectionDefectsTempobj.getOrDefault("label", null);

                    String detectionDefectsTemp_defectFeature = (String) detectionDefectsTempobj.getOrDefault("defectFeature", null);

                    Integer detectionDefectsMap_bb_height = (Integer) detectionDefectsMapobj.getOrDefault("bb_height", null);

                    Integer detectionDefectsMap_bb_width = (Integer) detectionDefectsMapobj.getOrDefault("bb_width", null);

                    Integer detectionDefectsMap_brightness = (Integer) detectionDefectsMapobj.getOrDefault("brightness", null);

                    String detectionDefectsMap_class_name = (String) detectionDefectsMapobj.getOrDefault("class_name", null);

                    String detectionDefectsMap_class_type = (String) detectionDefectsMapobj.getOrDefault("class_type", null);

                    Integer detectionDefectsMap_contrast = (Integer) detectionDefectsMapobj.getOrDefault("contrast", null);

                    String detectionDefectsMap_datetime = (String) detectionDefectsMapobj.getOrDefault("datetime", null);

                    String detectionDefectsMap_defect_feature = (String) detectionDefectsMapobj.getOrDefault("defect_feature", null);

                    Integer detectionDefectsMap_detection_value = (Integer) detectionDefectsMapobj.getOrDefault("detection_value", null);

                    Integer detectionDefectsMap_gradients = (Integer) detectionDefectsMapobj.getOrDefault("gradients", null);

                    Boolean detectionDefectsMap_if_model = (Boolean) detectionDefectsMapobj.getOrDefault("if_model", null);

                    String detectionDefectsMap_label = (String) detectionDefectsMapobj.getOrDefault("label", null);

                    Integer detectionDefectsMap_length = (Integer) detectionDefectsMapobj.getOrDefault("length", null);

                    Integer detectionDefectsMap_max20brightness = (Integer) detectionDefectsMapobj.getOrDefault("max20brightness", null);

                    Integer detectionDefectsMap_min20brightness = (Integer) detectionDefectsMapobj.getOrDefault("min20brightness", null);

                    Integer detectionDefectsMap_pixel_area = (Integer) detectionDefectsMapobj.getOrDefault("pixel_area", null);

                    Integer detectionDefectsMap_score = (Integer) detectionDefectsMapobj.getOrDefault("score", null);

                    String detectionDefectsMap_shape_type = (String) detectionDefectsMapobj.getOrDefault("shape_type", null);

                    Integer detectionDefectsMap_width = (Integer) detectionDefectsMapobj.getOrDefault("width", null);

                    Integer detectionDefectsMap_xmin = (Integer) detectionDefectsMapobj.getOrDefault("xmin", null);

                    Integer detectionDefectsMap_ymin = (Integer) detectionDefectsMapobj.getOrDefault("ymin", null);


                    String shapes_label = (String) shapesobj.getOrDefault("label", null);

                    String shapes_disputeResult = (String) shapesobj.getOrDefault("disputeResult", null);

                    String shapes_disputeProcessTime = (String) shapesobj.getOrDefault("disputeProcessTime", null);

                    Double shapes_score = (Double) shapesobj.getOrDefault("score", null);

                    Double shapes_length = (Double) shapesobj.getOrDefault("length", null);

                    Double shapes_pixel_area = (Double) shapesobj.getOrDefault("pixel_area", null);

                    Boolean shapes_if_model = (Boolean) shapesobj.getOrDefault("if_model", null);

                    //对象
                    JSONObject model_info = (JSONObject) shapesobj.getOrDefault("model_info", new JSONObject());

                    String shapes_model_info_model_name = (String) model_info.getOrDefault("model_name", null);

                    String shapes_model_info_model_id = (String) model_info.getOrDefault("model_id", null);

                    String shapes_model_info_model_date = (String) model_info.getOrDefault("model_date", null);

                    String shapes_model_info_model_author = (String) model_info.getOrDefault("model_author", null);
                    //对象
                    JSONObject labeller_info = (JSONObject) shapesobj.getOrDefault("labeller_info", new JSONObject());

                    String shapes_labeller_info_name = (String) labeller_info.getOrDefault("name", null);

                    String shapes_labeller_info_date = (String) labeller_info.getOrDefault("date", null);

                    String shapes_shape_type = (String) shapesobj.getOrDefault("shape_type", null);

                    Integer shapes_labelType = (Integer) shapesobj.getOrDefault("labelType", null);

                    Integer shapes_width = (Integer) shapesobj.getOrDefault("width", null);

                    String shapes_group_id = (String) shapesobj.getOrDefault("group_id", null);

                    String shapes_radius = (String) shapesobj.getOrDefault("radius", null);

                    JSONObject shapes_flags = (JSONObject) shapesobj.getOrDefault("flags", new JSONObject());

                    String shapes_tag = (String) shapesobj.getOrDefault("tag", null);

                    JSONArray shapes_points = (JSONArray) shapesobj.getOrDefault("points", new JSONArray().fluentAdd(new JSONObject()));


                    JSONArray detectionDefectsMap_points = (JSONArray) detectionDefectsMapobj.getOrDefault("points", new JSONArray().fluentAdd(new JSONObject()));

                    for (int ap = 0; ap < Math.max(Math.max(Math.max(Math.max(Math.max(detectionDefects.size(), detectionDefectsMap.size()), shapes.size()), detectionDefectsTemp.size()), detectionDefectsMap_points.size()), shapes_points.size()); ap++) {

                        JSONObject detectionDefectsMap_pointsObject = ap < detectionDefectsMap_points.size() ? (JSONObject) detectionDefectsMap_points.get(ap) : new JSONObject();

                        JSONObject shapes_points_Object = ap < shapes_points.size() ? (JSONObject) shapes_points.get(ap) : new JSONObject();

                        Integer detectionDefectsMap_points_x = (Integer) detectionDefectsMap_pointsObject.getOrDefault("x", null);
                        Integer detectionDefectsMap_points_y = (Integer) detectionDefectsMap_pointsObject.getOrDefault("y", null);
                        Integer shapes_points_x = (Integer) shapes_points_Object.getOrDefault("x", null);
                        Integer shapes_points_y = (Integer) shapes_points_Object.getOrDefault("y", null);

                        collect(Row.of(detectionDefect_x,
                                detectionDefect_y,
                                detectionDefect_width,
                                detectionDefect_height,
                                detectionDefect_label,
                                detectionDefect_defectFeature,
                                null,
                                null,
                                null,
                                response_systemType,
                                response_uuid,
                                response_productName,
                                response_modelVersion,
                                response_imageName,
                                response_imageWidth,
                                response_imageHeight,
                                response_isCrop,
                                response_internalIsCrop,
                                response_detectionAreaType,
                                response_taskId,
                                response_partNo,
                                response_protoVersion,
                                response_streamLength,
                                response_estProcTime,
                                response_system_id,
                                response_project_id,
                                response_project_name,
                                response_device_no,
                                response_productConfigKey,
                                response_productMaxTime,
                                response_productSwitch,
                                response_seatMaxTime,
                                response_productDetectionType,
                                response_productHandleType,
                                response_reviewDefect,
                                response_result,
                                response_bashSampleType,
                                response_modelSampleType,
                                response_sortingSampleType,
                                response_sortingSampleTypeDetail,
                                response_merge_type,
                                detectionDefectsTemp_x,
                                detectionDefectsTemp_y,
                                detectionDefectsTemp_width,
                                detectionDefectsTemp_height,
                                detectionDefectsTemp_label,
                                detectionDefectsTemp_defectFeature,
                                detectionDefectsMap_bb_height,
                                detectionDefectsMap_bb_width,
                                detectionDefectsMap_brightness,
                                detectionDefectsMap_class_name,
                                detectionDefectsMap_class_type,
                                detectionDefectsMap_contrast,
                                detectionDefectsMap_datetime,
                                detectionDefectsMap_defect_feature,
                                detectionDefectsMap_detection_value,
                                detectionDefectsMap_gradients,
                                detectionDefectsMap_if_model, detectionDefectsMap_label, detectionDefectsMap_length,
                                detectionDefectsMap_max20brightness,
                                detectionDefectsMap_min20brightness, detectionDefectsMap_pixel_area,
                                detectionDefectsMap_score,
                                detectionDefectsMap_shape_type,
                                detectionDefectsMap_width,
                                detectionDefectsMap_xmin,
                                detectionDefectsMap_ymin,
                                shapes_label,
                                shapes_disputeResult,
                                shapes_disputeProcessTime,
                                shapes_score,
                                shapes_length,
                                shapes_pixel_area,
                                shapes_if_model,
                                shapes_model_info_model_name,
                                shapes_model_info_model_id,
                                shapes_model_info_model_date,
                                shapes_model_info_model_author,
                                shapes_labeller_info_name,
                                shapes_labeller_info_date,
                                shapes_shape_type,
                                shapes_labelType,
                                shapes_width,
                                shapes_group_id,
                                shapes_radius,
                                shapes_tag,
                                detectionDefectsMap_points_x,
                                detectionDefectsMap_points_y,
                                shapes_points_x,
                                shapes_points_y
                        ));

                    }


                }
            }

        }

    }

    @FunctionHint(output = @DataTypeHint("Row<imageWidth Int,imageHeight  Int,imageSize  Int,detectionResults_points_x  Int,detectionResults_points_y  Int,detectionResults_class_name  String,detectionResults_xmin  Int,detectionResults_ymin  Int,detectionResults_bb_width  Int,detectionResults_bb_height  Int,isCrop  Int,detectionArea  Int,detectionAreaType  Int,detectionComment  String,modelResult  String,imageTime  String,requestTime  String>"))
    public static class RequestJsonToRow extends TableFunction {
        public void eval(String json) throws JsonProcessingException {
            ObjectMapper mapper = new ObjectMapper();
            JsonNode node = mapper.readTree(json);
            if (json == null)
                collect(Row.of(new Object[]{
                        null, null, null, null, null, null, null, null, null, null,
                        null, null, null, null, null, null, null}));
            if (node.isObject()) {
                JSONObject requestObj = JSON.parseObject(json);
                Integer imageWidth = (Integer) requestObj.getOrDefault("imageWidth", null);
                Integer imageHeight = (Integer) requestObj.getOrDefault("imageHeight", null);
                Integer imageSize = (Integer) requestObj.getOrDefault("imageSize", null);
                JSONArray detectionResults = (JSONArray) requestObj.getOrDefault("detectionResults", (new JSONArray()).fluentAdd(new JSONObject()));
                Integer isCrop = (Integer) requestObj.getOrDefault("isCrop", null);
                Integer detectionArea = (Integer) requestObj.getOrDefault("detectionArea", null);
                Integer detectionAreaType = (Integer) requestObj.getOrDefault("detectionAreaType", null);
                String detectionComment = (String) requestObj.getOrDefault("detectionComment", null);
                String modelResult = (String) requestObj.getOrDefault("modelResult", null);
                String imageTime = (String) requestObj.getOrDefault("imageTime", null);
                String requestTime = (String) requestObj.getOrDefault("requestTime", null);
                for (int i = 0; i < detectionResults.size(); i++) {
                    JSONObject detectionResultsObj = (i < detectionResults.size()) ? (JSONObject) detectionResults.get(i) : new JSONObject();
                    JSONArray points = (JSONArray) requestObj.getOrDefault("points", (new JSONArray()).fluentAdd(new JSONObject()));
                    String detectionResults_class_name = (String) detectionResultsObj.getOrDefault("class_name", null);
                    Integer detectionResults_xmin = (Integer) detectionResultsObj.getOrDefault("xmin", null);
                    Integer detectionResults_ymin = (Integer) detectionResultsObj.getOrDefault("ymin", null);
                    Integer detectionResults_bb_width = (Integer) detectionResultsObj.getOrDefault("width", null);
                    Integer detectionResults_bb_height = (Integer) detectionResultsObj.getOrDefault("height", null);
                    for (int j = 0; j < Math.max(points.size(), detectionResults.size()); j++) {
                        JSONObject pointsObj = (j < points.size()) ? (JSONObject) points.get(j) : new JSONObject();
                        Integer detectionResults_points_x = (Integer) pointsObj.getOrDefault("x", null);
                        Integer detectionResults_points_y = (Integer) pointsObj.getOrDefault("y", null);
                        collect(Row.of(new Object[]{
                                imageWidth, imageHeight, imageSize, detectionResults_points_x, detectionResults_points_y, detectionResults_class_name, detectionResults_xmin, detectionResults_ymin, detectionResults_bb_width, detectionResults_bb_height,
                                isCrop, detectionArea, detectionAreaType, detectionComment, modelResult, imageTime, requestTime}));
                    }
                }
            }
        }
    }

}


