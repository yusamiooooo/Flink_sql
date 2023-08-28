package org.example.flink_jdbc;

import kafka.Kafka;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.functions.FlatMapIterator;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.table.api.Tumble;
import org.apache.flink.util.Collector;
import org.example.flink_connect.flink_connect;
import org.example.kafka_connect.kafka_connect;

import java.util.Iterator;
import java.util.Properties;
import java.util.Random;

public class flink_sink_kafka {


    public static void main(String[] args) {
        // 创建 Flink 流处理环境


        //获取kafka配置环境
        kafka_connect kafka_connect = new kafka_connect();

        Properties props_source = kafka_connect.Secure();

        //获取flink配置环境
        flink_connect flink_connect = new flink_connect();

        StreamExecutionEnvironment environment = flink_connect.Secure();

        //设置kafka主题
        String topic = "aiotest";

        //flink配置kafka数据源
        FlinkKafkaConsumer<String> kafkaSource = new FlinkKafkaConsumer<>(topic, new SimpleStringSchema(), props_source);
        kafkaSource.setStartFromLatest();
        //2.输入端保证：执行Checkpoint的时候提交offset到Checkpoint(Flink用)
        kafkaSource.setCommitOffsetsOnCheckpoints(true);
        DataStreamSource<String> kafkaDS = environment.addSource(kafkaSource);

        kafkaDS.print();//把从 kafka 读取到的数据打印在控制台

        //3.对数据进行json转换
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndOneDS = kafkaDS.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {

                String[] words = value.split(" ");
                //遍历
                for (String word : words) {
                    String nihao = "nihao";
                    String connect=nihao+word;
                    System.out.println(connect);
                    out.collect(Tuple2.of(connect,1));
                }
            }
        });

        //3.对数据进行json转换
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndOneDS2 = kafkaDS.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {

                String[] words = value.split(" ");
                //遍历
                for (String word : words) {
                    String nihao = "nihao";
                    String connect=nihao+word;
                    System.out.println(connect);
                    out.collect(Tuple2.of(connect,1));
                }
            }
        });








        //执行
        try {
            environment.execute("kafka test");
        } catch (Exception e) {
            e.printStackTrace();
        }


    }


}
