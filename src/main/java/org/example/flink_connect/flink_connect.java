package org.example.flink_connect;

import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Properties;

public class flink_connect {
    public StreamExecutionEnvironment Secure() {

        // Todo: 构建流处理环境
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        // Todo: 开启checkpoint(非重点)
        // 默认checkpoint功能是disabled的，想要使用的时候需要先启用
        // 每隔5000 ms进行启动一个检查点【设置checkpoint的周期】
        environment.enableCheckpointing(5000);
        // 高级选项：
        // 设置模式为exactly-once （这是默认值）
        environment.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        // 确保检查点之间有至少500 ms的间隔【checkpoint最小间隔】
        environment.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
        // 检查点必须在一分钟内完成，或者被丢弃【checkpoint的超时时间】
        environment.getCheckpointConfig().setCheckpointTimeout(60000);
        // 同一时间只允许进行一个检查点
        environment.getCheckpointConfig().setMaxConcurrentCheckpoints(1);

        // Todo: 设置时间语义类型
        environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        return environment;

    }
}
