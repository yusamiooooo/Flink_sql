package org.example.kafka_connect;

import java.util.Properties;

public class kafka_connect {

    public Properties Secure() {

        Properties props_source = new Properties();
        //consumer配置
        props_source.put("bootstrap.servers", "172.16.116.123:9092");
        props_source.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props_source.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        //安全认证配置
        props_source.put("security.protocol", "PLAINTEXT");
        props_source.put("group.id", "flink");
        // 事务超时等待时间默认为15分钟(这里只能比15分钟小)
        props_source.put("transaction.timout.ms", 5 * 60 * 1000 + "");

        return props_source;

    }
}
