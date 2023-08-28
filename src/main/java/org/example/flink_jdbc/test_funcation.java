package org.example.flink_jdbc;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.type.TypeReference;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.types.Row;

import java.io.IOException;
import java.util.Map;

public class test_funcation {
    public static void main(String[] args) throws JsonProcessingException {

        ObjectMapper mapper = new ObjectMapper();

            // 将 JSON 字符串转换为 Map 对象
            Map<String, Object> map = mapper.readValue("{\"info\":{\"request\":{},\"state\":{\"skilllist\":[{\"userid\": \"111\",\"skilllevel\": \"99.0\"}]}}}", new TypeReference<Map<String, Object>>() {
            });

        // 获取 info 对象的值
        Object info = map.get("info");
// 判断 info 是否是一个 Map 对象
        if (info instanceof Map) {
            // 强制转换为 Map 类型
            Map<String, Object> infoMap = (Map<String, Object>) info;
            // 遍历 infoMap 的所有键
            for (String key : infoMap.keySet()) {
                // 获取键对应的值
                Object value = infoMap.get(key);
                // 打印键和值
                System.out.println(key + ": " + value);
            }

        }




    }
    }

