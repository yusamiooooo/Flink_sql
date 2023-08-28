package org.example.flink_connect;

import javafx.scene.control.Alert;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.PreparedStatement;

public class ClickhouseFunction extends RichSinkFunction<ClickhouseAlert> {
    Connection connection = null;

    String sql;

    public ClickhouseFunction(String sql) {
        this.sql = sql;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        connection = ClickUtils.getConnection();
    }

    @Override
    public void close() throws Exception {
        super.close();
        if(connection!=null){
            connection.close();
        }
    }

    @Override
    public void invoke(ClickhouseAlert value, Context context) throws Exception {
        PreparedStatement preparedStatement = connection.prepareStatement(sql);
        preparedStatement.setLong(1,value.timestamp);
        preparedStatement.setString(2,value.src_ip);
        preparedStatement.setString(3,value.src_port);
        preparedStatement.setString(4, value.dest_ip);
        preparedStatement.setString(5,value.dest_port);
        preparedStatement.setString(6,value.sid);
        preparedStatement.setString(7,value.signature);
        preparedStatement.setString(8,value.category);
        preparedStatement.setInt(9,value.severity);
        preparedStatement.setString(10,value.device_id);
        preparedStatement.addBatch();

        long startTime = System.currentTimeMillis();
        int[] ints = preparedStatement.executeBatch();
//        connection.commit();
        long endTime = System.currentTimeMillis();
//        System.out.println("批量插入完毕用时：" + (endTime - startTime) + " -- 插入数据 = " + ints.length);
        System.out.println("数据插入成功："+value);
    }
}
