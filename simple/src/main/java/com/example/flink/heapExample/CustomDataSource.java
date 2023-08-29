package com.example.flink.heapExample;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class CustomDataSource {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<HeapMetrics> ds = env.addSource(new HeapMonitorSource());
        ds.print();
        env.execute();
    }
}
