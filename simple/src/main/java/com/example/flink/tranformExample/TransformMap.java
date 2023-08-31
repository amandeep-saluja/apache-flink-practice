package com.example.flink.tranformExample;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Locale;

public class TransformMap {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> ds = env.socketTextStream("localhost", 9000, "\n");
        //DataStream<String> ds = env.fromElements("one", "two", "three", "four", "five");
        DataStream<String> output = ds.map(data->data.toUpperCase(Locale.ROOT));
        output.print();
        env.execute();
    }

    public static void runWithHardcodedData(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> ds = env.fromElements("one", "two", "three", "four", "five");
        DataStream<String> output = ds.map(data->data.toUpperCase(Locale.ROOT));
        output.print();
        env.execute();
    }
}
