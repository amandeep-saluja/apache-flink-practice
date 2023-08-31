package com.example.flink.collectionExample;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.List;

public class StreamObjectCreation {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        List<String> listObj = new ArrayList<>();
        listObj.add("First");
        listObj.add("Two");
        listObj.add("Third");

        DataStream<String> ds = env.fromCollection(listObj);

        ds.print();

        env.execute();
    }
}
