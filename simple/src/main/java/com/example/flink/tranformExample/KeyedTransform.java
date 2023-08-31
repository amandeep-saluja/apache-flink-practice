package com.example.flink.tranformExample;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class KeyedTransform {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Tuple2<String, Integer> p1 = new Tuple2<>("Player1", 10);
        Tuple2<String, Integer> p2 = new Tuple2<>("Player1", 20);
        Tuple2<String, Integer> p3 = new Tuple2<>("Player2", 30);
        Tuple2<String, Integer> p4 = new Tuple2<>("Player2", 130);
        Tuple2<String, Integer> p5 = new Tuple2<>("Player1", 100);
        Tuple2<String, Integer> p6 = new Tuple2<>("Player3", 100);

        DataStream<Tuple2<String, Integer>> ds = env.fromElements(p1, p2, p3, p4, p5, p6);
        KeyedStream<Tuple2<String, Integer>, String> keyedStream = ds.keyBy(v->v.f0);
        keyedStream.print();

        env.execute("Keyed Stream Example");
    }
}
