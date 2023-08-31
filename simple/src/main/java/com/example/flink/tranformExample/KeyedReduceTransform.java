package com.example.flink.tranformExample;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class KeyedReduceTransform {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        Tuple2<String, Integer> p1 = new Tuple2<>("Player1", 10);
        Tuple2<String, Integer> p2 = new Tuple2<>("Player1", 20);
        Tuple2<String, Integer> p3 = new Tuple2<>("Player2", 30);
        Tuple2<String, Integer> p4 = new Tuple2<>("Player2", 130);
        Tuple2<String, Integer> p5 = new Tuple2<>("Player1", 100);
        Tuple2<String, Integer> p6 = new Tuple2<>("Player3", 100);

        DataStream<Tuple2<String, Integer>> ds = environment.fromElements(p1, p2, p3, p4, p5, p6);
        SingleOutputStreamOperator<Tuple2<String, Integer>> keyedStream = ds.keyBy(v->v.f0).reduce(new MyReducer());
        keyedStream.print();

        environment.execute("Keyed Stream with reduce Example");
    }
}
