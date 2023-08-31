package com.example.flink.wordCountAllOps;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class WordCountStream {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        final ParameterTool parameterTool = ParameterTool.fromArgs(args);

        env.getConfig().setGlobalJobParameters(parameterTool);

        DataStream<String> ds = env.socketTextStream("localhost", 9999);

        DataStream<Tuple2<String, Integer>> out = ds.flatMap(new Splitter())
                .filter(new MyFilter())
                .keyBy(f->f.f0)
                .sum(1);
                //.reduce(new Combinator());

        out.print();

        env.execute("Word Count Stream with starts with N");
    }
}
class MyFilter implements FilterFunction<Tuple2<String, Integer>> {

    @Override
    public boolean filter(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
        return stringIntegerTuple2.f0.startsWith("N");
    }
}
class Splitter implements FlatMapFunction<String, Tuple2<String, Integer>> {

    @Override
    public void flatMap(String sentence, Collector<Tuple2<String, Integer>> collector) throws Exception {
        for(String word: sentence.split(" ")) {
            collector.collect(new Tuple2<>(word, 1));
        }
    }
}
class Combinator implements ReduceFunction<Tuple2<String, Integer>> {

    @Override
    public Tuple2<String, Integer> reduce(Tuple2<String, Integer> t0, Tuple2<String, Integer> t1) throws Exception {
        return new Tuple2<>(t0.f0, t0.f1+t1.f1);
    }
}