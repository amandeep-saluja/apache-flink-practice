package com.example.flink;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.FileProcessingMode;


public class FileReadAvgProfit {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        String path = "/home/ubuntu/Downloads/avg";

        TextInputFormat inputFormat = new TextInputFormat(new Path(path));

        DataStream<String> ds = env.readFile(inputFormat, path, FileProcessingMode.PROCESS_CONTINUOUSLY, 1000, BasicTypeInfo.STRING_TYPE_INFO);

        DataStream<Tuple2<String, Double>> out = ds.map(new Splitter()).keyBy(0).reduce(new MyReduce()).map(new AvgMap());

        out.print();

        env.execute();
    }
}

class Splitter implements MapFunction<String, Tuple5<String, String, String, Integer, Integer>> {

    @Override
    public Tuple5<String, String, String, Integer, Integer> map(String s) throws Exception {
        String[] row = s.split(",");
        return new Tuple5<>(row[1], row[2], row[3], Integer.parseInt(row[4]), 1);
    }
}

class MyReduce implements ReduceFunction<Tuple5<String, String, String, Integer, Integer>> {

    @Override
    public Tuple5<String, String, String, Integer, Integer> reduce(Tuple5<String, String, String, Integer, Integer> current, Tuple5<String, String, String, Integer, Integer> previous) throws Exception {
        return new Tuple5<>(current.f0, current.f1, current.f2, current.f3 + previous.f3, current.f4 + previous.f4);
    }
}

class AvgMap implements MapFunction<Tuple5<String, String, String, Integer, Integer>, Tuple2<String, Double>> {

    @Override
    public Tuple2<String, Double> map(Tuple5<String, String, String, Integer, Integer> tuple) throws Exception {
        return new Tuple2<>(tuple.f0, (tuple.f3 * 1.0) / tuple.f4);
    }
}