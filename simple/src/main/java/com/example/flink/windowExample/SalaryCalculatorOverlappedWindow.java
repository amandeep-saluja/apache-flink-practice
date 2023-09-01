package com.example.flink.windowExample;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

public class SalaryCalculatorOverlappedWindow
{
    public static void main(String[] args) throws Exception
    {
        // set up the streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

        env.setParallelism(4);

        DataStream<String> data = env.socketTextStream("localhost", 9090);

        // month, product, category, profit, count
        DataStream<Tuple2<String, Integer>> mapped = data.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String s) throws Exception {
                String[] arr = s.split(",");

                return new Tuple2<String, Integer>(arr[0],Integer.valueOf(arr[1]));
            }
        });
        /*DataStream<Tuple2<String, Integer>> reduced = mapped
                .keyBy(0)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(20)))
                .sum(1);*/
        DataStream<Tuple2<String, Integer>> reduced = mapped
                .keyBy(0)
                .window(SlidingProcessingTimeWindows.of(Time.seconds(30), Time.seconds(10)))
                .sum(1);



        reduced.print();
        // execute program
        env.execute("Salary Calculator");
    }


}

