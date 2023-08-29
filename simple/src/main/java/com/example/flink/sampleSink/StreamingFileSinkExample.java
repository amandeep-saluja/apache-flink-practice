package com.example.flink.sampleSink;


import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;

public class StreamingFileSinkExample {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(1000);
        env.setStateBackend((StateBackend) new FsStateBackend("file:///home/ubuntu/checkpoint"));
        DataStream<String> ds = env.addSource(new StringSource());
        StreamingFileSink<String> sink = StreamingFileSink.<String>forRowFormat(new Path("/home/ubuntu/Desktop/abc"), new SimpleStringEncoder<String>("UTF-8"))
                .withBucketAssigner(new DateTimeBucketAssigner<String>("yyyy-MM-dd--HH"))
                .build();
        ds.addSink(sink);
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
