package com.example.flink.fileExample;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.FileProcessingMode;

public class FileBasedContinuousRead {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        String path = "/home/ubuntu/Desktop/abc";

        TextInputFormat inputFormat = new TextInputFormat(new Path(path));

        DataStream<String> ds = env.readFile(inputFormat, path, FileProcessingMode.PROCESS_CONTINUOUSLY, 1000, BasicTypeInfo.STRING_TYPE_INFO);
        ds.print();

        env.execute();
    }
}