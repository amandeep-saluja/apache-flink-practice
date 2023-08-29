package com.example.flink.sampleSink;

import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

public class StringSource extends RichParallelSourceFunction<String> {

    @Override
    public void run(SourceContext<String> sourceContext) throws Exception {
        int i = 0;
        while(true) {
            sourceContext.collect(""+i);
            i+=1;
            Thread.sleep(1000);
        }
    }

    @Override
    public void cancel() {

    }
}
