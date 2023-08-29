package com.example.flink.heapExample;

import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryPoolMXBean;
import java.lang.management.MemoryType;
import java.lang.management.MemoryUsage;
import java.net.InetAddress;

public class HeapMonitorSource extends RichParallelSourceFunction<HeapMetrics> {
    public void run(SourceContext<HeapMetrics> arg0) throws Exception {

        while(true) {
            Integer jobid = this.getRuntimeContext().getIndexOfThisSubtask();
            String hostname = InetAddress.getLocalHost().getHostName();
            for(MemoryPoolMXBean mbbean : ManagementFactory.getMemoryPoolMXBeans()) {
                if(mbbean.getType()== MemoryType.HEAP) {
                    MemoryUsage m = mbbean.getUsage();
                    Long maxmem = m.getMax();
                    Long usedmem = m.getUsed();
                    arg0.collect(new HeapMetrics(mbbean.getName(), maxmem, usedmem, jobid, hostname));
                }
            }
            Thread.sleep(3000);
        }
    }

    public void cancel() {

    }
}