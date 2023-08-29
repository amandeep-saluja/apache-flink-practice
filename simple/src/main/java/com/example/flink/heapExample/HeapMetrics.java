package com.example.flink.heapExample;

public class HeapMetrics {
    public String area;
    public Long maxmem;
    public Long usedmem;
    public Integer jobId;
    public String hostname;
    public HeapMetrics() {

    }
    public HeapMetrics(String area,Long maxmem,Long usedmem,Integer jobId,String hostname) {

        this.area=area;
        this.maxmem=maxmem;
        this.usedmem=usedmem;
        this.jobId=jobId;
        this.hostname=hostname;
    }

    @Override
    public String toString() {

        return "HeapMetrics{" +
                "area=" + area +
                ", used=" + usedmem +
                ", max=" + maxmem +
                ", jobId=" + jobId +
                ", hostname='" + hostname + '\'' +
                '}';

    }

}
