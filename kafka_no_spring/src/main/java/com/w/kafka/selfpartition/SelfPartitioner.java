package com.w.kafka.selfpartition;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;

import java.util.List;
import java.util.Map;

/**
 * 自定义分区器   根据value值的分区
 */
public class SelfPartitioner implements Partitioner {
    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {

        //根据主题拿到分区
        List<PartitionInfo> partitionInfos = cluster.partitionsForTopic(topic);
        for (PartitionInfo partitionInfo : partitionInfos) {
            int partition = partitionInfo.partition();
            System.out.println("当前主题为："+topic+",分区为："+partition);
        }
        //分区数量
        int num=partitionInfos.size();
        System.out.println("当前主题为："+topic+",分区数量为："+num);
        //根据value的hashcode值来区分分区
        int parid = ((String) value).hashCode() % num;
        return parid;
    }

    @Override
    public void close() {
        // do nothing
    }

    @Override
    public void configure(Map<String, ?> configs) {
        // do nothing
    }
}
