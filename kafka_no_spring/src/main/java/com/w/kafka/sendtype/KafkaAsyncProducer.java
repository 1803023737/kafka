package com.w.kafka.sendtype;

import com.w.kafka.config.BusiConst;
import com.w.kafka.config.KafkaConst;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

public class KafkaAsyncProducer {

    /**
     * 消息生产者
     */
    private static KafkaProducer<String,String> producer=null;

    public static void main(String[] args) {

        /**
         * 初始化producer
         */
        producer=new KafkaProducer<String, String>(KafkaConst.producerConfig(StringSerializer.class,StringSerializer.class));
        /**
         * 创建消息实例
         */
        ProducerRecord<String,String> record;
        try {
            record=new ProducerRecord<String,String>(BusiConst.HELLO_TOPIC,"test3","test2 message");
            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                        if (null!=exception){
                            exception.printStackTrace();
                        }
                        if (null!=metadata){
                            System.out.println("offset:"+metadata.offset()+",partition:"+metadata.partition()+",topic:"+metadata.topic());
                        }
                }
            });
            System.out.println("do some thing");

        } catch (Exception e) {
            e.printStackTrace();
        }finally {
            //必须提交,关闭
            producer.close();
        }
    }

}
