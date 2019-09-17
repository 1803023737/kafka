package com.w.kafka.sendtype;

import com.w.kafka.config.BusiConst;
import com.w.kafka.config.KafkaConst;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class KafkaFutureProducer {

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
            record=new ProducerRecord<String,String>(BusiConst.HELLO_TOPIC,"test2","test2 message");
            Future<RecordMetadata> future = producer.send(record);
            System.out.println("do some thing");
            RecordMetadata recordMetadata = future.get();
            if (recordMetadata!=null){
                //拿到回馈
                System.out.println(recordMetadata.offset()+"   "+recordMetadata.partition()+ "   "+recordMetadata.toString());
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }

    }

}
