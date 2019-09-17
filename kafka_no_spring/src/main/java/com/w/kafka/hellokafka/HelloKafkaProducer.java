package com.w.kafka.hellokafka;

import com.w.kafka.config.BusiConst;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.Future;

/**
 * kafka生产者
 */
public class HelloKafkaProducer {

    public static void main(String[] args) {

        Properties properties=new Properties();
        //服务器地址
       // properties.put("bootstrap.servers","127.0.0.1:9092");
//        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
//        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9093");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        KafkaProducer<String,String> producer=new KafkaProducer<String, String>(properties);
        //消息记录
        ProducerRecord<String,String> record;
        try {
            record=new ProducerRecord<String,String>(BusiConst.HELLO_TOPIC,"message key","message body22");
            //发送
            Future<RecordMetadata> future = producer.send(record);
            System.out.println("message is send...");
            if (future!=null){
                RecordMetadata recordMetadata = future.get();
                System.out.println(recordMetadata.offset());

            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            //关闭
            producer.close();
        }

    }


}
