package com.w.kafka.hellokafka;

import com.w.kafka.config.BusiConst;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Collections;
import java.util.Properties;

public class HelloKafkaConsumer {

    public static void main(String[] args) {

        Properties properties=new Properties();
        //服务器地址  常量名称 可以通过常量配之类获得
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9093");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class);
        //消费者群组
        properties.put(ConsumerConfig.GROUP_ID_CONFIG,"test1");
        KafkaConsumer<String,String> consumer=new KafkaConsumer<String, String>(properties);
        //接收消息
        //拉去信息   只有拉取方式
        try {
            //订阅主题
            consumer.subscribe(Collections.singletonList(BusiConst.HELLO_TOPIC));
            while (true){
                //间隔多久拉取一次
                System.out.println("==========1=======");
                ConsumerRecords<String, String> records = consumer.poll(5000);
                System.out.println("==========1======="+records.count());
                for (ConsumerRecord<String, String> record : records) {
                    System.out.println(String.format("topic:%s;分区:%d;偏移量:%d;key:%s,value:%s",
                            record.topic(),record.partition(),record.offset(),record.key(),record.value()));
                }
                System.out.println("==========2=======");
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            consumer.close();
        }
    }

}
