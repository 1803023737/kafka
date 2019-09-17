package com.w.kafka.concurrent;

import com.w.kafka.config.BusiConst;
import com.w.kafka.config.KafkaConst;
import com.w.kafka.vo.DemoUser;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * 多线程下生产者
 */
public class KafkaConProducer {

    //发送消息的个数
    private static final int MSG_SIZE=1000;
    //负责发送消息的线程池  根据机器中核心的个数
    private static ExecutorService executorService
            = Executors.newFixedThreadPool(
            Runtime.getRuntime().availableProcessors());
    //屏障
    private static CountDownLatch countDownLatch
            = new CountDownLatch(MSG_SIZE);

    private static DemoUser makeUser(int id){
        //可以看到不同的消息
        DemoUser demoUser=new DemoUser(id);
        String userName="xiangxue_"+id;
        demoUser.setName(userName);
        return demoUser;
    }

    /**
     * 发送消息的任务
     */
    private static class ProduceWorker implements Runnable{

        private ProducerRecord<String,String> record;
        private KafkaProducer<String,String> producer;

        public ProduceWorker(ProducerRecord<String, String> record, KafkaProducer<String, String> producer) {
            this.record = record;
            this.producer = producer;
        }

        @Override
        public void run() {
            //发送
            final String id=Thread.currentThread().getId()+"-"+System.identityHashCode(producer);
            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if (null!=exception){
                        exception.printStackTrace();
                    }
                    if (null!=metadata){
                        System.out.println("offset:"+metadata.offset()+",partition:"+metadata.partition());
                    }
                }
            });
            System.out.println(id+":数据["+record+"]已发送");
            countDownLatch.countDown();
        }
    }

    public static void main(String[] args) {
        //只有一个发送者
        KafkaProducer<String, String> producer =
                new KafkaProducer<>(KafkaConst.producerConfig(StringSerializer.class, StringSerializer.class));
        try {
            for (int i=0;i<MSG_SIZE;i++){
                DemoUser demoUser = makeUser(i);
                ProducerRecord<String, String> record =
                        new ProducerRecord<>(BusiConst.CONCURRENT_USER_INFO_TOPIC, null, System.currentTimeMillis(), demoUser.getId() + "", demoUser.toString());
                Future<?> submit = executorService.submit(new ProduceWorker(record, producer));
            }
            //防止主线程太早退出  等待多有子线程执行完后退出
            countDownLatch.await();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            producer.close();
            executorService.shutdown();
        }
    }
}
