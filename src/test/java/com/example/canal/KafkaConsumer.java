package com.example.canal;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.assertj.core.util.Lists;

import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class KafkaConsumer {
    public static void main(String[] args) throws InterruptedException {
        //创建消费者的配置信息
        Properties properties = new Properties();
        //连接的集群
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.227.129:9092");
        //打开自动提交,自动提交的延迟为1s
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
        properties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        //key value反序列化的类
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        //消费者组
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "consumer3");
        //What to do when there is no initial offset in Kafka or if the current offset does not exist any more on the server
        //当消费者组名第一次消费此属性会生效，或者这个数据已经被删除了会生效
        //换了一个组或者offset已经不存在了会生效
        //默认是latest，以前的数据消费不到,earliest从最开始开始消费
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        //创建消费者
        org.apache.kafka.clients.consumer.KafkaConsumer<String, String> consumer = new org.apache.kafka.clients.consumer.KafkaConsumer(properties);
        //订阅主题
        consumer.subscribe(Lists.newArrayList("canal_test"));
        //获取数据 长轮询如果没有拉取到数据需要给一个延迟时间，防止空转
        ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofSeconds(30));
        for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
            System.out.println(consumerRecord.key() + "==>" + consumerRecord.value());
        }
        TimeUnit.SECONDS.sleep(30);
        //关闭连接
        consumer.close();
    }
}
