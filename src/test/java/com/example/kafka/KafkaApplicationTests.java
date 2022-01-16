package com.example.kafka;

import lombok.SneakyThrows;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.*;
import org.assertj.core.util.Lists;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import org.apache.kafka.common.serialization.Deserializer.*;

import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

@SpringBootTest
class KafkaApplicationTests {

    /**
     * 生产者
     */
    @Test
    void producer() {
        // TODO 1.配置
        Properties properties = new Properties();
        // ProducerConfig定义了所有的配置
        // todo 1.指定连接的kafka集群
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.227.129:9092");
        // todo 2.ack应答级别
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        // todo 3.重试次数
        properties.put(ProducerConfig.RETRIES_CONFIG, 3);
        // 大小到达16k或者时间到达1ms发送
        // todo 4.批次大小 16k
        properties.put("batch.size", 16384);
        // todo 5.等待时间 1ms
        properties.put("linger.ms", 1);
        // todo 6.recordaccumulator缓冲区大小
        properties.put("buffer.memory", 33554432);
        // todo 7.key value的序列化类
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        // todo 8.添加分区器
        properties.put(
                ProducerConfig.PARTITIONER_CLASS_CONFIG,
                "org.apache.kafka.clients.producer.RoundRobinPartitioner");
        //todo 9.添加拦截器
        properties.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, Lists.newArrayList("com.example.kafka.TimeInterceptor"));
        // TODO 2.发送
        KafkaProducer<String, String> producer = new KafkaProducer(properties);
        // todo 1.异步发送
        //    for (int i = 0; i < 10; i++) {
        //      producer.send(new ProducerRecord<String, String>("first", "test" + i));
        //    }
        // todo 2.带异步回调发送
        for (int i = 0; i < 10; i++) {
            // 指定分区按照指定的分区存储,否则按照key的hash值分区的，都没有指定则按照轮询分区存储
            producer.send(
                    new ProducerRecord<String, String>("first", "ni", "test" + i),
                    new Callback() {
                        @Override
                        public void onCompletion(RecordMetadata metadata, Exception exception) {
                            if (exception == null) {
                                System.out.println(metadata);
                            } else {
                                exception.printStackTrace();
                            }
                        }
                    });
        }
        // TODO 3.关闭资源
        producer.close();
    }

    @Test
    @SneakyThrows
    void consumer() {
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
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "consumer01");
        //What to do when there is no initial offset in Kafka or if the current offset does not exist any more on the server
        //当消费者组名第一次消费此属性会生效，或者这个数据已经被删除了会生效
        //换了一个组或者offset已经不存在了会生效
        //默认是latest，以前的数据消费不到,earliest从最开始开始消费
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        //创建消费者
        KafkaConsumer<String, String> consumer = new KafkaConsumer(properties);
        //订阅主题
        consumer.subscribe(Lists.newArrayList("first", "second"));
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
