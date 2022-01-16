package com.example.kafka;

import org.apache.kafka.clients.producer.*;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.Properties;

@SpringBootTest
class KafkaApplicationTests {

  @Test
  void contextLoads() {
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
}
