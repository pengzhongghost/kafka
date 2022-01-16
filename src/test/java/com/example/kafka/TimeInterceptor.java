package com.example.kafka;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

//拦截器
public class TimeInterceptor implements ProducerInterceptor<String, String> {

    private int success;
    private int error;

    @Override
    public void configure(Map<String, ?> configs) {

    }

    @Override
    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> record) {
        //取出具体的数据
        String value = System.currentTimeMillis() + "," + record.value();
        //创建一个新的对象
        return new ProducerRecord<>(record.topic(), record.partition(), record.key(), value);
    }

    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
        if (metadata != null) {
            success++;
        } else {
            error++;
        }
    }

    @Override
    public void close() {
        //只有producer.close()执行才会打印
        System.out.println("success:" + success);
        System.out.println("error:" + error);
    }

}
