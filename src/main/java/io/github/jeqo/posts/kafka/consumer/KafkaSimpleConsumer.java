package io.github.jeqo.posts.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.tamaya.Configuration;
import org.apache.tamaya.ConfigurationProvider;

import java.util.Arrays;
import java.util.Properties;

import static io.github.jeqo.posts.kafka.consumer.KafkaConsumerUtil.TOPIC;
import static io.github.jeqo.posts.kafka.consumer.KafkaConsumerUtil.createConsumer;

/**
 * Created by jeqo on 25.01.17.
 */
public class KafkaSimpleConsumer {

    public static void main(String[] args) {
        KafkaConsumer<String, String> consumer = createConsumer();
        consumer.subscribe(Arrays.asList(TOPIC));

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);

            for (ConsumerRecord<String, String> record : records)
                System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
        }
    }
}
