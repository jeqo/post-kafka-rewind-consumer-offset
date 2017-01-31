package io.github.jeqo.posts.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.util.Arrays;
import java.util.Set;

import static io.github.jeqo.posts.kafka.consumer.KafkaConsumerUtil.TOPIC;

/**
 * Created by jeqo on 25.01.17.
 */
public class KafkaConsumerFromBeginning {

    public static void main(String[] args) {

        KafkaConsumer<String, String> consumer = KafkaConsumerUtil.createConsumer();
        consumer.subscribe(Arrays.asList(TOPIC));
        boolean flag = true;

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);

            if (flag) {
                Set<TopicPartition> assignments = consumer.assignment();
                assignments.forEach(topicPartition ->
                        consumer.seekToBeginning(
                                Arrays.asList(topicPartition)));
                flag = false;
            }

            for (ConsumerRecord<String, String> record : records)
                System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
        }
    }
}
