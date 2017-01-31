package io.github.jeqo.posts.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

import java.time.Instant;
import java.util.*;

import static io.github.jeqo.posts.kafka.consumer.KafkaConsumerUtil.TOPIC;
import static io.github.jeqo.posts.kafka.consumer.KafkaConsumerUtil.createConsumer;
import static java.time.temporal.ChronoUnit.MINUTES;

/**
 * Created by jeqo on 25.01.17.
 */
public class KafkaConsumerFromTime {

    public static void main(String[] args) {
        KafkaConsumer<String, String> consumer = createConsumer();
        consumer.subscribe(Arrays.asList(TOPIC));

        boolean flag = true;

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);

            if (flag) {
                List<PartitionInfo> topicInfo = consumer.partitionsFor(TOPIC);
                Map<TopicPartition, Long> query = new HashMap<>();
                for (PartitionInfo info : topicInfo) {
                    query.put(
                            new TopicPartition(info.topic(), info.partition()),
                            Instant.now().minus(10, MINUTES).toEpochMilli());
                }

                Map<TopicPartition, OffsetAndTimestamp> result = consumer.offsetsForTimes(query);

                result.entrySet()
                        .stream()
                        .forEach(entry ->
                                consumer.seek(
                                        entry.getKey(),
                                        Optional.ofNullable(entry.getValue())
                                                .map(OffsetAndTimestamp::offset)
                                                .orElse(new Long(0))));

                flag = false;
            }


            for (ConsumerRecord<String, String> record : records)
                System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
        }


    }
}
