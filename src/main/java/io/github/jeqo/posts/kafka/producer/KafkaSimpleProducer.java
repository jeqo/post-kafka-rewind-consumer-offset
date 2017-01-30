package io.github.jeqo.posts.kafka.producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.tamaya.Configuration;
import org.apache.tamaya.ConfigurationProvider;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.FormatStyle;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.stream.IntStream;

/**
 * Created by jeqo on 25.01.17.
 */
public class KafkaSimpleProducer {

    public static void main(String[] args) {
        Configuration config = ConfigurationProvider.getConfiguration();

        String bootstrapServers = config.getOrDefault("kafka.bootstrap_servers", "localhost:9092");

        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        Producer<String, String> producer = new KafkaProducer<>(properties);

        IntStream.rangeClosed(1, 100)
                .boxed()
                .map(number -> new ProducerRecord<>(
                        "topic-1",
                        number.toString(),
                        number.toString()))
                .map(record -> producer.send(record))
                .forEach(result -> printMetadata(result));
        producer.close();
    }

    private static void printMetadata(Future<RecordMetadata> f) {
        try {
            DateTimeFormatter formatter = DateTimeFormatter.ofLocalizedDateTime(FormatStyle.SHORT)
                    .withZone(ZoneId.systemDefault());
            RecordMetadata metadata = f.get();
            System.out.println(String.format(
                    "offset=>%s ts=>%s",
                    metadata.offset(),
                    formatter.format(Instant.ofEpochMilli(metadata.timestamp()))));
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
    }
}
