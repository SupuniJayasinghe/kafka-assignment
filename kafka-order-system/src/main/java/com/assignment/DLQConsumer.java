package com.assignment;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.*;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class DLQConsumer {

    public static void main(String[] args) throws IOException {

        Schema schema = AvroUtils.getSchema();

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:29092");
        props.put("group.id", "dlq-consumer-group");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        props.put("auto.offset.reset", "earliest");

        KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList("orders-dlq"));

        System.out.println("DLQConsumer started...");

        while (true) {
            ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofMillis(1000));

            for (ConsumerRecord<String, byte[]> record : records) {
                try {
                    GenericRecord order = AvroUtils.deserialize(schema, record.value());

                    System.out.println("💀 DLQ ORDER FOUND!");
                    System.out.println(order);
                    System.out.println("----------------------------");

                } catch (Exception ex) {
                    System.out.println("DLQ message failed to deserialize!");
                }
            }
        }
    }
}
