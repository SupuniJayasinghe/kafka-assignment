package com.assignment;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class RetryProcessor {

    public static void main(String[] args) throws IOException {

        Schema schema = AvroUtils.getSchema();

        // Consumer for retry topic
        Properties consumerProps = new Properties();
        consumerProps.put("bootstrap.servers", "localhost:29092");
        consumerProps.put("group.id", "retry-processor-group");
        consumerProps.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumerProps.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        consumerProps.put("auto.offset.reset", "earliest");

        KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(consumerProps);
        consumer.subscribe(Collections.singletonList("orders-retry"));

        // Producer for DLQ
        Properties producerProps = new Properties();
        producerProps.put("bootstrap.servers", "localhost:29092");
        producerProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producerProps.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        Producer<String, byte[]> dlqProducer = new KafkaProducer<>(producerProps);

        System.out.println("RetryProcessor started...");

        while (true) {
            ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofMillis(1000));

            for (ConsumerRecord<String, byte[]> record : records) {

                try {
                    GenericRecord order = AvroUtils.deserialize(schema, record.value());

                    double price = (double) order.get("price");
                    System.out.println("Retrying Order: " + order);

                    if (price > 80) {
                        System.out.println("Still invalid after retry → sending to DLQ");

                        dlqProducer.send(new ProducerRecord<>(
                                "orders-dlq",
                                record.key(),
                                record.value()));
                        continue;
                    }

                    System.out.println("✅ Retry successful: Order is now valid.");

                } catch (Exception ex) {
                    System.out.println("Retry failed completely → sending to DLQ");

                    dlqProducer.send(new ProducerRecord<>(
                            "orders-dlq",
                            record.key(),
                            record.value()));
                }
            }
        }
    }
}
