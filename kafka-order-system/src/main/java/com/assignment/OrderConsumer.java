package com.assignment;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class OrderConsumer {

    private static double runningAverage = 0.0;
    private static int count = 0;

    public static void main(String[] args) throws IOException {

        Schema schema = AvroUtils.getSchema();

        // Consumer properties
        Properties consumerProps = new Properties();
        consumerProps.put("bootstrap.servers", "localhost:29092");
        consumerProps.put("group.id", "order-consumer-group");
        consumerProps.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumerProps.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        consumerProps.put("auto.offset.reset", "earliest");

        KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(consumerProps);
        consumer.subscribe(Collections.singletonList("orders"));

        // Producer for retry topic
        Properties producerProps = new Properties();
        producerProps.put("bootstrap.servers", "localhost:29092");
        producerProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producerProps.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        Producer<String, byte[]> retryProducer = new KafkaProducer<>(producerProps);

        System.out.println("OrderConsumer started...");

        while (true) {
            ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofMillis(1000));

            for (ConsumerRecord<String, byte[]> record : records) {
                try {
                    GenericRecord order = AvroUtils.deserialize(schema, record.value());

                    double price = (double) order.get("price");

                    // Update running average
                    count++;
                    runningAverage = ((runningAverage * (count - 1)) + price) / count;

                    System.out.println("Received Order: " + order);
                    System.out.println("Current Running Average = " + runningAverage);
                    System.out.println("----------------------------------------------");

                    // Condition to simulate error: price > 80 → send to retry
                    if (price > 80) {
                        System.out.println("🚨 Price too high! Sending order to RETRY topic.");

                        retryProducer.send(new ProducerRecord<>(
                                "orders-retry",
                                record.key(),
                                record.value()));

                        continue;
                    }

                } catch (Exception ex) {
                    ex.printStackTrace();
                    System.out.println("Deserialization failed → sending to RETRY");

                    retryProducer.send(new ProducerRecord<>(
                            "orders-retry",
                            record.key(),
                            record.value()));
                }
            }
        }
    }
}
