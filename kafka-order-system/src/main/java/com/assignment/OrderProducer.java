package com.assignment;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.*;

// import java.io.IOException;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;

public class OrderProducer {

    public static void main(String[] args) throws Exception {

        Schema schema = AvroUtils.getSchema();

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:29092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");

        try (Producer<String, byte[]> producer = new KafkaProducer<>(props)) {

            Random random = new Random();

            while (true) {
                GenericRecord order = new GenericData.Record(schema);
                order.put("orderId", UUID.randomUUID().toString());
                order.put("product", "Item-" + random.nextInt(5));
                order.put("price", random.nextFloat() * 100);

                byte[] data = AvroUtils.serialize(schema, order);

                ProducerRecord<String, byte[]> record = new ProducerRecord<>("orders", order.get("orderId").toString(),
                        data);

                producer.send(record);

                System.out.println("Sent: " + order);

                Thread.sleep(1000);
            }
        }
    }
}
