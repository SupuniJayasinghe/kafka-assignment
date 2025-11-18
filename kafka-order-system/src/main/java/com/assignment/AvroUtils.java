package com.assignment;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.*;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;

public class AvroUtils {

    public static Schema getSchema() throws IOException {
        return new Schema.Parser().parse(new File("src/main/resources/avro/order.avsc"));
    }

    public static byte[] serialize(Schema schema, GenericRecord record) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
        GenericDatumWriter<GenericRecord> writer = new GenericDatumWriter<>(schema);
        writer.write(record, encoder);
        encoder.flush();
        return out.toByteArray();
    }

    public static GenericRecord deserialize(Schema schema, byte[] data) throws IOException {
        Decoder decoder = DecoderFactory.get().binaryDecoder(data, null);
        GenericDatumReader<GenericRecord> reader = new GenericDatumReader<>(schema);
        return reader.read(null, decoder);
    }
}
