package de.azapps.kafkabackup.common.record;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.sql.Time;
import java.util.Base64;

import org.apache.kafka.common.record.TimestampType;

public class RecordJSONSerdeTest {

    private static final String topic = "test-topic";
    private static final int partition = 42;
    private static final long offset = 123;
    private static final TimestampType timestampType = TimestampType.LOG_APPEND_TIME;
    private static final Long timestamp = 573831430000L;
    private static byte[] keyBytes;
    private static String keyBase64;
    private static byte[] valueBytes;
    private static String valueBase64;
    private static final String JSON_ENCODING = "UTF-8";
    private static ObjectMapper mapper;
    private static RecordJSONSerde serde;

    @BeforeAll
    public static void beforeAll() throws Exception {
        // encoding here is not really important, we just want some bytes
        keyBytes = "test-key".getBytes("UTF-8");
        valueBytes = "test-value".getBytes("UTF-8");
        // using Base64 as encoding in the json is part of our Record serialization format however:
        keyBase64 = Base64.getEncoder().encodeToString(keyBytes);
        valueBase64 = Base64.getEncoder().encodeToString(valueBytes);
    }

    @BeforeEach
    public void beforeEach() throws Exception {
        mapper = new ObjectMapper();
        SimpleModule module = new SimpleModule();
        module.addDeserializer(Record.class, new RecordJSONSerde.Deserializer());
        module.addSerializer(Record.class, new RecordJSONSerde.Serializer());
        mapper.registerModule(module);

        serde = new RecordJSONSerde();
    }

    @Test
    public void readTest() throws Exception {
        // GIVEN
        // TODO: add headers
        byte[] json = String.format("{\"topic\":\"%s\",\"partition\":%d,\"offset\":%d,\"timestamp_type\":\"%s\",\"timestamp\":%d,\"key\":\"%s\",\"value\":\"%s\"}", topic, partition, offset, timestampType, timestamp, keyBase64, valueBase64).getBytes(JSON_ENCODING);
        InputStream inputStream = new ByteArrayInputStream(json);

        // WHEN
        Record actual = serde.read(inputStream);

        // THEN
        Record expected = new Record(topic, partition, keyBytes, valueBytes, offset, timestamp, timestampType);
        assertEquals(expected, actual);
    }

    @Test
    public void writeTest() throws Exception {
        // GIVEN
        // TODO: add headers
        Record record = new Record(topic, partition, keyBytes, valueBytes, offset, timestamp, timestampType);

        // WHEN
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        serde.write(outputStream, record);
        byte[] actual = outputStream.toByteArray();

        // THEN
        // NOTE: here we make some (semi-dangerous) assumptions regarding
        // - deterministic key ordering, and
        // - compact formatting without white-space
        byte[] expected = String.format("{\"topic\":\"%s\",\"partition\":%d,\"offset\":%d,\"timestamp_type\":\"%s\",\"timestamp\":%d,\"key\":\"%s\",\"value\":\"%s\"}", topic, partition, offset, timestampType, timestamp, keyBase64, valueBase64).getBytes(JSON_ENCODING);
        assertArrayEquals(expected, actual);
    }

    @Test
    public void deserializeTest() throws Exception {
        // GIVEN
        // TODO: add headers
        byte[] json = String.format("{\"topic\":\"%s\",\"partition\":%d,\"offset\":%d,\"timestamp_type\":\"%s\",\"timestamp\":%d,\"key\":\"%s\",\"value\":\"%s\"}", topic, partition, offset, timestampType, timestamp, keyBase64, valueBase64).getBytes(JSON_ENCODING);
        InputStream inputStream = new ByteArrayInputStream(json);

        // WHEN
        Record actual = mapper.readValue(inputStream, Record.class);

        // THEN
        Record expected = new Record(topic, partition, keyBytes, valueBytes, offset, timestamp, timestampType);
        assertEquals(expected, actual);
    }

    @Test
    public void deserializeTestNullKeyAndValue() throws Exception {
        // GIVEN
        // TODO: add headers
        byte[] json = String.format("{\"topic\":\"%s\",\"partition\":%d,\"offset\":%d,\"timestamp_type\":\"%s\",\"timestamp\":%d,\"key\":null,\"value\":null}", topic, partition, offset, timestampType, timestamp).getBytes(JSON_ENCODING);
        InputStream inputStream = new ByteArrayInputStream(json);

        // WHEN
        Record actual = mapper.readValue(inputStream, Record.class);

        // THEN
        Record expected = new Record(topic, partition, null, null, offset, timestamp, timestampType);
        assertEquals(expected, actual);
    }

    @Test
    public void deserializeTestNoTimestampType() throws Exception {
        // GIVEN
        // TODO: add headers
        TimestampType noTimestampType = TimestampType.NO_TIMESTAMP_TYPE;
        byte[] json = String.format("{\"topic\":\"%s\",\"partition\":%d,\"offset\":%d,\"timestamp_type\":\"%s\",\"timestamp\":null,\"key\":null,\"value\":null}", topic, partition, offset, noTimestampType).getBytes(JSON_ENCODING);
        InputStream inputStream = new ByteArrayInputStream(json);

        // WHEN
        Record actual = mapper.readValue(inputStream, Record.class);

        // THEN
        Record expected = new Record(topic, partition, null, null, offset, null, noTimestampType);
        assertEquals(expected, actual);
    }

    @Test
    public void serializeTest() throws Exception {
        // GIVEN
        // TODO: add headers
        Record record = new Record(topic, partition, keyBytes, valueBytes, offset, timestamp, timestampType);

        // WHEN
        byte[] actual = mapper.writeValueAsBytes(record);

        // THEN
        // NOTE: here we make some (semi-dangerous) assumptions regarding
        // - deterministic key ordering, and
        // - compact formatting without white-space
        byte[] expected = String.format("{\"topic\":\"%s\",\"partition\":%d,\"offset\":%d,\"timestamp_type\":\"%s\",\"timestamp\":%d,\"key\":\"%s\",\"value\":\"%s\"}", topic, partition, offset, timestampType, timestamp, keyBase64, valueBase64).getBytes(JSON_ENCODING);
        assertArrayEquals(expected, actual);
    }

    @Test
    public void serializeTestNullKeyAndValue() throws Exception {
        // GIVEN
        // TODO: add headers
        Record record = new Record(topic, partition, null, null, offset, timestamp, timestampType);

        // WHEN
        byte[] actual = mapper.writeValueAsBytes(record);

        // THEN
        // NOTE: here we make some (semi-dangerous) assumptions regarding
        // - deterministic key ordering, and
        // - compact formatting without white-space
        byte[] expected = String.format("{\"topic\":\"%s\",\"partition\":%d,\"offset\":%d,\"timestamp_type\":\"%s\",\"timestamp\":%d,\"key\":null,\"value\":null}", topic, partition, offset, timestampType, timestamp).getBytes(JSON_ENCODING);
        assertArrayEquals(expected, actual);
    }

    @Test
    public void serializeTestNoTimestampType() throws Exception {
        // GIVEN
        // TODO: add headers
        Record record = new Record(topic, partition, null, null, offset);

        // WHEN
        byte[] actual = mapper.writeValueAsBytes(record);

        // THEN
        // NOTE: here we make some (semi-dangerous) assumptions regarding
        // - deterministic key ordering, and
        // - compact formatting without white-space
        byte[] expected = String.format("{\"topic\":\"%s\",\"partition\":%d,\"offset\":%d,\"timestamp_type\":\"%s\",\"timestamp\":null,\"key\":null,\"value\":null}", topic, partition, offset, TimestampType.NO_TIMESTAMP_TYPE).getBytes(JSON_ENCODING);
        assertArrayEquals(expected, actual);
    }
}

