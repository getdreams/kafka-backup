package de.azapps.kafkabackup.common.record;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;

import de.azapps.kafkabackup.common.partition.cloud.S3BatchDeserializer;
import de.azapps.kafkabackup.common.partition.cloud.S3BatchWriter;
import java.util.List;
import lombok.SneakyThrows;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;

public class RecordJSONSerdeTest extends JSONTest {
    // Service under test:
    private static RecordJSONSerde sutSerde;

    @BeforeEach
    public void beforeEach() {
        sutSerde = new RecordJSONSerde();
    }

    @Test
    public void readTest() throws Exception {
        // GIVEN
        InputStream inputStream = new ByteArrayInputStream(jsonWithAllFields());

        // WHEN
        Record actual = sutSerde.read(inputStream);

        // THEN
        Record expected = new Record(topic, partition, keyBytes, valueBytes, offset, timestamp, timestampType, headers);
        assertEquals(expected, actual);
    }

    @Test
    public void writeTest() throws Exception {
        // GIVEN
        Record record = new Record(topic, partition, keyBytes, valueBytes, offset, timestamp, timestampType, headers);

        // WHEN
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        sutSerde.write(outputStream, record);
        byte[] actual = outputStream.toByteArray();

        // THEN
        byte[] expected = jsonWithAllFields();
        assertArrayEquals(expected, actual);
    }

    @SneakyThrows
    @Test
    public void readAllTest() {
        // GIVEN
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();

        byteArrayOutputStream.write(jsonWithAllFields());
        byteArrayOutputStream.write(S3BatchWriter.UTF8_UNIX_LINE_FEED);
        byteArrayOutputStream.write(jsonWithAllFields());
        byteArrayOutputStream.write(S3BatchWriter.UTF8_UNIX_LINE_FEED);

        InputStream is = new ByteArrayInputStream(byteArrayOutputStream.toByteArray());
        // WHEN
        List<Record> records = sutSerde.readAll(is);

        // THEN
        assertEquals(2, records.size());
        Record expected = new Record(topic, partition, keyBytes, valueBytes, offset, timestamp, timestampType, headers);
        assertEquals(expected, records.get(0));
        assertEquals(expected, records.get(1));
    }
}

