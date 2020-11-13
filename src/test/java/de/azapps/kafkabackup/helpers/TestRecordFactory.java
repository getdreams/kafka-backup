package de.azapps.kafkabackup.helpers;

import de.azapps.kafkabackup.common.record.Record;
import java.nio.charset.StandardCharsets;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.header.Header;

public class TestRecordFactory {
  private static final String topic = "test-topic";
  private static final int partition = 42;
  private static final long offset = 123;
  private static final TimestampType timestampType = TimestampType.LOG_APPEND_TIME;
  private static final Long timestamp = 573831430000L;
  // encoding here is not really important, we just want some bytes
  private static final byte[] keyBytes = "test-key".getBytes(StandardCharsets.UTF_8);
  private static final byte[] valueBytes = "test-value".getBytes(StandardCharsets.UTF_8);
  // Header fixtures:
  private static final String header0Key = "header0-key";
  private static final String header1Key = "header1-key";
  private static final String header2Key = "header2-key";
  private static final byte[] header0ValueBytes = "header0-value".getBytes(StandardCharsets.UTF_8);
  private static final byte[] header1ValueBytes = "header1-value".getBytes(StandardCharsets.UTF_8);
  private static final byte[] header2ValueBytes = null;
  private static final ConnectHeaders headers = new ConnectHeaders();
  static {
    headers.add(header0Key, new SchemaAndValue(Schema.OPTIONAL_BYTES_SCHEMA, header0ValueBytes));
    headers.add(header1Key, new SchemaAndValue(Schema.OPTIONAL_BYTES_SCHEMA, header1ValueBytes));
    headers.add(header2Key, new SchemaAndValue(Schema.OPTIONAL_BYTES_SCHEMA, header2ValueBytes));
  }

  public static Record getRecord() {
    return  new Record(topic, partition, keyBytes, valueBytes, offset, timestamp, timestampType, headers);
  }

  public static Record getRecord(Iterable<Header> headers) {
    return  new Record(topic, partition, keyBytes, valueBytes, offset, timestamp, timestampType, headers);
  }

  public static Record getRecordWithOffset(long offset) {
    return  new Record(topic, partition, keyBytes, valueBytes, offset, timestamp, timestampType, headers);
  }

  public static Record getRecordWithOffsetAndTimestamp(long offset, long timestamp) {
    return  new Record(topic, partition, keyBytes, valueBytes, offset, timestamp, timestampType, headers);
  }
}
