package de.azapps.kafkabackup.common.partition.cloud;

import static org.junit.jupiter.api.Assertions.assertEquals;
import de.azapps.kafkabackup.common.record.JSONTest;
import de.azapps.kafkabackup.common.record.Record;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.util.List;
import lombok.SneakyThrows;
import org.junit.jupiter.api.Test;

public class S3BatchDeserializerTest extends JSONTest {

  @SneakyThrows
  @Test
  public void whenTwoMessagesInBatch_ThenTwoRecordsDeserialized() {
    //given
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();

    byteArrayOutputStream.write(jsonWithAllFields());
    byteArrayOutputStream.write(S3BatchWriter.UTF8_UNIX_LINE_FEED);
    byteArrayOutputStream.write(jsonWithAllFields());
    byteArrayOutputStream.write(S3BatchWriter.UTF8_UNIX_LINE_FEED);

    S3BatchDeserializer s3BatchDeserializer = new S3BatchDeserializer();

    InputStream is = new ByteArrayInputStream(byteArrayOutputStream.toByteArray());
    // when
    List<Record> records = s3BatchDeserializer.deserialize(is);

    assertEquals(2, records.size());
  }
}
