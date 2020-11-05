package de.azapps.kafkabackup.common.partition.cloud;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import de.azapps.kafkabackup.common.record.Record;
import de.azapps.kafkabackup.storage.s3.AwsS3Service;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class S3PartitionWriterTest {

  public static final String TOPIC = "topic";
  private static final int PARTITION = 0;
  private static final long OFFSET = 123L;

  @Mock
  AwsS3Service awsS3Service;

  @Test
  public void whenSameRecordPutTwice_ThenOnlyOnAddedToInternalBuffer() {
    // given
    MockitoAnnotations.initMocks(this);

    S3PartitionWriter s3PartitionWriter = new S3PartitionWriter(awsS3Service,
        "bucket", new TopicPartition(TOPIC, PARTITION),
        2, 100000);

    Record testRecord = new Record(TOPIC, PARTITION, "key".getBytes(), "value".getBytes(), OFFSET);

    // when
    s3PartitionWriter.append(testRecord);
    s3PartitionWriter.append(testRecord);
    // and
    s3PartitionWriter.flush();

    // then
    verify(awsS3Service, never()).saveFile(any(), any(), any(), any());
  }

  @Test
  public void whenDifferentRecords_ThenWrittenWrittenToS3() {
    // given
    MockitoAnnotations.initMocks(this);

    S3PartitionWriter s3PartitionWriter = new S3PartitionWriter(awsS3Service,
        "bucket", new TopicPartition(TOPIC, PARTITION),
        2, 100000);

    Record testRecord = new Record(TOPIC, PARTITION, "key".getBytes(), "value".getBytes(), OFFSET);
    Record testRecord2 = new Record(TOPIC, PARTITION, "key".getBytes(), "value".getBytes(), OFFSET + 1L);

    // when
    s3PartitionWriter.append(testRecord);
    s3PartitionWriter.append(testRecord2);
    // and
    s3PartitionWriter.flush();

    // then
    verify(awsS3Service, times(1)).saveFile(any(), any(), any(), any());
  }

}
