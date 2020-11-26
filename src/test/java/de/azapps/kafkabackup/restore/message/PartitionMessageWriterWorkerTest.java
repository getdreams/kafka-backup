package de.azapps.kafkabackup.restore.message;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import de.azapps.kafkabackup.common.TopicConfiguration;
import de.azapps.kafkabackup.common.record.Record;
import de.azapps.kafkabackup.helpers.TestRecordFactory;
import de.azapps.kafkabackup.restore.common.RestoreArgsWrapper;
import de.azapps.kafkabackup.storage.s3.AwsS3Service;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class PartitionMessageWriterWorkerTest {

  private static final String TOPIC = "TOPIC_1";
  private static final String FILE = "FILE_NAME";
  private PartitionMessageWriterWorker sut;

  LocalDateTime baseDate = LocalDateTime.of(2020, 11, 07, 11, 22, 33);

  @Mock
  private AwsS3Service awsS3Service;

  @Mock
  private RestoreArgsWrapper restoreArgsWrapper;

  @Mock
  private RestoreMessageS3Service restoreMessageS3Service;

  @Mock
  private RestoreMessageProducer restoreMessageProducer;

  @BeforeEach
  public void init() {
    TopicPartitionToRestore topicPartitionToRestore = new TopicPartitionToRestore(new TopicConfiguration(TOPIC, 1, 1),
        0);
    sut = new PartitionMessageWriterWorker(topicPartitionToRestore, restoreArgsWrapper,
        restoreMessageS3Service, restoreMessageProducer);

    when(restoreMessageS3Service.getMessageBackupFileNames(eq(TOPIC), eq(0)))
        .thenReturn(List.of(FILE));

    when(restoreMessageS3Service.readBatchFile(eq(FILE))).thenReturn(prepareRecords());
  }

  @Test
  public void shouldRestoreMessagesWhenTimestampToRestoreNotSet() {
    // GIVEN
    when(restoreArgsWrapper.getTimestampToRestore()).thenReturn(Optional.empty());

    // when
    sut.run();

    // then
    ArgumentCaptor<List<Record>> captor = ArgumentCaptor.forClass(List.class);
    verify(restoreMessageProducer, times(1)).produceRecords(captor.capture());

    assertEquals(3, captor.getValue().size());

    assertTrue(captor.getValue().stream().anyMatch(record -> record.timestamp().equals(baseDate.minusHours(1).toInstant(ZoneOffset.UTC).toEpochMilli())));
    assertTrue(captor.getValue().stream().anyMatch(record -> record.timestamp().equals(baseDate.toInstant(ZoneOffset.UTC).toEpochMilli())));
    assertTrue(captor.getValue().stream().anyMatch(record -> record.timestamp().equals(baseDate.plusHours(1).toInstant(ZoneOffset.UTC).toEpochMilli())));
  }

  @Test
  public void shouldNotRestoreNewerMessagesWhenTimestampToRestoreIsSet() {
    // GIVEN
    when(restoreArgsWrapper.getTimestampToRestore())
        .thenReturn(Optional.of(baseDate.toInstant(ZoneOffset.UTC).toEpochMilli()));

    // when
    sut.run();

    // then
    ArgumentCaptor<List<Record>> captor = ArgumentCaptor.forClass(List.class);
    verify(restoreMessageProducer, times(1)).produceRecords(captor.capture());

    assertEquals(2, captor.getValue().size());

    assertTrue(captor.getValue().stream().anyMatch(record -> record.timestamp().equals(baseDate.minusHours(1).toInstant(ZoneOffset.UTC).toEpochMilli())));
    assertTrue(captor.getValue().stream().anyMatch(record -> record.timestamp().equals(baseDate.toInstant(ZoneOffset.UTC).toEpochMilli())));
    assertFalse(captor.getValue().stream().anyMatch(record -> record.timestamp().equals(baseDate.plusHours(1).toInstant(ZoneOffset.UTC).toEpochMilli())));
  }

  private List<Record> prepareRecords() {
    return List.of(
        TestRecordFactory.getRecordWithOffsetAndTimestamp(0,
            baseDate.minusHours(1).toInstant(ZoneOffset.UTC).toEpochMilli()),
        TestRecordFactory
            .getRecordWithOffsetAndTimestamp(1, baseDate.toInstant(ZoneOffset.UTC).toEpochMilli()),
        TestRecordFactory.getRecordWithOffsetAndTimestamp(2,
            baseDate.plusHours(1).toInstant(ZoneOffset.UTC).toEpochMilli())
    );
  }

}
