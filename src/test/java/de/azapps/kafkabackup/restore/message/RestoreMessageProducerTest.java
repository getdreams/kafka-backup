package de.azapps.kafkabackup.restore.message;

import static org.junit.jupiter.api.Assertions.assertEquals;
import de.azapps.kafkabackup.common.TopicConfiguration;
import de.azapps.kafkabackup.common.record.Record;
import de.azapps.kafkabackup.helpers.TestRecordFactory;
import de.azapps.kafkabackup.restore.common.RestoreArgsWrapper;
import de.azapps.kafkabackup.restore.message.RestoreMessageService.TopicPartitionToRestore;
import java.io.File;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import lombok.SneakyThrows;
import org.junit.jupiter.api.Test;

public class RestoreMessageProducerTest {

  @Test
  public void shouldNotProduceMessagesForDuplicateInBackupBatch() {
    // GIVEN
    TopicPartitionToRestore partitionToRestore = getPartitionToRestore();
    RestoreMessageProducer restoreMessageProducer = new RestoreMessageProducer(getConfig(), partitionToRestore);
    List<Record> records = new ArrayList<>();
    records.add(TestRecordFactory.getRecordWithOffset(0L));
    records.add(TestRecordFactory.getRecordWithOffset(1L));
    records.add(TestRecordFactory.getRecordWithOffset(1L));
    records.add(TestRecordFactory.getRecordWithOffset(2L));

    // WHEN
    restoreMessageProducer.produceRecords(records);

    // THEN
    assertEquals(3, partitionToRestore.getRestoredMessageInfoMap().entrySet().size());

  }

  @Test
  public void shouldNotProduceMessagesForDuplicateInAnotherBackupFile() {
    // GIVEN
    TopicPartitionToRestore partitionToRestore = getPartitionToRestore();
    RestoreMessageProducer restoreMessageProducer = new RestoreMessageProducer(getConfig(), partitionToRestore);
    List<Record> records = new ArrayList<>();
    records.add(TestRecordFactory.getRecordWithOffset(0L));
    records.add(TestRecordFactory.getRecordWithOffset(1L));
    records.add(TestRecordFactory.getRecordWithOffset(2L));

    List<Record> secondBatch = new ArrayList<>();
    secondBatch.add(TestRecordFactory.getRecordWithOffset(2L));
    secondBatch.add(TestRecordFactory.getRecordWithOffset(3L));
    secondBatch.add(TestRecordFactory.getRecordWithOffset(4L));

    // WHEN
    restoreMessageProducer.produceRecords(records);
    restoreMessageProducer.produceRecords(secondBatch);

    // THEN
    assertEquals(5, partitionToRestore.getRestoredMessageInfoMap().entrySet().size());

  }

  private TopicPartitionToRestore getPartitionToRestore() {
    TopicConfiguration tc = new TopicConfiguration("TEST_TOPIC", 1, 1);
    return new TopicPartitionToRestore(tc, 0);
  }

  @SneakyThrows
  private RestoreArgsWrapper getConfig() {
    File test = File.createTempFile("test", ".config");
    Files.write(test.toPath(),
        ("aws.s3.region=region"
            + "\nkafka.bootstrap.servers=server1"
            + "\naws.s3.bucketNameForConfig=bucketName"
            + "\nrestore.hash=hash"
            + "\nrestore.dryRun=true"
            + "\nrestore.mode=topics,messages"
        ).getBytes());

    return RestoreArgsWrapper.of(test.getPath());
  }


}
