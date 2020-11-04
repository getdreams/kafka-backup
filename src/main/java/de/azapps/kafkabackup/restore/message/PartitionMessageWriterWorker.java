package de.azapps.kafkabackup.restore.message;

import de.azapps.kafkabackup.common.record.Record;
import de.azapps.kafkabackup.restore.common.RestoreArgsWrapper;
import de.azapps.kafkabackup.restore.message.RestoreMessageService.TopicPartitionToRestore;
import de.azapps.kafkabackup.storage.s3.AwsS3Service;
import java.util.List;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Getter
@Slf4j
public class PartitionMessageWriterWorker implements Runnable {

  private final TopicPartitionToRestore topicPartitionToRestore;
  private final RestoreMessageS3Service restoreMessageS3Service;
  private final RestoreArgsWrapper restoreArgsWrapper;
  private final String identifier;

  public PartitionMessageWriterWorker(
      TopicPartitionToRestore topicPartitionToRestore,
      AwsS3Service awsS3Service,
      RestoreArgsWrapper restoreArgsWrapper) {
    this.topicPartitionToRestore = topicPartitionToRestore;
    this.restoreMessageS3Service = new RestoreMessageS3Service(awsS3Service, "ADD_BUCKET_TO_RESTORE_CONFIG");
    this.restoreArgsWrapper = restoreArgsWrapper;
    this.identifier = topicPartitionToRestore.getTopicPartitionId();
  }

  @Override
  public void run() {
    try {
      log.info("Restoring topic partition {}", topicPartitionToRestore);
      topicPartitionToRestore.setMessageRestorationStatus(MessageRestorationStatus.RUNNING);
      // get messages from S3
      List<String> filesToRestore = restoreMessageS3Service
          .getMessageBackupFileNames(topicPartitionToRestore.getTopicConfiguration().getTopicName(),
              topicPartitionToRestore.getPartitionNumber());

      log.debug("Got {} files to restore.", filesToRestore.size());
      // TODO write messages to partition
      filesToRestore.forEach(this::restoreBackupFile);

      // return map of offsets - maybe add it to TopicPartitionToRestore

      topicPartitionToRestore.setMessageRestorationStatus(MessageRestorationStatus.SUCCESS);
    } catch (RuntimeException ex) {
      log.error(String.format("Message restoration for topic partition %s failed",
          topicPartitionToRestore.getTopicPartitionId()),
          ex);
      topicPartitionToRestore.setMessageRestorationStatus(MessageRestorationStatus.ERROR);
    }
  }

  private void restoreBackupFile(String backupFileKey) {
    List<Record> records = restoreMessageS3Service.readBatchFile(backupFileKey);
    log.debug("Got {} messages from batch file {}.", records.size(), backupFileKey);

    produceRecords(records);
  }

  private void produceRecords(List<Record> records) {
    // TODO check in offset map if record is not duplicate
    // produce record
    // add record key and offset to offset map
  }
}
