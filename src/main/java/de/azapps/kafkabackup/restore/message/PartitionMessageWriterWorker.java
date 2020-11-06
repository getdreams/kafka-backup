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
  private final RestoreMessageProducer restoreMessageProducer;

  public PartitionMessageWriterWorker(
      TopicPartitionToRestore topicPartitionToRestore,
      AwsS3Service awsS3Service,
      RestoreArgsWrapper restoreArgsWrapper) {
    this.topicPartitionToRestore = topicPartitionToRestore;
    this.restoreMessageS3Service = new RestoreMessageS3Service(awsS3Service,
        restoreArgsWrapper.getMessageBackupBucket());
    this.restoreArgsWrapper = restoreArgsWrapper;
    this.identifier = topicPartitionToRestore.getTopicPartitionId();
    this.restoreMessageProducer = new RestoreMessageProducer(restoreArgsWrapper, topicPartitionToRestore);
  }

  @Override
  public void run() {
    try {
      log.info("Restoring messages for topic partition {}", topicPartitionToRestore);
      topicPartitionToRestore.setMessageRestorationStatus(MessageRestorationStatus.RUNNING);

      List<String> filesToRestore = restoreMessageS3Service
          .getMessageBackupFileNames(topicPartitionToRestore.getTopicConfiguration().getTopicName(),
              topicPartitionToRestore.getPartitionNumber());

      log.debug("Got {} files to restore.", filesToRestore.size());

      filesToRestore.forEach(this::restoreBackupFile);

      topicPartitionToRestore.setMessageRestorationStatus(MessageRestorationStatus.SUCCESS);

      log.info("Message restoration succeeded for topic partition {}", topicPartitionToRestore.getTopicPartitionId());
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

    restoreMessageProducer.produceRecords(records);
  }
}
