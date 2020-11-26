package de.azapps.kafkabackup.restore.message;

import de.azapps.kafkabackup.common.record.Record;
import de.azapps.kafkabackup.restore.common.RestoreArgsWrapper;
import de.azapps.kafkabackup.restore.common.RestoreConfigurationHelper.TopicPartitionToRestore;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
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
      RestoreArgsWrapper restoreArgsWrapper,
      RestoreMessageS3Service restoreMessageS3Service,
      RestoreMessageProducer restoreMessageProducer) {
    this.topicPartitionToRestore = topicPartitionToRestore;
    this.restoreMessageS3Service = restoreMessageS3Service;
    this.restoreArgsWrapper = restoreArgsWrapper;
    this.identifier = topicPartitionToRestore.getTopicPartitionId();
    this.restoreMessageProducer = restoreMessageProducer;
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

      restoreMessageProducer.initiateProducer(topicPartitionToRestore, restoreArgsWrapper);

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

    final Optional<Long> timestampToRestore = this.restoreArgsWrapper.getTimestampToRestore();

    if (timestampToRestore.isPresent()) {
      List<Record> filtered = records.stream().filter(record -> record.timestamp() <= timestampToRestore.get()).collect(
          Collectors.toList());
      if (filtered.size() == 0) {
        log.info("All messages in batch newer than timestamp to restore: {}", timestampToRestore.get());
        return;
      }

      restoreMessageProducer.produceRecords(filtered);
    }
    else {
      restoreMessageProducer.produceRecords(records);
    }
  }
}
