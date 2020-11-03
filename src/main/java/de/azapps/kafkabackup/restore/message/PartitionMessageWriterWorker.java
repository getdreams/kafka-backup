package de.azapps.kafkabackup.restore.message;

import de.azapps.kafkabackup.restore.message.RestoreMessageService.TopicPartitionToRestore;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
@Getter
public class PartitionMessageWriterWorker implements Runnable {

  private final TopicPartitionToRestore topicPartitionToRestore;
  private final String identifier;

  @Override
  public void run() {
    try {
      // TODO write messages to partition
      // TODO store map of old and new offsets
      // TODO prevent duplicates
      topicPartitionToRestore.setMessageRestorationStatus(MessageRestorationStatus.RUNNING);
      Thread.sleep(20000L);
      topicPartitionToRestore.setMessageRestorationStatus(MessageRestorationStatus.SUCCESS);
    } catch (RuntimeException | InterruptedException exception) {
      topicPartitionToRestore.setMessageRestorationStatus(MessageRestorationStatus.ERROR);
    }
  }
}
