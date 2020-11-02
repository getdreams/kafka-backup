package de.azapps.kafkabackup.restore;

import de.azapps.kafkabackup.restore.RestoreMessageService.TopicPartitionToRestore;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
@Getter
public class PartitionWriterWorker implements Runnable {

  private final TopicPartitionToRestore topicPartitionToRestore;
  private final String identifier;

  @Override
  public void run() {
    try {
      Thread.sleep(20000L);
    } catch (RuntimeException | InterruptedException exception) {
      topicPartitionToRestore.setMessageRestorationStatus(MessageRestorationStatus.ERROR);
    }
  }
}
