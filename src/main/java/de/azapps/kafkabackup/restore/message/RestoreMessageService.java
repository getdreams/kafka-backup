package de.azapps.kafkabackup.restore.message;

import static java.util.stream.Collectors.groupingBy;
import de.azapps.kafkabackup.restore.common.RestoreArgsWrapper;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class RestoreMessageService {
  ExecutorService executor;
  private final Map<String, PartitionMessageWriterWorker> partitionWriters;
  private final RestoreMessageS3Service restoreMessageS3Service;

  public RestoreMessageService(int restoreMessagesMaxThreads, RestoreMessageS3Service restoreMessageS3Service) {
    this.executor = Executors.newFixedThreadPool(restoreMessagesMaxThreads);
    this.restoreMessageS3Service = restoreMessageS3Service;

    partitionWriters = new HashMap<>();

    log.info("RestoreMessageService initiated. Max number of threads: " + restoreMessagesMaxThreads);
  }

  public Map<String, TopicPartitionToRestore> restoreMessages(RestoreArgsWrapper restoreArgsWrapper, Map<String,
      TopicPartitionToRestore> partitionsToRestore) {

    partitionsToRestore
        .forEach((id, partitionToRestore) -> {
          RestoreMessageProducer restoreMessageProducer = new RestoreMessageProducer(restoreArgsWrapper, partitionToRestore);
          final PartitionMessageWriterWorker worker = new PartitionMessageWriterWorker(partitionToRestore,
              restoreArgsWrapper, restoreMessageS3Service, restoreMessageProducer);
          partitionWriters.put(worker.getIdentifier(), worker);
          executor.submit(worker);
        });

    while (anyPartitionWriterWaitingOrRunning()) {
      try {
        log.info(
            "Waiting for workers to finish. Partition message workers info: " + partitionMessageWriterWorkersInfo());
        Thread.sleep(2000L);
      } catch (InterruptedException e) {
        log.error(e.getMessage());
      }
    }

    executor.shutdownNow();
    log.info("All workers finished. Partition message workers info: " + partitionMessageWriterWorkersInfo());
    return partitionsToRestore;
  }

  private boolean anyPartitionWriterWaitingOrRunning() {
    return partitionWriters.entrySet()
        .stream()
        .anyMatch(partitionWriter ->
            partitionWriter.getValue().getTopicPartitionToRestore().getMessageRestorationStatus().ordinal()
                <= MessageRestorationStatus.RUNNING.ordinal());
  }

  private String partitionMessageWriterWorkersInfo() {
    Map<MessageRestorationStatus, List<PartitionMessageWriterWorker>> workersMap = partitionWriters.values()
        .stream()
        .collect(groupingBy(worker -> worker.getTopicPartitionToRestore().getMessageRestorationStatus()));

    List<PartitionMessageWriterWorker> succeededWorkers = workersMap.getOrDefault(MessageRestorationStatus.SUCCESS,
        Collections.emptyList());
    List<PartitionMessageWriterWorker> runningWorkers = workersMap.getOrDefault(MessageRestorationStatus.RUNNING,
        Collections.emptyList());
    List<PartitionMessageWriterWorker> waitingWorkers = workersMap.getOrDefault(MessageRestorationStatus.WAITING,
        Collections.emptyList());
    List<PartitionMessageWriterWorker> errorWorkers = workersMap.getOrDefault(MessageRestorationStatus.ERROR,
        Collections.emptyList());

      final StringBuilder infoBuilder = new StringBuilder();
          infoBuilder.append(String.format("Workers count:\nALL=%d\nSUCCESS=%d\nRUNNING=%d\nWAITING=%d\nERROR=%d",
          partitionWriters.values().size(),
          succeededWorkers.size(),
          runningWorkers.size(),
          waitingWorkers.size(),
          errorWorkers.size()));

      if (runningWorkers.size() > 0) {
        infoBuilder.append("\nRunning workers:");
        runningWorkers.forEach(worker -> {
          final TopicPartitionToRestore topicPartitionToRestore = worker.getTopicPartitionToRestore();
          infoBuilder.append(String.format("\nWorker{Id:%s, Topic:%s, Partition: %s}", worker.getIdentifier(),
              topicPartitionToRestore.getTopicConfiguration().getTopicName(),
              topicPartitionToRestore.getPartitionNumber()));
        });
      }

    if (errorWorkers.size() > 0) {
      infoBuilder.append("Error workers:");
      errorWorkers.forEach(worker -> {
        final TopicPartitionToRestore topicPartitionToRestore = worker.getTopicPartitionToRestore();
        infoBuilder.append(String.format("\nWorker{Id:%s, Topic:%s, Partition: %s}", worker.getIdentifier(),
            topicPartitionToRestore.getTopicConfiguration().getTopicName(),
            topicPartitionToRestore.getPartitionNumber()));
      });
    }

      return infoBuilder.toString();
  }
}
