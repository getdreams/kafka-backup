package de.azapps.kafkabackup.restore.message;

import static java.util.stream.Collectors.groupingBy;
import de.azapps.kafkabackup.common.AdminClientService;
import de.azapps.kafkabackup.common.TopicConfiguration;
import de.azapps.kafkabackup.common.TopicsConfig;
import de.azapps.kafkabackup.restore.common.RestoreArgsWrapper;
import de.azapps.kafkabackup.restore.common.RestoreConfigurationHelper;
import de.azapps.kafkabackup.restore.topic.RestoreTopicService;
import de.azapps.kafkabackup.storage.s3.AwsS3Service;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.Data;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class RestoreMessageService {

  private final AdminClientService adminClientService;
  private final AwsS3Service awsS3Service;
  ExecutorService executor;
  private final RestoreConfigurationHelper restoreConfigurationHelper;
  private final Map<String, PartitionMessageWriterWorker> partitionWriters;
  private final RestoreMessageS3Service restoreMessageS3Service;

  public RestoreMessageService(AwsS3Service awsS3Service, AdminClientService adminClientService,
      int restoreMessagesMaxThreads,
      RestoreMessageS3Service restoreMessageS3Service) {
    this.awsS3Service = awsS3Service;
    this.restoreConfigurationHelper = new RestoreConfigurationHelper(awsS3Service);
    this.adminClientService = adminClientService;
    this.executor = Executors.newFixedThreadPool(restoreMessagesMaxThreads);
    this.restoreMessageS3Service = restoreMessageS3Service;

    partitionWriters = new HashMap<>();

    log.info("RestoreMessageService initiated. Max number of threads: " + restoreMessagesMaxThreads);
  }

  public List<TopicPartitionToRestore> restoreMessages(RestoreArgsWrapper restoreArgsWrapper) {
    TopicsConfig topicsConfig = restoreConfigurationHelper.getTopicsConfig(restoreArgsWrapper.getHashToRestore(),
        restoreArgsWrapper.getConfigBackupBucket());

    List<TopicPartitionToRestore> partitionsToRestore = getPartitionsToRestore(topicsConfig,
        RestoreTopicService.getTopicsList(restoreArgsWrapper, topicsConfig));

    partitionsToRestore.stream()
        .forEach(partitionToRestore -> {
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
          infoBuilder.append(String.format("\nWorker{Id:%s, Topic:%s, Partition: %s}", worker.getIdentifier(), topicPartitionToRestore.topicConfiguration.getTopicName(),
              topicPartitionToRestore.getPartitionNumber()));
        });
      }

    if (errorWorkers.size() > 0) {
      infoBuilder.append("Error workers:");
      errorWorkers.forEach(worker -> {
        final TopicPartitionToRestore topicPartitionToRestore = worker.getTopicPartitionToRestore();
        infoBuilder.append(String.format("\nWorker{Id:%s, Topic:%s, Partition: %s}", worker.getIdentifier(), topicPartitionToRestore.topicConfiguration.getTopicName(),
            topicPartitionToRestore.getPartitionNumber()));
      });
    }

      return infoBuilder.toString();
  }

  private List<TopicPartitionToRestore> getPartitionsToRestore(TopicsConfig config, List<String> topicsToRestore) {

    Supplier<Stream<TopicConfiguration>> streamSupplier = () -> config.getTopics().stream()
        .filter(topic -> topicsToRestore.isEmpty() || topicsToRestore.contains(topic.getTopicName()));

    List<String> topicNames = streamSupplier.get().map(TopicConfiguration::getTopicName).collect(Collectors.toList());

    List<String> existingTopics = adminClientService.describeAllTopics().stream().map(TopicConfiguration::getTopicName)
        .filter(topicNames::contains)
        .collect(Collectors.toList());

    List<String> notExistingTopics = topicNames.stream().filter(topic -> !existingTopics.contains(topic))
        .collect(Collectors.toList());

    if (notExistingTopics.size() > 0) {
      log.error("Some of the topics configured to be restored does not exist in cluster" +
              " - restore has been canceled. Topics not existing in cluster: {}",
          notExistingTopics);
      throw new RuntimeException("Some of the topics configured to be restored does not exist in cluster");
    }

    return streamSupplier.get()
        .flatMap(this::mapToTopicPartitions)
        .collect(Collectors.toList());
  }

  private Stream<TopicPartitionToRestore> mapToTopicPartitions(TopicConfiguration topicConfiguration) {
    List<TopicPartitionToRestore> topicPartitions = new ArrayList<>();
    for (int i = 0; i < topicConfiguration.getPartitionsNumber(); i++) {
      topicPartitions.add(
          new TopicPartitionToRestore(topicConfiguration, i));
    }
    return topicPartitions.stream();
  }

  @Getter
  @Setter
  public static class TopicPartitionToRestore {

    final TopicConfiguration topicConfiguration;
    final int partitionNumber;
    private MessageRestorationStatus messageRestorationStatus;
    private Map<Long, RestoredMessageInfo> restoredMessageInfoMap;
    private long maxOriginalOffset = 0L;

    public TopicPartitionToRestore(TopicConfiguration topicConfiguration, int partitionNumber) {
      this.topicConfiguration = topicConfiguration;
      this.partitionNumber = partitionNumber;
      this.messageRestorationStatus = MessageRestorationStatus.WAITING;
      this.restoredMessageInfoMap = new HashMap<>();
    }

    public String getTopicPartitionId() {
      return topicConfiguration.getTopicName() + "." + partitionNumber;
    }

    public void addRestoredMessageInfo(long originalOffset, byte[] key, Long newOffset) {
      restoredMessageInfoMap.put(originalOffset, new RestoredMessageInfo(originalOffset, key, newOffset));
      maxOriginalOffset = Math.max(maxOriginalOffset, originalOffset);
    }
  }

  @Data
  public static class RestoredMessageInfo {
    private final long originalOffset;
    private final byte[] key;
    private final Long newOffset;
  }
}
