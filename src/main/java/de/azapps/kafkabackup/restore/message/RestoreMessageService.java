package de.azapps.kafkabackup.restore.message;

import static java.util.stream.Collectors.groupingBy;
import de.azapps.kafkabackup.common.AdminClientService;
import de.azapps.kafkabackup.common.TopicConfiguration;
import de.azapps.kafkabackup.common.TopicsConfig;
import de.azapps.kafkabackup.restore.common.RestoreArgsWrapper;
import de.azapps.kafkabackup.restore.common.RestoreConfigurationHelper;
import de.azapps.kafkabackup.storage.s3.AwsS3Service;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class RestoreMessageService {

  private final AdminClientService adminClientService;
  ExecutorService executor;
  private final RestoreConfigurationHelper restoreConfigurationHelper;
  private final RestoreArgsWrapper restoreArgsWrapper;
  private Map<String, PartitionMessageWriterWorker> partitionWriters;

  public RestoreMessageService(AwsS3Service awsS3Service, AdminClientService adminClientService,
      RestoreArgsWrapper restoreArgsWrapper) {
    this.restoreConfigurationHelper = new RestoreConfigurationHelper(awsS3Service);
    this.restoreArgsWrapper = restoreArgsWrapper;
    this.adminClientService = adminClientService;

    final int restoreMessagesMaxThreads = restoreArgsWrapper.getRestoreMessagesMaxThreads();
    this.executor = Executors.newFixedThreadPool(restoreMessagesMaxThreads);

    log.info("RestoreMessageService initiated. Max number of threads: " + restoreMessagesMaxThreads);
  }

  public void restoreMessages() {
    TopicsConfig topicsConfig = restoreConfigurationHelper.getTopicsConfig(restoreArgsWrapper.getHashToRestore(),
        restoreArgsWrapper.getConfigBackupBucket());

    List<TopicPartitionToRestore> partitionsToRestore = getPartitionsToRestore(topicsConfig,
        restoreArgsWrapper.getTopicsToRestore());

    partitionsToRestore.stream()
        .forEach(partitionToRestore -> {
          executor.submit(new PartitionMessageWriterWorker(partitionToRestore,
              partitionToRestore.getTopicPartitionId()));
        });

    while (anyPartitionWriterWaitingOrRunning()) {
      try {
        log.info(
            "Waiting for workers to finish. Partition message workers info: " + partitionMessageWriterWorkersInfo());
        Thread.sleep(5000L);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }

    log.info("All workers finished.Partition message workers info: " + partitionMessageWriterWorkersInfo());
  }

  private boolean anyPartitionWriterWaitingOrRunning() {
    return partitionWriters.entrySet()
        .stream()
        .anyMatch(partitionWriter ->
            partitionWriter.getValue().getTopicPartitionToRestore().getMessageRestorationStatus().ordinal()
                <= MessageRestorationStatus.RUNNING.ordinal());
  }

  private Map<MessageRestorationStatus, List<PartitionMessageWriterWorker>> partitionMessageWriterWorkersInfo() {
    return partitionWriters.values()
        .stream()
        .collect(groupingBy(worker -> worker.getTopicPartitionToRestore().getMessageRestorationStatus()));
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

  @RequiredArgsConstructor
  @Getter
  @Setter
  public static class TopicPartitionToRestore {

    final TopicConfiguration topicConfiguration;
    final int partitionNumber;
    private MessageRestorationStatus messageRestorationStatus;

    public String getTopicPartitionId() {
      return topicConfiguration.getTopicName() + "." + partitionNumber;
    }
  }
}
