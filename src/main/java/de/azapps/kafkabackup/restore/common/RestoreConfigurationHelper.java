package de.azapps.kafkabackup.restore.common;

import com.amazonaws.services.s3.model.S3Object;
import com.fasterxml.jackson.databind.ObjectMapper;
import de.azapps.kafkabackup.common.AdminClientService;
import de.azapps.kafkabackup.common.TopicConfiguration;
import de.azapps.kafkabackup.common.TopicsConfig;
import de.azapps.kafkabackup.restore.message.MessageRestorationStatus;
import de.azapps.kafkabackup.restore.topic.RestoreTopicService;
import de.azapps.kafkabackup.storage.s3.AwsS3Service;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

@RequiredArgsConstructor
@Slf4j
public class RestoreConfigurationHelper {

  private final AwsS3Service awsS3Service;
  private final AdminClientService adminClientService;
  private final ObjectMapper objectMapper = new ObjectMapper();

  public TopicsConfig getTopicsConfig(String configChecksum, String bucketWithTopicConfigs) {
    S3Object file = awsS3Service.getFile(bucketWithTopicConfigs, configChecksum + ".json");
    try {
      return objectMapper.readValue(file.getObjectContent(), TopicsConfig.class);
    } catch (IOException e) {
      throw new RuntimeException("Unable to parse file: " + file.getKey(), e);
    }
  }

  public Map<String, TopicPartitionToRestore> getPartitionsToRestore(RestoreArgsWrapper restoreArgsWrapper) {

    TopicsConfig topicsConfig = getTopicsConfig(restoreArgsWrapper.getHashToRestore(),
        restoreArgsWrapper.getConfigBackupBucket());

    List<String> topicsToRestore = RestoreTopicService.getTopicsList(restoreArgsWrapper, topicsConfig);
    Supplier<Stream<TopicConfiguration>> streamSupplier = () -> topicsConfig.getTopics().stream()
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
        .collect(Collectors.toMap(TopicPartitionToRestore::getTopicPartitionId, Function.identity()));
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
    private Map<Long, Long> restoredMessageInfoMap;
    private long maxOriginalOffset = -1L;

    public TopicPartitionToRestore(TopicConfiguration topicConfiguration, int partitionNumber) {
      this.topicConfiguration = topicConfiguration;
      this.partitionNumber = partitionNumber;
      this.messageRestorationStatus = MessageRestorationStatus.WAITING;
      this.restoredMessageInfoMap = new HashMap<>();
      // Start out by always mapping offset 0 to offset 0 (for empty topics)
      addRestoredMessageInfo(0L, 0L);
    }

    public String getTopicPartitionId() {
      return topicConfiguration.getTopicName() + "." + partitionNumber;
    }

    public void addRestoredMessageInfo(long originalOffset, Long newOffset) {
      restoredMessageInfoMap.put(originalOffset,newOffset);
      maxOriginalOffset = Math.max(maxOriginalOffset, originalOffset);
    }
  }
}
