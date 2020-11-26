package de.azapps.kafkabackup.restore.common;

import com.amazonaws.services.s3.model.S3Object;
import com.fasterxml.jackson.databind.ObjectMapper;
import de.azapps.kafkabackup.common.AdminClientService;
import de.azapps.kafkabackup.common.TopicConfiguration;
import de.azapps.kafkabackup.common.TopicsConfig;
import de.azapps.kafkabackup.restore.message.TopicPartitionToRestore;
import de.azapps.kafkabackup.restore.topic.RestoreTopicService;
import de.azapps.kafkabackup.storage.s3.AwsS3Service;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.RequiredArgsConstructor;
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
}
