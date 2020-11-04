package de.azapps.kafkabackup.restore;

import com.amazonaws.services.s3.model.S3Object;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import de.azapps.kafkabackup.common.AdminClientService;
import de.azapps.kafkabackup.common.TopicConfiguration;
import de.azapps.kafkabackup.common.TopicsConfig;
import de.azapps.kafkabackup.common.topic.restore.RestoreArgsWrapper;
import de.azapps.kafkabackup.storage.s3.AwsS3Service;
import java.io.IOException;
import java.util.List;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.NewTopic;

@Slf4j
@RequiredArgsConstructor
class RestoreTopicService {

  private final AdminClientService adminClientService;
  private final AwsS3Service awsS3Service;

  private final ObjectMapper objectMapper = new ObjectMapper();

  public void restoreTopics(RestoreArgsWrapper restoreTopicsArgsWrapper) {
    TopicsConfig topicsConfig = getTopicsConfig(restoreTopicsArgsWrapper.getHashToRestore(),
        restoreTopicsArgsWrapper.getConfigBackupBucket());

    restoreTopics(topicsConfig, getTopicsList(restoreTopicsArgsWrapper, topicsConfig),
        restoreTopicsArgsWrapper.isDryRun());
  }

  private List<String> getTopicsList(RestoreArgsWrapper restoreArgsWrapper, TopicsConfig config) {
    List<String> topicsFromConfig = config.getTopics().stream()
        .map(TopicConfiguration::getTopicName)
        .collect(Collectors.toList());

    String topicAllowRegex = restoreArgsWrapper.getTopicsAllowListRegex();
    String topicDenyRegex = restoreArgsWrapper.getTopicsDenyListRegex();

    return topicsFromConfig.stream()
        .filter(topicName -> topicName.matches(topicAllowRegex))
        .filter(topicName -> !topicName.matches(topicDenyRegex))
        .collect(Collectors.toList());

  }

  private void restoreTopics(TopicsConfig config, List<String> topicsToRestore, boolean isDryRun) {
    log.info("Restoring topics. Dry run mode: {}", isDryRun);
    createTopics(config, topicsToRestore, isDryRun);
    log.info("All topics has been created");
  }

  private TopicsConfig getTopicsConfig(String configChecksum, String bucketWithTopicConfigs) {
    S3Object file = awsS3Service.getFile(bucketWithTopicConfigs, configChecksum + ".json");
    try {
      return objectMapper.readValue(file.getObjectContent(), TopicsConfig.class);
    } catch (IOException e) {
      throw new RuntimeException("Unable to parse file: " + file.getKey(), e);
    }
  }


  private List<NewTopic> createTopics(TopicsConfig config, List<String> topicsToRestore, boolean isDryRun) {

    Supplier<Stream<TopicConfiguration>> streamSupplier = () -> config.getTopics().stream()
        .filter(topic -> topicsToRestore == null || topicsToRestore.contains(topic.getTopicName()));

    List<String> topicNames = streamSupplier.get().map(TopicConfiguration::getTopicName).collect(Collectors.toList());

    List<String> existingTopics = adminClientService.describeAllTopics().stream().map(TopicConfiguration::getTopicName)
        .filter(topicNames::contains)
        .collect(Collectors.toList());

    if (existingTopics.size() > 0) {
      log.error("Some of the topics from configuration already exists - restore has been canceled. Existing topics: {}",
          existingTopics);
      throw new RuntimeException("Some of the topics from configuration already exists");
    }

    List<NewTopic> newTopicList = streamSupplier.get().map(topicConfiguration -> {
      NewTopic newTopic = new NewTopic(topicConfiguration.getTopicName(), topicConfiguration.getPartitionsNumber(),
          (short) topicConfiguration.getReplicationFactor());

      newTopic.configs(topicConfiguration.getConfiguration());
      return newTopic;
    })
        .collect(Collectors.toList());

    if (!isDryRun) {
      adminClientService.createTopics(newTopicList);
    } else {
      log.info("DryRun mode. Topics to be created: \n{}", newTopicList);
    }

    return newTopicList;
  }
}
