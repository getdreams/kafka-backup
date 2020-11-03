package de.azapps.kafkabackup.restore.topic;

import de.azapps.kafkabackup.common.AdminClientService;
import de.azapps.kafkabackup.common.TopicConfiguration;
import de.azapps.kafkabackup.common.TopicsConfig;
import de.azapps.kafkabackup.restore.common.RestoreConfigurationHelper;
import de.azapps.kafkabackup.restore.common.RestoreArgsWrapper;
import de.azapps.kafkabackup.storage.s3.AwsS3Service;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.NewTopic;

@Slf4j
public
class RestoreTopicService {

  private final AdminClientService adminClientService;
  private final AwsS3Service awsS3Service;

  private final RestoreConfigurationHelper restoreConfigurationHelper;

  public RestoreTopicService(AdminClientService adminClientService, AwsS3Service awsS3Service) {
    this.adminClientService = adminClientService;
    this.awsS3Service = awsS3Service;

    restoreConfigurationHelper = new RestoreConfigurationHelper(awsS3Service);
  }

  public void restoreTopics(RestoreArgsWrapper restoreTopicsArgsWrapper) {
    TopicsConfig topicsConfig = restoreConfigurationHelper.getTopicsConfig(restoreTopicsArgsWrapper.getHashToRestore(),
        restoreTopicsArgsWrapper.getConfigBackupBucket());

    restoreTopics(topicsConfig, restoreTopicsArgsWrapper.getTopicsToRestore(), restoreTopicsArgsWrapper.isDryRun());
  }

  private void restoreTopics(TopicsConfig config, List<String> topicsToRestore, boolean isDryRun) {
    log.info("Restoring topics. Dry run mode: {}", isDryRun);
    createTopics(config, topicsToRestore, isDryRun);
    log.info("All topics has been created");
  }

  private List<NewTopic> createTopics(TopicsConfig config, List<String> topicsToRestore, boolean isDryRun) {

    Supplier<Stream<TopicConfiguration>> streamSupplier = () -> config.getTopics().stream()
        .filter(topic -> topicsToRestore.isEmpty() || topicsToRestore.contains(topic.getTopicName()));

    List<String> topicNames = streamSupplier.get().map(TopicConfiguration::getTopicName).collect(Collectors.toList());

    List<String> existingTopics = adminClientService.describeAllTopics().stream().map(TopicConfiguration::getTopicName)
        .filter(topicNames::contains)
        .collect(Collectors.toList());

    List<String> topicsWithoutConfigBackup = topicsToRestore.stream().filter(topic -> !topicNames.contains(topic))
        .collect(Collectors.toList());

    if (topicsWithoutConfigBackup.size() > 0) {
      log.error("Some of the topics configured to be restored does not have configuration backup" +
              " - restore has been canceled. Topics missing configuration backup topics: {}",
          topicsWithoutConfigBackup);
      throw new RuntimeException("Some of the topics configured to be restored does not have configuration backup");
    }

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
