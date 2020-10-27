package de.azapps.kafkabackup.restore;

import com.amazonaws.services.s3.model.S3Object;
import com.fasterxml.jackson.databind.ObjectMapper;
import de.azapps.kafkabackup.common.AdminClientService;
import de.azapps.kafkabackup.common.TopicConfiguration;
import de.azapps.kafkabackup.common.TopicsConfig;
import de.azapps.kafkabackup.storage.s3.AwsS3Service;
import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.NewTopic;

@Slf4j
@RequiredArgsConstructor
class RestoreTopicService {

  private final String bucketWithTopicConfigs = "__config";
  private final AdminClientService adminClientService;
  private final AwsS3Service awsS3Service;

  private final ObjectMapper objectMapper = new ObjectMapper();

  public void restoreTopics(String configurationChecksum) {
    TopicsConfig config = getTopicsConfig(configurationChecksum);
    restoreTopics(config);

  }
  public void restoreTopics(long restorationTimestamp) {
    TopicsConfig config = getTopicsConfig(restorationTimestamp);
    restoreTopics(config);
  }

   void restoreTopics(TopicsConfig config) {
    log.info("Restoring topics configuration...");
    createTopics(config);
    log.info("All topics has been created");
  }

  private TopicsConfig getTopicsConfig(long restorationTimestamp) {
    return null;
  }

  private TopicsConfig getTopicsConfig(String configChecksum) {
    S3Object file = awsS3Service.getFile(bucketWithTopicConfigs, configChecksum + ".json");
    try {
      objectMapper.readValue(file.getObjectContent(), TopicsConfig.class);
    } catch (IOException e) {
      throw new RuntimeException("Unable to parse file: " + file.getKey());
    }
    return null;
  }


  private List<NewTopic> createTopics(TopicsConfig config) {

    List<TopicConfiguration> topics = config.getTopics();
    List<String> topicNames = topics.stream().map(TopicConfiguration::getTopicName).collect(Collectors.toList());

    List<String> existingTopics = adminClientService.describeAllTopics().stream().map(TopicConfiguration::getTopicName)
        .filter(topicNames::contains)
        .collect(Collectors.toList());

    if(existingTopics.size() > 0) {
      log.error("Some of the topics from configuration already exists - restore has been canceled. Existing topics: {}",
          existingTopics);
      throw new RuntimeException("Some of the topics from configuration already exists");
    }

    List<NewTopic> newTopicList = topics.stream().map(topicConfiguration -> {
      NewTopic newTopic = new NewTopic(topicConfiguration.getTopicName(), topicConfiguration.getPartitionsNumber(),
          (short) topicConfiguration.getReplicationFactor());

      newTopic.configs(topicConfiguration.getConfiguration());
      return newTopic;
    })
        .collect(Collectors.toList());

    adminClientService.createTopics(newTopicList);


    return newTopicList;
  }
}
