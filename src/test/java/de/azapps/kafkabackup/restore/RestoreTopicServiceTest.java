package de.azapps.kafkabackup.restore;

import static de.azapps.kafkabackup.common.topic.restore.RestoreArgsWrapper.ALL_TOPICS_REGEX;
import static de.azapps.kafkabackup.common.topic.restore.RestoreArgsWrapper.NONE_TOPICS_REGEX;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import com.amazonaws.services.s3.model.S3Object;
import de.azapps.kafkabackup.common.AdminClientService;
import de.azapps.kafkabackup.common.TopicConfiguration;
import de.azapps.kafkabackup.common.TopicsConfig;
import de.azapps.kafkabackup.restore.common.RestoreArgsWrapper;
import de.azapps.kafkabackup.restore.topic.RestoreTopicService;
import de.azapps.kafkabackup.common.topic.restore.TopicsListMode;
import de.azapps.kafkabackup.storage.s3.AwsS3Service;
import java.io.ByteArrayInputStream;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.kafka.clients.admin.NewTopic;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class RestoreTopicServiceTest {
  @Mock
  AdminClientService adminClientService;
  @Mock
  AwsS3Service awsS3Service;

  private RestoreTopicService sut;

  @BeforeEach
  public void init() {
    sut = new RestoreTopicService(adminClientService, awsS3Service);
  }

  @Test
  public void shouldRestoreAllTopics() {
    // given
    TopicConfiguration topicConfigurationForTopic1 = new TopicConfiguration("topic1", 3, 3);
    topicConfigurationForTopic1.setConfiguration(Map.of("property1", "value1"));

    TopicConfiguration topicConfigurationForTopic2 = new TopicConfiguration("topic2", 3, 3);
    topicConfigurationForTopic2.setConfiguration(Map.of("property1", "value1"));
    TopicsConfig topicsConfig = TopicsConfig
        .of(List.of(topicConfigurationForTopic1, topicConfigurationForTopic2));

    S3Object s3Object = new S3Object();
    s3Object.setObjectContent(new ByteArrayInputStream(topicsConfig.toJson().getBytes()));
    when(awsS3Service.getFile(any(), any())).thenReturn(s3Object);

    RestoreArgsWrapper restoreArgsWrapper = RestoreArgsWrapper.builder()
        .topicsAllowListRegex(ALL_TOPICS_REGEX)
        .topicsDenyListRegex(NONE_TOPICS_REGEX)
        .build();


    // when
    sut.restoreTopics(restoreArgsWrapper);

    // then
    ArgumentCaptor<List<NewTopic>> newTopics = ArgumentCaptor.forClass(List.class);
    verify(adminClientService, times(1)).createTopics(newTopics.capture());


    assertEquals(newTopics.getValue().size(), 2);
    List<String> namesOfCreatedTopics = newTopics.getValue().stream().map(NewTopic::name).collect(Collectors.toList());
    assertTrue(namesOfCreatedTopics.contains("topic1"));
    assertTrue(namesOfCreatedTopics.contains("topic2"));
  }

  @Test
  public void shouldRestoreOnlyTopicFromConfiguration() {
    // given
    TopicConfiguration topicConfiguration = new TopicConfiguration("topic1", 3, 3);
    topicConfiguration.setConfiguration(Map.of("property1", "value1"));
    TopicsConfig topicsConfig = TopicsConfig
        .of(List.of(topicConfiguration));

    S3Object s3Object = new S3Object();
    s3Object.setObjectContent(new ByteArrayInputStream(topicsConfig.toJson().getBytes()));
    when(awsS3Service.getFile(any(), any())).thenReturn(s3Object);

    RestoreArgsWrapper restoreArgsWrapper = RestoreArgsWrapper.builder()
        .topicsAllowListRegex("topic1")
        .topicsDenyListRegex(NONE_TOPICS_REGEX)
        .build();


    // when
    sut.restoreTopics(restoreArgsWrapper);

    // then
    ArgumentCaptor<List<NewTopic>> newTopics = ArgumentCaptor.forClass(List.class);
    verify(adminClientService, times(1)).createTopics(newTopics.capture());

    assertEquals(newTopics.getValue().size(), 1);
    NewTopic resultNewTopic = newTopics.getValue().get(0);
    assertEquals(resultNewTopic.name(), "topic1");
    assertEquals(resultNewTopic.numPartitions(), 3);
    assertEquals(resultNewTopic.replicationFactor(), 3);
    assertEquals(resultNewTopic.configs(), Map.of("property1", "value1"));
  }

  @Test
  public void shouldOmitTopicsFromDenyListDuringRestoration() {
    // given
    TopicConfiguration topicConfigurationForTopic1 = new TopicConfiguration("topic1", 3, 3);
    topicConfigurationForTopic1.setConfiguration(Map.of("property1", "value1"));

    TopicConfiguration topicConfigurationForTopic2 = new TopicConfiguration("topic2", 3, 3);
    topicConfigurationForTopic2.setConfiguration(Map.of("property1", "value1"));
    TopicsConfig topicsConfig = TopicsConfig
        .of(List.of(topicConfigurationForTopic1, topicConfigurationForTopic2));

    S3Object s3Object = new S3Object();
    s3Object.setObjectContent(new ByteArrayInputStream(topicsConfig.toJson().getBytes()));
    when(awsS3Service.getFile(any(), any())).thenReturn(s3Object);

    RestoreArgsWrapper restoreArgsWrapper = RestoreArgsWrapper.builder()
        .topicsAllowListRegex(ALL_TOPICS_REGEX)
        .topicsDenyListRegex("topic1")
        .build();


    // when
    sut.restoreTopics(restoreArgsWrapper);

    // then
    ArgumentCaptor<List<NewTopic>> newTopics = ArgumentCaptor.forClass(List.class);
    verify(adminClientService, times(1)).createTopics(newTopics.capture());


    assertEquals(newTopics.getValue().size(), 1);
    List<String> namesOfCreatedTopics = newTopics.getValue().stream().map(NewTopic::name).collect(Collectors.toList());
    assertTrue(namesOfCreatedTopics.contains("topic2"));
  }

  @Test
  public void shouldNotCallCreateTopicsOnKafkaWhenInDryRun() {
    // given
    TopicConfiguration topicConfiguration = new TopicConfiguration("topic1", 3, 3);
    topicConfiguration.setConfiguration(Map.of("property1", "value1"));
    TopicsConfig topicsConfig = TopicsConfig
        .of(List.of(topicConfiguration));

    S3Object s3Object = new S3Object();
    s3Object.setObjectContent(new ByteArrayInputStream(topicsConfig.toJson().getBytes()));
    when(awsS3Service.getFile(any(), any())).thenReturn(s3Object);

    RestoreArgsWrapper restoreArgsWrapper = RestoreArgsWrapper.builder()
        .topicsAllowListRegex(ALL_TOPICS_REGEX)
        .topicsDenyListRegex(NONE_TOPICS_REGEX)
        .isDryRun(true)
        .build();


    // when
    sut.restoreTopics(restoreArgsWrapper);

    // then
    verify(adminClientService, times(0)).createTopics(anyList());
  }

  @Test
  public void shouldThrowExceptionIfTopicFromConfigurationAlreadyExists() {
    // given
    TopicConfiguration topicConfiguration = new TopicConfiguration("topic1", 3, 3);
    topicConfiguration.setConfiguration(Map.of("property1", "value1"));
    TopicsConfig topicsConfig = TopicsConfig
        .of(List.of(topicConfiguration));

    S3Object s3Object = new S3Object();
    s3Object.setObjectContent(new ByteArrayInputStream(topicsConfig.toJson().getBytes()));
    when(awsS3Service.getFile(any(), any())).thenReturn(s3Object);

    RestoreArgsWrapper restoreArgsWrapper = RestoreArgsWrapper.builder()
        .topicsAllowListRegex("topic1")
        .topicsDenyListRegex(NONE_TOPICS_REGEX)
        .build();

    when(adminClientService.describeAllTopics()).thenReturn(List.of(topicConfiguration));

    // when
    RuntimeException runtimeException = assertThrows(RuntimeException.class,
        () -> sut.restoreTopics(restoreArgsWrapper));

    // then
    assertEquals(runtimeException.getMessage(), "Some of the topics from configuration already exists");
  }
}