package de.azapps.kafkabackup.restore.offset;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;
import com.amazonaws.services.s3.model.S3Object;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import de.azapps.kafkabackup.common.TopicConfiguration;
import de.azapps.kafkabackup.restore.common.KafkaConsumerFactory;
import de.azapps.kafkabackup.restore.common.RestoreArgsWrapper;
import de.azapps.kafkabackup.restore.message.TopicPartitionToRestore;
import de.azapps.kafkabackup.storage.s3.AwsS3Service;
import java.io.ByteArrayInputStream;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class RestoreOffsetServiceTest {

  private final static String TEST_BUCKET_NAME = "bucketName";
  private final static RestoreArgsWrapper DEFAULT_RESTORE_ARGS = RestoreArgsWrapper.builder()
      .build();

  private final static Map<String, Object> DEFAULT_CONSUMER_CONFIG = ImmutableMap.of(
      "group.id", "groupId",
      "key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer",
      "value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer"
  );

  @Mock
  private AwsS3Service awsS3Service;
  @Mock
  private OffsetMapper offsetMapper;

  private RestoreOffsetService sut;


  @BeforeEach
  public void init() {
    sut = new RestoreOffsetService(awsS3Service, TEST_BUCKET_NAME, DEFAULT_RESTORE_ARGS, offsetMapper);
  }


  @Test
  public void shouldNotCommitAnyOffsetsIfDruRunIsEnabled() {
    // given
    when(awsS3Service.getBucketObjectKeys(any(), any(), any())).thenReturn(ImmutableList.of("1", "2", "3"));
    S3Object s3Object = new S3Object();
    s3Object.setObjectContent(new ByteArrayInputStream("{\"group1\":2}".getBytes()));
    when(awsS3Service.getFile(any(), any())).thenReturn(s3Object);

    KafkaConsumerFactory kafkaConsumerFactoryMock = Mockito.mock(KafkaConsumerFactory.class);
    KafkaConsumerFactory.setFactory(kafkaConsumerFactoryMock);

    Map<String, TopicPartitionToRestore> partitionsToRestore = ImmutableMap.of(
        "topicName.1", new TopicPartitionToRestore(new TopicConfiguration("topicName", 3, 1), 1));

    // when
    sut.restoreOffsets(partitionsToRestore, true);

    // then
    verifyZeroInteractions(kafkaConsumerFactoryMock);
  }

  @Test
  public void shouldCommitProperOffsets() {
    // given
    when(awsS3Service.getBucketObjectKeys(eq(TEST_BUCKET_NAME), eq("topicName/001/"), eq("/")))
        .thenReturn(ImmutableList.of("1", "3", "2"));
    S3Object s3Object = new S3Object();
    s3Object.setObjectContent(new ByteArrayInputStream("{\"group1\":5}".getBytes()));
    when(awsS3Service.getFile(any(), any())).thenReturn(s3Object);
    when(offsetMapper.getNewOffset(any(), anyLong(), anyLong())).thenReturn(Long.valueOf(5));

    KafkaConsumerFactory kafkaConsumerFactoryMock = Mockito.mock(KafkaConsumerFactory.class);
    KafkaConsumerFactory.setFactory(kafkaConsumerFactoryMock);
    KafkaConsumer kafkaConsumer = Mockito.mock(KafkaConsumer.class);
    when(kafkaConsumerFactoryMock.createConsumer(any(), any(), any())).thenReturn(kafkaConsumer);

    Map<String, TopicPartitionToRestore> partitionsToRestore = ImmutableMap.of(
        "topicName.1", new TopicPartitionToRestore(new TopicConfiguration("topicName", 1, 1), 1));

    // when
    sut.restoreOffsets(partitionsToRestore, false);

    // then
    ArgumentCaptor<Map<TopicPartition, OffsetAndMetadata>> argumentCaptor = ArgumentCaptor.forClass(Map.class);
    verify(kafkaConsumer, times(1)).commitSync(argumentCaptor.capture());
    Map<TopicPartition, OffsetAndMetadata> offsetsMap = argumentCaptor.getValue();
    assertEquals(offsetsMap.size(), 1);
    TopicPartition topicPartition = new TopicPartition("topicName", 1);
    assertTrue(offsetsMap.containsKey(topicPartition));
    assertEquals(offsetsMap.get(topicPartition), new OffsetAndMetadata(5));
  }

  @Test
  public void shouldCommitOffsetsForProperPartition() {
    // given
    when(awsS3Service.getBucketObjectKeys(eq(TEST_BUCKET_NAME), eq("topicName/001/"), eq("/")))
        .thenReturn(ImmutableList.of("1", "3", "2"));
    S3Object s3Object = new S3Object();
    s3Object.setObjectContent(new ByteArrayInputStream("{\"group1\":5}".getBytes()));
    when(awsS3Service.getFile(any(), any())).thenReturn(s3Object);
    when(offsetMapper.getNewOffset(any(), anyLong(), anyLong())).thenReturn(Long.valueOf(5));

    KafkaConsumerFactory kafkaConsumerFactoryMock = Mockito.mock(KafkaConsumerFactory.class);
    KafkaConsumerFactory.setFactory(kafkaConsumerFactoryMock);
    KafkaConsumer kafkaConsumer = Mockito.mock(KafkaConsumer.class);
    when(kafkaConsumerFactoryMock.createConsumer(any(), any(), any())).thenReturn(kafkaConsumer);

    Map<String, TopicPartitionToRestore> partitionsToRestore = ImmutableMap.of(
        "topicName.1", new TopicPartitionToRestore(new TopicConfiguration("topicName", 1, 1), 1));
    // when

    sut.restoreOffsets(partitionsToRestore, false);

    // then
    ArgumentCaptor<Set<TopicPartition>> argumentCaptor = ArgumentCaptor.forClass(Set.class);
    verify(kafkaConsumer, times(1)).assign(argumentCaptor.capture());
    Set<TopicPartition> assignedPartitions = argumentCaptor.getValue();
    assertEquals(assignedPartitions.size(), 1);
    assertTrue(assignedPartitions.contains(new TopicPartition("topicName", 1)));
  }

  @Test
  public void shouldCommitOffsetsUsingProperConsumerGroupName() {
    // given
    when(awsS3Service.getBucketObjectKeys(eq(TEST_BUCKET_NAME), eq("topicName/001/"), eq("/")))
        .thenReturn(ImmutableList.of("1", "3", "2"));
    S3Object s3Object = new S3Object();
    s3Object.setObjectContent(new ByteArrayInputStream("{\"group1\":5}".getBytes()));
    when(awsS3Service.getFile(any(), any())).thenReturn(s3Object);
    when(offsetMapper.getNewOffset(any(), anyLong(), anyLong())).thenReturn(Long.valueOf(5));

    KafkaConsumerFactory kafkaConsumerFactoryMock = Mockito.mock(KafkaConsumerFactory.class);
    KafkaConsumerFactory.setFactory(kafkaConsumerFactoryMock);
    KafkaConsumer kafkaConsumer = Mockito.mock(KafkaConsumer.class);
    when(kafkaConsumerFactoryMock.createConsumer(any(), any(), any())).thenReturn(kafkaConsumer);

    Map<String, TopicPartitionToRestore> partitionsToRestore = ImmutableMap.of(
        "topicName.1", new TopicPartitionToRestore(new TopicConfiguration("topicName", 1, 1), 1));

    // when
    sut.restoreOffsets(partitionsToRestore, false);

    // then

    ArgumentCaptor<Map<String, Object>> argumentCaptor = ArgumentCaptor.forClass(Map.class);
    verify(kafkaConsumerFactoryMock, times(1))
        .createConsumer(eq(byte[].class), eq(byte[].class), argumentCaptor.capture());

    Map<String, Object> configMap = argumentCaptor.getValue();
    assertTrue(configMap.containsKey("group.id"));
    assertEquals(configMap.get("group.id"), "group1");
  }

  @Test
  public void shouldStopWholeRestorationProcessIfExceptionWillBeCaughtProcessingAnyConsumerGroup() {
    // given
    when(awsS3Service.getBucketObjectKeys(eq(TEST_BUCKET_NAME), eq("topicName/001/"), eq("/")))
        .thenReturn(ImmutableList.of("1", "3", "2"));
    S3Object s3Object = new S3Object();
    s3Object.setObjectContent(new ByteArrayInputStream("{\"group1\":5}".getBytes()));
    when(awsS3Service.getFile(any(), any())).thenReturn(s3Object);
    when(offsetMapper.getNewOffset(any(), anyLong(), anyLong())).thenThrow(new RuntimeException("exception message"));

    KafkaConsumerFactory kafkaConsumerFactoryMock = Mockito.mock(KafkaConsumerFactory.class);
    KafkaConsumerFactory.setFactory(kafkaConsumerFactoryMock);

    Map<String, TopicPartitionToRestore> partitionsToRestore = ImmutableMap.of(
        "topicName.1", new TopicPartitionToRestore(new TopicConfiguration("topicName", 1, 1), 1));

    // when
    RuntimeException expectedException = assertThrows(RuntimeException.class, () -> {
      sut.restoreOffsets(partitionsToRestore, false);
    });

    // then
    assertEquals(expectedException.getMessage(), "exception message");
    verifyZeroInteractions(kafkaConsumerFactoryMock);
  }
}