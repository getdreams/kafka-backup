package de.azapps.kafkabackup.restore.offset;

import com.amazonaws.services.s3.model.S3Object;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import de.azapps.kafkabackup.restore.common.KafkaConsumerFactory;
import de.azapps.kafkabackup.restore.common.RestoreArgsWrapper;
import de.azapps.kafkabackup.restore.message.TopicPartitionToRestore;
import de.azapps.kafkabackup.storage.s3.AwsS3Service;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

@RequiredArgsConstructor
@Slf4j
public class RestoreOffsetService {


  private final AwsS3Service awsS3Service;
  private final String bucketName;
  private final RestoreArgsWrapper restoreArgsWrapper;
  private final OffsetMapper offsetMapper;

  public RestoreOffsetService(AwsS3Service awsS3Service, String bucketName,
      RestoreArgsWrapper restoreArgsWrapper) {
    this.awsS3Service = awsS3Service;
    this.bucketName = bucketName;
    this.restoreArgsWrapper = restoreArgsWrapper;
    this.offsetMapper = new OffsetMapper();
  }

  public void restoreOffsets(Map<String, TopicPartitionToRestore> partitionsToRestore, boolean isDryRun) {
    log.info("Restoring offsets. Dry run mode: {}", isDryRun);

    // consumergroup -> topicpartition -> (new) offset
    Map<String, Map<TopicPartition, OffsetAndMetadata>> newCGTopicPartitionOffsets = new HashMap<>();

    partitionsToRestore.forEach((id, partitionToRestore) -> {
      String topic = partitionToRestore.getTopicConfiguration().getTopicName();
      int partition = partitionToRestore.getPartitionNumber();

      // Get the cg -> (old) offset map
      Map<String, Long> oldCGOffsets = getOffsetBackups(partitionToRestore.getTopicConfiguration().getTopicName(),
          partitionToRestore.getPartitionNumber());

      oldCGOffsets.forEach((cg, oldOffset) -> {
        Map<Long, Long> offsetMap = partitionToRestore.getRestoredMessageInfoMap();
        long maxOriginalOffset = partitionToRestore.getMaxOriginalOffset();
        // Map old offset to new offset
        try {
          long newOffset = offsetMapper.getNewOffset(offsetMap, maxOriginalOffset, oldOffset);

          Map<TopicPartition, OffsetAndMetadata> tps = Optional.ofNullable(newCGTopicPartitionOffsets.get(cg))
              .orElse(new HashMap<>());
          tps.put(new TopicPartition(topic, partition), new OffsetAndMetadata(newOffset));
          // Add to list of offsets to commit
          newCGTopicPartitionOffsets.put(cg, tps);
        } catch (Exception ex) {
          log.error(
              "Error occurred when processing consumer group. Consumer group name: {}, topic name: {}, partition number: {}",
              cg, partitionToRestore.getTopicConfiguration().getTopicName(),
              partitionToRestore.getPartitionNumber());
          throw ex;
        }
      });
    });

    newCGTopicPartitionOffsets.forEach((cg, cgOffsets) -> {

      if (isDryRun) {
        log.info("Committing offsets for consumer group {} dry run mode.", cg);
        cgOffsets.forEach((tp, offset) ->
            log.info("Topic: {}, Partition: {}, Offset: {}", tp.topic(), tp.partition(), offset.offset()));

      } else {
        Set<TopicPartition> topicPartitions = cgOffsets.keySet();

        Map<String, Object> groupConsumerConfig = new HashMap<>();
        groupConsumerConfig.put("group.id", cg);
        groupConsumerConfig.put("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        groupConsumerConfig.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        groupConsumerConfig.putAll(restoreArgsWrapper.saslConfig());

        KafkaConsumer<byte[], byte[]> consumer = KafkaConsumerFactory.getFactory()
            .createConsumer(byte[].class, byte[].class, groupConsumerConfig);
        consumer.assign(topicPartitions);
        consumer.commitSync(cgOffsets);
        consumer.close();
      }
    });
  }

  private Map<String, Long> getOffsetBackups(String topic, int partition) {
    try {
      final String prefix = String.format("%s/%03d/", topic, partition);
      Optional<String> latestOffsetFile = awsS3Service.getBucketObjectKeys(bucketName, prefix, "/").stream()
          .sorted(Comparator.reverseOrder())
          .findFirst();

      if (latestOffsetFile.isPresent()) {
        S3Object obj = awsS3Service.getFile(bucketName, latestOffsetFile.get());
        ObjectMapper mapper = new ObjectMapper();
        TypeReference<HashMap<String, Long>> typeRef = new TypeReference<HashMap<String, Long>>() {
        };
        return mapper.readValue(obj.getObjectContent(), typeRef);
      } else {
        return Collections.emptyMap();
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}