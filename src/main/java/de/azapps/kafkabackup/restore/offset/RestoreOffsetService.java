package de.azapps.kafkabackup.restore.offset;

import com.amazonaws.services.s3.model.S3Object;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import de.azapps.kafkabackup.restore.common.RestoreArgsWrapper;
import de.azapps.kafkabackup.restore.common.RestoreConfigurationHelper.TopicPartitionToRestore;
import de.azapps.kafkabackup.storage.s3.AwsS3Service;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

@RequiredArgsConstructor
@Slf4j
public class RestoreOffsetService {


  private final AwsS3Service awsS3Service;
  private final String bucketName;
  private final RestoreArgsWrapper restoreArgsWrapper;

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
          long newOffset = getNewOffset(offsetMap, maxOriginalOffset, oldOffset);

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

        Consumer<byte[], byte[]> consumer = new KafkaConsumer<>(groupConsumerConfig);
        consumer.assign(topicPartitions);
        consumer.commitSync(cgOffsets);
        consumer.close();
      }
    });
  }

  private Long getNewOffset(Map<Long, Long> offsetMap, long maxOriginalOffset, long oldOffset) {
    if (offsetMap.containsKey(oldOffset)) {
      return offsetMap.get(oldOffset);
    } else {
      // The high watermark will not match a restored message, so we need to find the *new* high watermark
      if (oldOffset > maxOriginalOffset) {
        if (offsetMap.containsKey(maxOriginalOffset)) {
          return offsetMap.get(maxOriginalOffset) + 1;
        } else {
          throw new RuntimeException(
              "Could not find mapped offset in restored cluster, and could not map to a new high watermark either");
        }
      } else {
        return getNewOffset(offsetMap, maxOriginalOffset, oldOffset + 1);
      }
    }
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