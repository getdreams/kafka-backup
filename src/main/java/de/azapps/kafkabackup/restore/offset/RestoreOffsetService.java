package de.azapps.kafkabackup.restore.offset;

import com.amazonaws.services.s3.model.S3Object;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import de.azapps.kafkabackup.restore.message.RestoreMessageService.TopicPartitionToRestore;
import de.azapps.kafkabackup.restore.message.RestoreMessageService.RestoredMessageInfo;
import de.azapps.kafkabackup.storage.s3.AwsS3Service;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

@RequiredArgsConstructor
public class RestoreOffsetService {


  private final AwsS3Service awsS3Service;
  private final String bucketName;
  private final String targetBootstrapServers;

  //  What we have:
//
//      - List of (topics, partition) to restore. Containing:
//      - Offset mapping: (Old) Offset -> (New) Offset
//
//
//  Translate function:
//  // The S3 stuff
//
//  CG_Map: consumer group -> List<(Topic, Partition, Offset)>
//
//  For each (topic, partition):
//      - Download the offset.json file
//    - Use the offset mapping to translate to new offsets
//    - Populate the CG_Map
//
//
//
//  Restoration function:
//  // The kafka stuff
//
//  For each cg in CG_Map:
//      - Create consumer with this CG_Map
//    - commit all the topic, partition, offsets


  public void restoreOffsets(List<TopicPartitionToRestore> partitionsToRestore) {

    // consumergroup -> topicpartition -> (new) offset
    Map<String, Map<TopicPartition, OffsetAndMetadata>> newCGTopicPartitionOffsets = new HashMap<>();

    // For each (topic, partition)
    partitionsToRestore.forEach(partitionToRestore -> {
      String topic = partitionToRestore.getTopicConfiguration().getTopicName();
      int partition = partitionToRestore.getPartitionNumber();

      // Get the cg -> (old) offset map
      Map<String, Long> oldCGOffsets = getOffsetBackups(partitionToRestore.getTopicConfiguration().getTopicName(),
          partitionToRestore.getPartitionNumber());

      // For each consumer group
      oldCGOffsets.entrySet().forEach(entry -> {
        String cg = entry.getKey();
        Long oldOffset = entry.getValue();
        Map<Long, RestoredMessageInfo> offsetMap = partitionToRestore.getRestoredMessageInfoMap();
        long maxOriginalOffset = partitionToRestore.getMaxOriginalOffset();
        // Map old offset to new offset
        long newOffset = getNewOffset(offsetMap, maxOriginalOffset, oldOffset);
        Map<TopicPartition, OffsetAndMetadata> tps = Optional.ofNullable(newCGTopicPartitionOffsets.get(cg))
            .orElse(Collections.emptyMap());
        tps.put(new TopicPartition(topic, partition), new OffsetAndMetadata(newOffset));
        // Add to list of offsets to commit
        newCGTopicPartitionOffsets.put(cg, tps);
      });
    });
    // Now we have all the consumer groups to restore, with their new offsets
    // For each consumer group
    newCGTopicPartitionOffsets.entrySet().forEach(entry -> {
      String cg = entry.getKey();
      Map<TopicPartition, OffsetAndMetadata> cgOffset = entry.getValue();
      Set<TopicPartition> assignment = cgOffset.keySet();
      // Create a consumer

      Map<String, Object> groupConsumerConfig = new HashMap<>();
      groupConsumerConfig.put("group.id", cg);
      groupConsumerConfig.put("cluster.bootstrap.servers", targetBootstrapServers);
      groupConsumerConfig.put("key.deserializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
      groupConsumerConfig.put("value.deserialier", "org.apache.kafka.common.serialization.ByteArraySerializer");
      Consumer<byte[], byte[]> consumer = new KafkaConsumer<>(groupConsumerConfig);
      // Assign it the topic partitions
      consumer.assign(assignment);
      // Commit the offsets
      consumer.commitSync(cgOffset);
      consumer.close();
    });
  }

  private Long getNewOffset(Map<Long, RestoredMessageInfo> offsetMap, long maxOriginalOffset, long oldOffset) {
    if (offsetMap.containsKey(oldOffset)) {
      return offsetMap.get(oldOffset).getNewOffset();
    } else {
      // The high watermark will not match a restored message, so we need to find the *new* high watermark
      if (oldOffset > maxOriginalOffset) {
        if (offsetMap.containsKey(maxOriginalOffset)) {
          return offsetMap.get(maxOriginalOffset).getNewOffset() + 1;
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