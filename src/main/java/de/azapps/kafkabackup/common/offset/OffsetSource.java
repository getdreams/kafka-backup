package de.azapps.kafkabackup.common.offset;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import de.azapps.kafkabackup.common.partition.PartitionIndex;
import de.azapps.kafkabackup.common.partition.PartitionReader;
import de.azapps.kafkabackup.common.segment.SegmentIndex;
import de.azapps.kafkabackup.sink.BackupSinkTask;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class OffsetSource {
    private static final Logger log = LoggerFactory.getLogger(OffsetSource.class);
    private Map<TopicPartition, OffsetStoreFile> topicOffsets = new HashMap<>();
    private Map<String, Object> consumerConfig;

    public OffsetSource(Path backupDir, List<String> topics, Map<String, Object> consumerConfig) throws IOException {
        this.consumerConfig = consumerConfig;
        for (String topic : topics) {
            findOffsetStores(backupDir, topic);
        }
    }

    private void findOffsetStores(Path backupDir, String topic) throws IOException {
        Path topicDir = Paths.get(backupDir.toString(), topic);
        for (Path f : Files.list(topicDir).collect(Collectors.toList())) {
            Optional<Integer> partition = OffsetUtils.isOffsetStoreFile(f);
            if (partition.isPresent()) {
                TopicPartition topicPartition = new TopicPartition(topic, partition.get());
                topicOffsets.put(topicPartition, new OffsetStoreFile(f));
            }
        }
    }

    public void syncGroupForOffset(TopicPartition topicPartition, long sourceOffset, long targetOffset) throws IOException {
        OffsetStoreFile offsetStoreFile = topicOffsets.get(topicPartition);
        List<String> groups;
        if (offsetStoreFile != null) {
            // __consumer_offsets contains the offset of the message to read next. So we need to search for the offset + 1
            // if we do not do that we might miss
            groups = offsetStoreFile.groupForOffset(sourceOffset + 1);
        } else {
            // This is normal: if no consumer group was backed up for this topic partition the topicOffsets map returns null.
            groups = Collections.emptyList();
        }
        if (groups != null && groups.size() > 0) {
            for (String group : groups) {
                Map<String, Object> groupConsumerConfig = new HashMap<>(consumerConfig);
                groupConsumerConfig.put("group.id", group);
                Consumer<byte[], byte[]> consumer = new KafkaConsumer<>(groupConsumerConfig);
                consumer.assign(Collections.singletonList(topicPartition));
                // ! Target Offset + 1 as we commit the offset of the "next message to read"
                OffsetAndMetadata offsetAndMetadata = new OffsetAndMetadata(targetOffset + 1);
                Map<TopicPartition, OffsetAndMetadata> offsets = Collections.singletonMap(topicPartition, offsetAndMetadata);
                consumer.commitSync(offsets);
                consumer.close();
                log.info("Committed target offset " + (targetOffset + 1) + " for group " + group + " for topic " + topicPartition.topic() + " partition " + topicPartition.partition());
            }
        }
    }

    private static class OffsetStoreFile {
        private Map<Long, List<String>> offsetGroups = new HashMap<>();

        TypeReference<HashMap<String,Long>> typeRef
                = new TypeReference<HashMap<String,Long>>() {};


        OffsetStoreFile(Path storeFile) throws IOException {
            ObjectMapper mapper = new ObjectMapper();
            Map<String, Long> groupOffsets = mapper.readValue(storeFile.toFile(), typeRef);
            for (String group : groupOffsets.keySet()) {
                Long offset = groupOffsets.get(group);
                if (offsetGroups.containsKey(offset)) {
                    List<String> groups = offsetGroups.get(offset);
                    groups.add(group);
                } else {
                    List<String> groups = new ArrayList<>(1);
                    groups.add(group);
                    offsetGroups.put(offset, groups);
                }
            }
        }

        List<String> groupForOffset(Long offset) {
            return offsetGroups.get(offset);
        }
    }

}

