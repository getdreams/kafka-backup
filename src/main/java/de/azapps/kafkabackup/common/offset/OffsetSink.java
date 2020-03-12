package de.azapps.kafkabackup.common.offset;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ConsumerGroupListing;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsOptions;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.RetriableException;

@Slf4j
@RequiredArgsConstructor
public abstract class OffsetSink {

    private final AdminClient adminClient;
    private final long consumerGroupsMaxAgeMs;

    private Collection<String> consumerGroups = Collections.emptySet();
    private long consumerGroupsUpdatedAt = 0;

    private Collection<TopicPartition> partitions = Collections.emptySet();
    ReadWriteLock partitionsRWLock = new ReentrantReadWriteLock();
    Lock partitionsWriteLock = partitionsRWLock.writeLock();
    Lock partitionsReadLock = partitionsRWLock.readLock();

    private Collection<String> getConsumerGroups() {
        long currentTime = System.currentTimeMillis();
        if(currentTime - this.consumerGroupsUpdatedAt > consumerGroupsMaxAgeMs) {
            try {
                consumerGroups = adminClient.listConsumerGroups()
                    .all().get().stream()
                    .map(ConsumerGroupListing::groupId)
                    .collect(Collectors.toSet());

                consumerGroupsUpdatedAt = currentTime;
            } catch (InterruptedException | ExecutionException e) {
                throw new RetriableException(e);
            }
        }
        return consumerGroups;
    }

    public void setPartitions(Collection<TopicPartition> partitions) {
        partitionsWriteLock.lock();
        try {
            this.partitions = partitions;
        } finally {
            partitionsWriteLock.unlock();
        }
    }

    public void syncOffsets() {
        List<String> listOfExecutionExceptions = getConsumerGroups().stream()
            .flatMap(consumerGroup -> {
                try {
                    syncOffsetsForGroup(consumerGroup);
                    return Stream.empty();
                } catch (IOException e) {
                    return Stream.of(e);
                }
            })
            .map(Throwable::getMessage)
            .collect(Collectors.toList());

        if (!listOfExecutionExceptions.isEmpty()) {
            throw new RuntimeException("At least one exception was caught when trying to sync consumer groups offsets: "
                    + String.join("; ", listOfExecutionExceptions));
        }
    }

    private void syncOffsetsForGroup(String consumerGroup) throws IOException {
        Map<TopicPartition, OffsetAndMetadata> partitionOffsetsAndMetadata = new HashMap<>();
        partitionsReadLock.lock();
        try {
            // Due to https://issues.apache.org/jira/browse/KAFKA-9507, we need to fetch offsets one TopicPartition at a time
            // and catch the exception that occurs if the consumer group is missing from the TopicPartition's committed offsets.
            // TODO: fix when 2.4.1 is released!
            for (TopicPartition tp : partitions) {
                ListConsumerGroupOffsetsOptions listConsumerGroupOffsetsOptions = new ListConsumerGroupOffsetsOptions();
                listConsumerGroupOffsetsOptions.topicPartitions(Collections.singletonList(tp));
                try {
                    Map<TopicPartition, OffsetAndMetadata> offsetsAndMetadata = adminClient
                            .listConsumerGroupOffsets(consumerGroup, listConsumerGroupOffsetsOptions)
                            .partitionsToOffsetAndMetadata()
                            .get();
                    partitionOffsetsAndMetadata.put(tp, offsetsAndMetadata.get(tp));
                } catch (ExecutionException e) {
                    log.debug("No committed offsets for consumer group {} on topic {} partition {}", consumerGroup, tp.topic(), tp.partition());
                } catch (InterruptedException e) {
                    throw new RetriableException(e);
                }
            }
        } finally {
            partitionsReadLock.unlock();
        }
        writeOffsetsForGroup(consumerGroup, partitionOffsetsAndMetadata);
    }

    public abstract void writeOffsetsForGroup(String consumerGroup, Map<TopicPartition, OffsetAndMetadata> partitionOffsets) throws IOException;
    public abstract void flush();
    public abstract void close();
}
