package de.azapps.kafkabackup.sink;

import de.azapps.kafkabackup.common.offset.OffsetSink;
import de.azapps.kafkabackup.common.partition.PartitionIndex;
import de.azapps.kafkabackup.common.partition.PartitionWriter;
import de.azapps.kafkabackup.common.record.Record;
import de.azapps.kafkabackup.common.segment.SegmentIndex;
import de.azapps.kafkabackup.common.segment.SegmentWriter;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class BackupSinkTask extends SinkTask {

  private static final Logger log = LoggerFactory.getLogger(BackupSinkTask.class);
  private Path targetDir;
  private Map<TopicPartition, PartitionWriter> partitionWriters = new HashMap<>();
  private long maxSegmentSize;
  private OffsetSink offsetSink;

  @Override
  public String version() {
    return "0.1";
  }

  @Override
  public void start(Map<String, String> props) {
    BackupSinkConfig config = new BackupSinkConfig(props);
    log.info("Initialized backupSink props: {}", props);
    config.logAll();
    try {
      targetDir = Paths.get(config.targetDir());
      maxSegmentSize = config.maxSegmentSize();
      Files.createDirectories(targetDir);

      // Setup OffsetSink
      Map<String, Object> adminConfig = config.adminConfig();
      log.info("Initialized AdminClient: {}", adminConfig);
      AdminClient adminClient = AdminClient.create(adminConfig);
      offsetSink = new OffsetSink(adminClient, targetDir);
      log.info("Initialized BackupSinkTask with target dir {}", targetDir);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void put(Collection<SinkRecord> records) {
    try {
      for (SinkRecord sinkRecord : records) {
        log.info("Start backing up topic {}, Pogsartition {}, up to offset {}",
            sinkRecord.topic(), sinkRecord.kafkaPartition(), sinkRecord.kafkaOffset());
        TopicPartition topicPartition = new TopicPartition(sinkRecord.topic(), sinkRecord.kafkaPartition());
        PartitionWriter partition = partitionWriters.get(topicPartition);
        partition.append(Record.fromSinkRecord(sinkRecord));
        log.info("Backed up Topic {}, Partition {}, up to offset {}", sinkRecord.topic(), sinkRecord.kafkaPartition(),
            sinkRecord.kafkaOffset());
      }
      // Todo: refactor to own worker. E.g. using the scheduler of MM2
      offsetSink.syncConsumerGroups();
      offsetSink.syncOffsets();
    } catch (IOException | SegmentIndex.IndexException | PartitionIndex.IndexException | SegmentWriter.SegmentException e) {
      throw new RuntimeException(e);
    }
  }

  public void open(Collection<TopicPartition> partitions) {
    super.open(partitions);
    try {
      for (TopicPartition topicPartition : partitions) {
        log.info("BackupSinkTask.open");
        Path topicDir = Paths.get(targetDir.toString(), topicPartition.topic());
        Files.createDirectories(topicDir);
        PartitionWriter partitionWriter = new PartitionWriter(topicPartition.topic(), topicPartition.partition(),
            topicDir, maxSegmentSize);
        long lastWrittenOffset = partitionWriter.lastWrittenOffset();

        // Note that we must *always* request that we seek to an offset here. Currently the
        // framework will still commit Kafka offsets even though we track our own (see KAFKA-3462),
        // which can result in accidentally using that offset if one was committed but no files
        // were written to disk. To protect against this, even if we
        // just want to start at offset 0 or reset to the earliest offset, we specify that
        // explicitly to forcibly override any committed offsets.
        if (lastWrittenOffset > 0) {
          context.offset(topicPartition, lastWrittenOffset + 1);
          log.info("Initialized Topic {}, Partition {}. Last written offset: {}"
              , topicPartition.topic(), topicPartition.partition(), lastWrittenOffset);
        } else {
          // The offset was not found, so rather than forcibly set the offset to 0 we let the
          // consumer decide where to start based upon standard consumer offsets (if available)
          // or the consumer's `auto.offset.reset` configuration
          log.info("Resetting offset for {} based upon existing consumer group offsets or, if "
              + "there are none, the consumer's 'auto.offset.reset' value.", topicPartition);
        }
        this.partitionWriters.put(topicPartition, partitionWriter);
      }
      if (partitions.isEmpty()) {
        log.info("No partitions assigned to BackupSinkTask");
      }
    } catch (IOException | SegmentIndex.IndexException | PartitionIndex.IndexException e) {
      throw new RuntimeException(e);
    }
  }

  public void close(Collection<TopicPartition> partitions) {
    super.close(partitions);
    try {
      for (TopicPartition topicPartition : partitions) {
        PartitionWriter partitionWriter = partitionWriters.get(topicPartition);
        partitionWriter.close();
        partitionWriters.remove(topicPartition);
        log.info("Closed BackupSinkTask for Topic {}, Partition {}"
            , topicPartition.topic(), topicPartition.partition());
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void stop() {
    try {
      for (PartitionWriter partition : partitionWriters.values()) {
        partition.close();
      }
      offsetSink.close();
      log.info("Stopped BackupSinkTask");
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void flush(Map<TopicPartition, OffsetAndMetadata> currentOffsets) {
    try {
      for (PartitionWriter partitionWriter : partitionWriters.values()) {
        partitionWriter.flush();
        log.info("Flushed Topic {}, Partition {}"
            , partitionWriter.topic(), partitionWriter.partition());
      }
      offsetSink.flush();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

}
