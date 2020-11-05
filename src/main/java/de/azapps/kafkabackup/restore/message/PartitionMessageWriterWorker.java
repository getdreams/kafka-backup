package de.azapps.kafkabackup.restore.message;

import com.google.common.collect.Lists;
import de.azapps.kafkabackup.common.record.Record;
import de.azapps.kafkabackup.restore.common.RestoreArgsWrapper;
import de.azapps.kafkabackup.restore.message.RestoreMessageService.TopicPartitionToRestore;
import de.azapps.kafkabackup.storage.s3.AwsS3Service;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.javatuples.Pair;

@Getter
@Slf4j
public class PartitionMessageWriterWorker implements Runnable {

  private static final int PRODUCER_BATCH_SIZE = 1000;
  private final TopicPartitionToRestore topicPartitionToRestore;
  private final RestoreMessageS3Service restoreMessageS3Service;
  private final RestoreArgsWrapper restoreArgsWrapper;
  private final String identifier;
  private KafkaProducer<byte[], byte[]> kafkaProducer;

  public PartitionMessageWriterWorker(
      TopicPartitionToRestore topicPartitionToRestore,
      AwsS3Service awsS3Service,
      RestoreArgsWrapper restoreArgsWrapper) {
    this.topicPartitionToRestore = topicPartitionToRestore;
    this.restoreMessageS3Service = new RestoreMessageS3Service(awsS3Service, restoreArgsWrapper.getMessageBackupBucket());
    this.restoreArgsWrapper = restoreArgsWrapper;
    this.identifier = topicPartitionToRestore.getTopicPartitionId();
    initiateProducer(topicPartitionToRestore, restoreArgsWrapper);
  }

  @Override
  public void run() {
    try {
      log.info("Restoring messages for topic partition {}", topicPartitionToRestore);
      topicPartitionToRestore.setMessageRestorationStatus(MessageRestorationStatus.RUNNING);

      List<String> filesToRestore = restoreMessageS3Service
          .getMessageBackupFileNames(topicPartitionToRestore.getTopicConfiguration().getTopicName(),
              topicPartitionToRestore.getPartitionNumber());

      log.debug("Got {} files to restore.", filesToRestore.size());

      filesToRestore.forEach(this::restoreBackupFile);

      topicPartitionToRestore.setMessageRestorationStatus(MessageRestorationStatus.SUCCESS);

      log.info("Message restoration succeeded for topic partition {}", topicPartitionToRestore.getTopicPartitionId());
    } catch (RuntimeException ex) {
      log.error(String.format("Message restoration for topic partition %s failed",
          topicPartitionToRestore.getTopicPartitionId()),
          ex);
      topicPartitionToRestore.setMessageRestorationStatus(MessageRestorationStatus.ERROR);
    }
  }

  private void restoreBackupFile(String backupFileKey) {
    List<Record> records = restoreMessageS3Service.readBatchFile(backupFileKey);
    log.debug("Got {} messages from batch file {}.", records.size(), backupFileKey);

    produceRecords(records);
  }

  private void initiateProducer(TopicPartitionToRestore topicPartitionToRestore,
      RestoreArgsWrapper restoreArgsWrapper) {
    Properties props = new Properties();
    props.put("bootstrap.servers", restoreArgsWrapper.getKafkaBootstrapServers());
    props.put("acks", "all");
    props.put("retries", 1);
    props.put("batch.size", PRODUCER_BATCH_SIZE);
    props.put("linger.ms", 1);
    props.put("buffer.memory", 33554432);
    props.put("key.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
    props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");

    props.put("transactional.id", "restore-transactional-id" + topicPartitionToRestore.getTopicPartitionId());
    this.kafkaProducer = new KafkaProducer<>(props);
    kafkaProducer.initTransactions();
  }

  private void produceRecords(List<Record> recordsToProduce) {
    try {
      kafkaProducer.beginTransaction();
      List<List<Record>> partitionedRecords = Lists.partition(recordsToProduce, PRODUCER_BATCH_SIZE);

      for (List<Record> batch : partitionedRecords) {
        List<Pair<Record, Future<RecordMetadata>>> futures = new ArrayList<>();
        batch.forEach(record -> {
          if (topicPartitionToRestore.getRestoredMessageInfoMap().containsKey(record.kafkaOffset())) {
            log.info("Skipping duplicate for offset {}. Topic partition {}", record.kafkaOffset(),
                topicPartitionToRestore.getTopicPartitionId());
            return;
          }

          ProducerRecord<byte[], byte[]> producerRecord = new ProducerRecord<>(record.topic(),
              record.kafkaPartition(),
              record.timestamp(),
              record.key(),
              record.value());
          Future<RecordMetadata> future = kafkaProducer.send(producerRecord);
          futures.add(new Pair<>(record, future));
        });
        kafkaProducer.commitTransaction();

        for (Pair<Record, Future<RecordMetadata>> recordFuturePair : futures) {
          RecordMetadata recordMetadata = recordFuturePair.getValue1().get();
          Record record = recordFuturePair.getValue0();
          topicPartitionToRestore.addRestoredMessageInfo(record.kafkaOffset(), record.key(), recordMetadata.offset());
        }
      }
    }
    catch (RuntimeException | InterruptedException | ExecutionException ex) {
      kafkaProducer.close();
      throw new RuntimeException(ex);
    }
  }
}
