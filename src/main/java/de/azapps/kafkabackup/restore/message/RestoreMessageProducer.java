package de.azapps.kafkabackup.restore.message;

import com.google.common.collect.Lists;
import de.azapps.kafkabackup.common.record.Record;
import de.azapps.kafkabackup.restore.common.RestoreArgsWrapper;
import de.azapps.kafkabackup.restore.message.RestoreMessageService.TopicPartitionToRestore;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.errors.InvalidTimestampException;
import org.javatuples.Pair;

@Slf4j
public class RestoreMessageProducer {
  private static final int PRODUCER_BATCH_SIZE = 1;

  private final RestoreArgsWrapper restoreArgsWrapper;
  private final TopicPartitionToRestore topicPartitionToRestore;

  private long dryRunOffset;
  private KafkaProducer<byte[], byte[]> kafkaProducer;

  public RestoreMessageProducer(RestoreArgsWrapper restoreArgsWrapper, TopicPartitionToRestore topicPartitionToRestore) {
    this.restoreArgsWrapper = restoreArgsWrapper;
    this.topicPartitionToRestore = topicPartitionToRestore;

    initiateProducer(topicPartitionToRestore, restoreArgsWrapper);
  }
  private void initiateProducer(TopicPartitionToRestore topicPartitionToRestore,
      RestoreArgsWrapper restoreArgsWrapper) {
    if (!restoreArgsWrapper.isDryRun()) {
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
    else {
      dryRunOffset = 0l;
    }
  }

  public void produceRecords(List<Record> recordsToProduce) {
    try {

      List<List<Record>> partitionedRecords = Lists.partition(recordsToProduce, PRODUCER_BATCH_SIZE);

      for (List<Record> batch : partitionedRecords) {
        beginTransaction();
        List<Pair<Record, Future<RecordMetadata>>> futures = new ArrayList<>();
        batch.forEach(record -> {
          if (topicPartitionToRestore.getRestoredMessageInfoMap().containsKey(record.kafkaOffset())) {
            log.info("Skipping duplicate for offset {}. Topic partition {}", record.kafkaOffset(),
                topicPartitionToRestore.getTopicPartitionId());
            return;
          }

          Future<RecordMetadata> future = produceRecord(record);

          futures.add(new Pair<>(record, future));
        });

        commitTransaction();

        updateTargetOffsets(futures);
      }
    }
    catch (InvalidTimestampException ex) {
      kafkaProducer.abortTransaction();
    }
    catch (RuntimeException | InterruptedException | ExecutionException ex) {
      kafkaProducer.close();
      throw new RuntimeException(ex);
    }
  }

  private Future<RecordMetadata> produceRecord(Record record) {
    ProducerRecord<byte[], byte[]> producerRecord = new ProducerRecord<>(record.topic(),
        record.kafkaPartition(),
        record.timestamp(),
        record.key(),
        record.value());

    if (this.restoreArgsWrapper.isDryRun()) {
      log.info("Producing record. Original offset: {}, topic: {}, partition: {}",
          record.kafkaOffset(),
          producerRecord.topic(),
          producerRecord.partition());
      topicPartitionToRestore.addRestoredMessageInfo(record.kafkaOffset(), record.key(), dryRunOffset);
      dryRunOffset++;
      return null;
    }
    else {
      topicPartitionToRestore.addRestoredMessageInfo(record.kafkaOffset(), record.key(), null);
      return kafkaProducer.send(producerRecord);
    }
  }

  private void commitTransaction() {
    if (!this.restoreArgsWrapper.isDryRun()) {
      kafkaProducer.commitTransaction();
    }
  }

  private void beginTransaction() {
    if (!this.restoreArgsWrapper.isDryRun()) {
      kafkaProducer.beginTransaction();
    }
  }

  private void updateTargetOffsets(List<Pair<Record, Future<RecordMetadata>>> futures)
      throws InterruptedException, ExecutionException {
    for (Pair<Record, Future<RecordMetadata>> recordFuturePair : futures) {
      Record record = recordFuturePair.getValue0();

      if (this.restoreArgsWrapper.isDryRun()) {
        if (recordFuturePair.getValue1() == null) {
          log.debug("Dry run and empty future.");
        }
        else {
          throw new RuntimeException("Future not empty in dry run mode for record " + record.kafkaOffset());
        }
      }
      else {
        RecordMetadata recordMetadata = recordFuturePair.getValue1().get();
        topicPartitionToRestore.addRestoredMessageInfo(record.kafkaOffset(), record.key(), recordMetadata.offset());
      }
    }
  }

}