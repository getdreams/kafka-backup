package de.azapps.kafkabackup.common.topic;

import de.azapps.kafkabackup.common.TopicsConfig;
import de.azapps.kafkabackup.common.KafkaConfigReader;
import de.azapps.kafkabackup.common.KafkaConfigWriter;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.concurrent.Semaphore;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RequiredArgsConstructor

public class TopicConfigurationService {

  private static final Logger log = LoggerFactory.getLogger(TopicConfigurationService.class);

  private String lastConfigHash = "";
  private long lastTopicConfigurationCheckTime;

  private final long minTopicConfigurationCheckInterval;
  private final KafkaConfigReader kafkaConfigReader;
  private final KafkaConfigWriter kafkaConfigWriter;

  Semaphore configurationCheckSemaphore = new Semaphore(1);

  public void runTopicConfigurationCheck() {
    log.debug("Starting Configuration check thread.");
    new Thread((() -> {
      checkTopicConfiguration();
    })).start();
  }

  // Public and synchronous for unit-testing
  public void checkTopicConfiguration() {
    if (kafkaConfigWriter == null) {
      log.warn("No KafkaConfigWriter provided. Skipping configuration check.");
      return;
    }

    if (!configurationCheckIntervalElapsed()) {
      log.debug("Configuration check interval not elapsed. Skipping check.");
      return;
    }

    final boolean canPerformCheck = configurationCheckSemaphore.tryAcquire();

    if (canPerformCheck) {
      try {
        TopicsConfig topicsConfig = kafkaConfigReader.readCurrentConfig();

        log.debug("Topic config: {}", topicsConfig.toJson());

        log.info("Topic configuration fetched. Last hash: {}, new hash: {}", lastConfigHash,
            topicsConfig.checksum());

        lastTopicConfigurationCheckTime = System.currentTimeMillis();

        if (lastConfigHash.equals(topicsConfig.checksum())) {
          log.info("Topic configuration did not change.");
          return;
        }

        kafkaConfigWriter.storeConfigBackup(topicsConfig);
        log.debug("Configuration saved. Hash {}", topicsConfig.checksum());
        lastConfigHash = topicsConfig.checksum();
      } catch (RuntimeException ex) {
        log.error("Error occurred while checking topic configuration.", ex);
      } finally {
        configurationCheckSemaphore.release();
      }
    }
  }

  private boolean configurationCheckIntervalElapsed() {
    log.info("Last check time {} min interval {} ms",
        LocalDateTime.ofInstant(Instant.ofEpochMilli(lastTopicConfigurationCheckTime), ZoneId.systemDefault()),
        minTopicConfigurationCheckInterval);
    return System.currentTimeMillis() - lastTopicConfigurationCheckTime > minTopicConfigurationCheckInterval;
  }

}
