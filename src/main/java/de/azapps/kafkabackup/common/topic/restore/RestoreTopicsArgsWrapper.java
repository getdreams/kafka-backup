package de.azapps.kafkabackup.common.topic.restore;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Properties;
import lombok.Getter;
import lombok.ToString;

@Getter
@ToString
public class RestoreTopicsArgsWrapper {

  public static final String KAFKA_CONFIG_BACKUP_BUCKET = "aws.s3.bucketNameForConfig";
  public static final String KAFKA_BOOTSTRAP_SERVERS = "kafka.bootstrapServers";

  public static final String RESTORE_DRY_RUN = "restore.dryRun";
  public static final String RESTORE_IGNORE_EXISTING_TOPICS = "restore.ignoreExistingTopics";
  public static final String RESTORE_TIME = "restore.time";
  public static final String RESTORE_HASH = "restore.hash";


  private String configBackupBucket;
  private String kafkaBootstrapServers;
  private String hashToRestore;
  private LocalDateTime timeToRestore;
  private boolean ignoreExistingTopics;
  private boolean isDryRun;

  public void readProperties(String path) {
    try (InputStream fileInputStream = new FileInputStream(path)) {
      Properties properties = new Properties();
      properties.load(fileInputStream);

      kafkaBootstrapServers = properties.getProperty(KAFKA_BOOTSTRAP_SERVERS);
      if (kafkaBootstrapServers == null) {
        throw new RestoreConfigurationException(
            String.format("Missing required property %s", KAFKA_BOOTSTRAP_SERVERS));
      }

      configBackupBucket = properties.getProperty(KAFKA_CONFIG_BACKUP_BUCKET);
      if (configBackupBucket == null) {
        throw new RestoreConfigurationException(
            String.format("Missing required property %s", KAFKA_CONFIG_BACKUP_BUCKET));
      }

      try {
        final String restoreTimeString = properties.getProperty(RESTORE_TIME);
        if (restoreTimeString != null) {
          timeToRestore = LocalDateTime.parse(restoreTimeString, DateTimeFormatter.ISO_DATE_TIME);
        }
      } catch (DateTimeParseException ex) {
        throw new RestoreConfigurationException("Could not parse" + RESTORE_TIME + " . Expected ISO_DATE_TIME format.");
      }

      hashToRestore = properties.getProperty(RESTORE_HASH);

      if (hashToRestore == null && timeToRestore == null) {
        throw new RestoreConfigurationException("Neither hash nor time to restore specified. One must be set.");
      }

      if (hashToRestore != null && timeToRestore != null) {
        throw new RestoreConfigurationException("Both hash and time to restore specified. Only one allowed.");
      }

      ignoreExistingTopics = Boolean.parseBoolean(properties.getProperty(RESTORE_IGNORE_EXISTING_TOPICS, "false"));
      isDryRun = Boolean.parseBoolean(properties.getProperty(RESTORE_DRY_RUN, "true"));
    } catch (IOException ex) {
      System.out.println(ex.getMessage());
    }
  }

  public class RestoreConfigurationException extends RuntimeException {

    public RestoreConfigurationException(String message) {
      super(message);
    }
  }
}
