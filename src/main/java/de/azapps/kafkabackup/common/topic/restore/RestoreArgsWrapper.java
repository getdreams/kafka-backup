package de.azapps.kafkabackup.common.topic.restore;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import lombok.Getter;
import lombok.ToString;

@Getter
@ToString
public class RestoreArgsWrapper {

  public static final String SHOULD_RESTORE_TOPICS = "restore.shouldRestoreTopics";
  public static final String SHOULD_RESTORE_MESSAGES = "restore.shouldRestoreMessages";
  public static final String SHOULD_RESTORE_OFFSETS = "restore.shouldRestoreOffsets";

  public static final String AWS_S3_REGION = "aws.s3.region";
  public static final String AWS_S3_ENDPOINT = "aws.s3.endpoint";
  public static final String AWS_S3_PATH_STYLE_ACCESS_ENABLED = "aws.s3.pathStyleAccessEnabled";

  public static final String KAFKA_CONFIG_BACKUP_BUCKET = "aws.s3.bucketNameForConfig";
  public static final String KAFKA_BOOTSTRAP_SERVERS = "kafka.bootstrap.servers";

  public static final String RESTORE_DRY_RUN = "restore.dryRun";
  public static final String RESTORE_TOPIC_LIST = "restore.topicList";
  public static final String RESTORE_TIME = "restore.time";
  public static final String RESTORE_HASH = "restore.hash";

  private String awsEndpoint;
  private String awsRegion;
  private Boolean pathStyleAccessEnabled;

  private String configBackupBucket;
  private String kafkaBootstrapServers;
  private String hashToRestore;
  private LocalDateTime timeToRestore;
  private List<String> topicsToRestore;
  private boolean isDryRun;
  private boolean shouldRestoreTopics;
  private boolean shouldRestoreMessages;
  private boolean shouldRestoreOffsets;

  public void readProperties(String path) {
    try (InputStream fileInputStream = new FileInputStream(path)) {
      Properties properties = new Properties();
      properties.load(fileInputStream);

      awsRegion = properties.getProperty(AWS_S3_REGION);
      if (awsRegion == null) {
        throw new RestoreConfigurationException(
            String.format("Missing required property %s", AWS_S3_REGION));
      }

      awsEndpoint = properties.getProperty(AWS_S3_ENDPOINT);

      shouldRestoreTopics = Boolean.parseBoolean(properties.getProperty(SHOULD_RESTORE_TOPICS, "false"));
      shouldRestoreMessages = Boolean.parseBoolean(properties.getProperty(SHOULD_RESTORE_MESSAGES, "false"));
      shouldRestoreOffsets = Boolean.parseBoolean(properties.getProperty(SHOULD_RESTORE_OFFSETS, "false"));

      if (!(shouldRestoreOffsets || shouldRestoreMessages || shouldRestoreTopics)) {
        throw new RestoreConfigurationException(
            String.format("Neither topics, messages nor offsets are set to be restored. Set on of %s, %s, %s.",
                SHOULD_RESTORE_TOPICS,SHOULD_RESTORE_MESSAGES, SHOULD_RESTORE_OFFSETS));
      }

      pathStyleAccessEnabled = Boolean.parseBoolean(properties.getProperty(AWS_S3_PATH_STYLE_ACCESS_ENABLED, "false"));

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

      topicsToRestore = List.of(properties.getProperty(RESTORE_TOPIC_LIST, "").split(","));
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

  public long getTimestampToRestore() {
    return this.timeToRestore.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
  }

  public Map<String, Object> adminConfig() {
    Map<String, Object> props = new HashMap<>();
    // First use envvars to populate certain props
    String saslMechanism = System.getenv("CONNECT_ADMIN_SASL_MECHANISM");
    if (saslMechanism != null) {
      props.put("sasl.mechanism", saslMechanism);
    }
    String securityProtocol = System.getenv("CONNECT_ADMIN_SECURITY_PROTOCOL");
    if (securityProtocol != null) {
      props.put("security.protocol", securityProtocol);
    }
    // NOTE: this is secret, so we *cannot* put it in the task config
    String saslJaasConfig = System.getenv("CONNECT_ADMIN_SASL_JAAS_CONFIG");
    if (saslJaasConfig != null) {
      props.put("sasl.jaas.config", saslJaasConfig);
    }
    // Then override with task config
    props.put("bootstrap.servers", kafkaBootstrapServers);

    return props;
  }
}
