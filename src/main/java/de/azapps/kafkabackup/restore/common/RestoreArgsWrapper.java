package de.azapps.kafkabackup.restore.common;

import static de.azapps.kafkabackup.restore.common.RestoreArg.optional;
import static de.azapps.kafkabackup.restore.common.RestoreArg.required;
import static java.lang.Boolean.parseBoolean;
import static java.lang.Integer.parseInt;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import lombok.Builder;
import lombok.Getter;
import lombok.ToString;

@Getter
@ToString
@Builder
public class RestoreArgsWrapper {


  public static final String SHOULD_RESTORE_TOPICS = "restore.shouldRestoreTopics";
  public static final String SHOULD_RESTORE_MESSAGES = "restore.shouldRestoreMessages";
  public static final String SHOULD_RESTORE_OFFSETS = "restore.shouldRestoreOffsets";
  public static final String RESTORE_MODE = "restore.mode";

  public static final String AWS_S3_REGION = "aws.s3.region";
  public static final String AWS_S3_ENDPOINT = "aws.s3.endpoint";
  public static final String AWS_S3_PATH_STYLE_ACCESS_ENABLED = "aws.s3.pathStyleAccessEnabled";

  public static final String KAFKA_CONFIG_BACKUP_BUCKET = "aws.s3.bucketNameForConfig";
  public static final String KAFKA_BOOTSTRAP_SERVERS = "kafka.bootstrap.servers";

  public static final String RESTORE_DRY_RUN = "restore.dryRun";
  public static final String RESTORE_TOPIC_LIST = "restore.topicList";
  public static final String RESTORE_TIME = "restore.time";
  public static final String RESTORE_HASH = "restore.hash";

  public static final String RESTORE_MESSAGES_MAX_THREADS = "restore.messages.maxThreads";

  private final String awsEndpoint;
  private final String awsRegion;
  private final Boolean pathStyleAccessEnabled;

  private final String configBackupBucket;
  private final String kafkaBootstrapServers;
  private final String hashToRestore;
  private final LocalDateTime timeToRestore;
  private final List<String> topicsToRestore;
  private final boolean isDryRun;
  private final RestoreMode restoreMode;
  private final int restoreMessagesMaxThreads;

  public static final List<RestoreArg> args = List.of(
      required(AWS_S3_REGION),
      required(KAFKA_BOOTSTRAP_SERVERS),
      required(KAFKA_CONFIG_BACKUP_BUCKET),
      required(RESTORE_HASH),
      required(RESTORE_MODE),

      optional(AWS_S3_PATH_STYLE_ACCESS_ENABLED),
      optional(RESTORE_DRY_RUN),
      optional(RESTORE_TOPIC_LIST),
      optional(RESTORE_TIME),
      optional(RESTORE_MESSAGES_MAX_THREADS)
  );

  public static RestoreArgsWrapper of(String path) {
    RestoreArgsWrapperBuilder builder = RestoreArgsWrapper.builder();

    Properties properties = readPropertiesFromFile(path);

    validateProperties(properties);

    builder.awsRegion(properties.getProperty(AWS_S3_REGION));
    builder.awsEndpoint(properties.getProperty(AWS_S3_ENDPOINT));
    builder.kafkaBootstrapServers(properties.getProperty(KAFKA_BOOTSTRAP_SERVERS));
    builder.configBackupBucket(properties.getProperty(KAFKA_CONFIG_BACKUP_BUCKET));
    builder.hashToRestore(properties.getProperty(RESTORE_HASH));
    builder.restoreMode(RestoreMode.valueOf(properties.getProperty(RESTORE_MODE)));

    builder.pathStyleAccessEnabled(parseBoolean(properties.getProperty(AWS_S3_PATH_STYLE_ACCESS_ENABLED, "false")));
    builder.isDryRun(parseBoolean(properties.getProperty(RESTORE_DRY_RUN, "true")));
    builder.topicsToRestore(
        properties.containsKey(RESTORE_TOPIC_LIST) ? List.of(properties.getProperty(RESTORE_TOPIC_LIST).split(","))
            : List.of());

    builder.timeToRestore(getRestoreTime(properties));
    builder.restoreMessagesMaxThreads(parseInt(properties.getProperty(RESTORE_MESSAGES_MAX_THREADS, "1")));

    return builder.build();
}

  private static LocalDateTime getRestoreTime(Properties properties) {
      return Optional.ofNullable(properties.getProperty(RESTORE_TIME))
          .map((restoreTimeString) ->
              LocalDateTime.parse(restoreTimeString, DateTimeFormatter.ISO_DATE_TIME))
          .orElse(null);
  }

  private static void validateProperties(Properties properties) {
    args.stream().filter(RestoreArg::isRequired).forEach(restoreArg -> {
      if (properties.get(restoreArg.getName()) == null) {
        throw new RestoreConfigurationException(
            String.format("Missing required property: %s", restoreArg.getName()));
      }
    });
  }

  private static Properties readPropertiesFromFile(String path) {
    try {
      InputStream fileInputStream = new FileInputStream(path);
      Properties properties = new Properties();
      properties.load(fileInputStream);
      return properties;
    } catch (IOException e) {
      throw new RuntimeException("Unable to load properties from file", e);
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
