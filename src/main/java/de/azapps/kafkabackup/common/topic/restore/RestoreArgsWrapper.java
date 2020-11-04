package de.azapps.kafkabackup.common.topic.restore;

import static de.azapps.kafkabackup.common.topic.restore.RestoreArg.param;
import static de.azapps.kafkabackup.common.topic.restore.RestoreArg.singleParam;
import static de.azapps.kafkabackup.common.topic.restore.RestoreMode.MESSAGES;
import static de.azapps.kafkabackup.common.topic.restore.RestoreMode.OFFSETS;
import static de.azapps.kafkabackup.common.topic.restore.RestoreMode.TOPICS;
import static java.lang.Boolean.parseBoolean;
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
  public static final String RESTORE_TOPIC_LIST_MODE = "restore.topicsList.mode";
  public static final String RESTORE_TOPIC_LIST_VALUE = "restore.topicsList.value";
  public static final String RESTORE_TIME = "restore.time";
  public static final String RESTORE_HASH = "restore.hash";

  private final String awsEndpoint;
  private final String awsRegion;
  private final Boolean pathStyleAccessEnabled;

  private final String configBackupBucket;
  private final String kafkaBootstrapServers;
  private final String hashToRestore;
  private final LocalDateTime timeToRestore;
  private final TopicsListMode topicsListMode;
  private final List<String> topicsList;
  private final String topicsRegexp;
  private final boolean isDryRun;
  private final RestoreMode restoreMode;

  public static final List<RestoreArg> args = List.of(
      param(singleParam(AWS_S3_REGION).isRequired(true)),
      param(singleParam(KAFKA_BOOTSTRAP_SERVERS).isRequired(true)),
      param(singleParam(KAFKA_CONFIG_BACKUP_BUCKET).isRequired(true)),
      param(singleParam(RESTORE_HASH).isRequired(true)),
      param(singleParam(RESTORE_MODE).isRequired(true)
          .allowedValues(List.of(MESSAGES.name(), TOPICS.name(), OFFSETS.name()))),

      param(singleParam(AWS_S3_PATH_STYLE_ACCESS_ENABLED).isRequired(false)),
      param(singleParam(RESTORE_DRY_RUN).isRequired(false)),
      param(singleParam(RESTORE_TOPIC_LIST_MODE).isRequired(true)),
      param(singleParam(RESTORE_TOPIC_LIST_VALUE).isRequired(false)),
      param(singleParam(RESTORE_TIME).isRequired(false))
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

    TopicsListMode topicListMode = Optional.ofNullable(properties.getProperty(RESTORE_TOPIC_LIST_MODE))
        .map(TopicsListMode::valueOf)
        .orElse(TopicsListMode.ALL_TOPICS);

    builder.topicsListMode(topicListMode);
    switch (topicListMode) {
      case BLACKLIST:
      case WHITELIST:
        builder.topicsList(List.of(properties.getProperty(RESTORE_TOPIC_LIST_VALUE).split(",")));
        break;
      case REGEXP:
        builder.topicsRegexp(properties.getProperty(RESTORE_TOPIC_LIST_VALUE));
    }

    builder.timeToRestore(getRestoreTime(properties));

    return builder.build();
}

  private static LocalDateTime getRestoreTime(Properties properties) {
      return Optional.ofNullable(properties.getProperty(RESTORE_TIME))
          .map((restoreTimeString) ->
              LocalDateTime.parse(restoreTimeString, DateTimeFormatter.ISO_DATE_TIME))
          .orElse(null);
  }

  private static void validateProperties(Properties properties) {
    args.forEach(restoreArg -> {
      if (restoreArg.isRequired()) {
        restoreArg.getNames().stream()
            .filter(paramName -> properties.get(paramName) == null)
            .findAny()
            .ifPresent((arg) -> {
              throw new RestoreConfigurationException(
                  String.format("Missing required property: %s", arg));
            });
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
