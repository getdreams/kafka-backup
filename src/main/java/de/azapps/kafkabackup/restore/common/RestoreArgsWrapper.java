package de.azapps.kafkabackup.restore.common;

import static de.azapps.kafkabackup.restore.common.RestoreArg.param;
import static de.azapps.kafkabackup.restore.common.RestoreArg.singleParam;
import static de.azapps.kafkabackup.restore.common.RestoreMode.MESSAGES;
import static de.azapps.kafkabackup.restore.common.RestoreMode.OFFSETS;
import static de.azapps.kafkabackup.restore.common.RestoreMode.TOPICS;
import static java.lang.Boolean.parseBoolean;
import static java.lang.Integer.parseInt;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.stream.Collectors;
import lombok.Builder;
import lombok.Getter;
import lombok.ToString;

@Getter
@ToString
@Builder
public class RestoreArgsWrapper {


  public static final String RESTORE_MODE = "restore.mode";

  public static final String AWS_S3_REGION = "aws.s3.region";
  public static final String AWS_S3_ENDPOINT = "aws.s3.endpoint";
  public static final String AWS_S3_PATH_STYLE_ACCESS_ENABLED = "aws.s3.pathStyleAccessEnabled";

  public static final String KAFKA_CONFIG_BACKUP_BUCKET = "aws.s3.bucketNameForConfig";
  public static final String MESSAGE_BACKUP_BUCKET = "aws.s3.bucketNameForMessages";
  public static final String KAFKA_BOOTSTRAP_SERVERS = "kafka.bootstrap.servers";

  public static final String RESTORE_DRY_RUN = "restore.dryRun";
  public static final String RESTORE_TOPIC_ALLOW_LIST = "restore.topicsAllowList";
  public static final String RESTORE_TOPIC_DENY_LIST = "restore.topicsDenyList";
  public static final String RESTORE_TIME = "restore.time";
  public static final String RESTORE_HASH = "restore.hash";

  public static final String ALL_TOPICS_REGEX = ".*";
  public static final String NONE_TOPICS_REGEX = "$^";

  public static final String RESTORE_MESSAGES_MAX_THREADS = "restore.messages.maxThreads";

  private final String awsEndpoint;
  private final String awsRegion;
  private final Boolean pathStyleAccessEnabled;

  private final String configBackupBucket;
  private final String messageBackupBucket;
  private final String kafkaBootstrapServers;
  private final String hashToRestore;
  private final LocalDateTime timeToRestore;
  private final String topicsAllowListRegex;
  private final String topicsDenyListRegex;
  private final boolean isDryRun;
  private final List<RestoreMode> restoreMode;
  private final int restoreMessagesMaxThreads;

  public static final List<RestoreArg> args = List.of(
      param(singleParam(AWS_S3_REGION).isRequired(true)),
      param(singleParam(KAFKA_BOOTSTRAP_SERVERS).isRequired(true)),
      param(singleParam(KAFKA_CONFIG_BACKUP_BUCKET).isRequired(true)),
      param(singleParam(RESTORE_HASH).isRequired(true)),
      param(singleParam(RESTORE_MODE).isRequired(true)
          .allowedValues(List.of(MESSAGES.name(), TOPICS.name(), OFFSETS.name()))),

      param(singleParam(AWS_S3_PATH_STYLE_ACCESS_ENABLED).isRequired(false)),
      param(singleParam(RESTORE_DRY_RUN).isRequired(false)),
      param(singleParam(RESTORE_TOPIC_ALLOW_LIST).isRequired(false)),
      param(singleParam(RESTORE_TOPIC_DENY_LIST).isRequired(false)),
      param(singleParam(RESTORE_TIME).isRequired(false)),
      param(singleParam(RESTORE_MESSAGES_MAX_THREADS).isRequired(false))
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
    builder.restoreMode(Arrays.stream(properties.getProperty(RESTORE_MODE).split(",")).sequential()
        .map(restoreModeName -> RestoreMode.valueOf(restoreModeName.toUpperCase())).collect(Collectors.toList()));
    builder.messageBackupBucket(properties.getProperty(MESSAGE_BACKUP_BUCKET));

    builder.pathStyleAccessEnabled(parseBoolean(properties.getProperty(AWS_S3_PATH_STYLE_ACCESS_ENABLED, "false")));
    builder.isDryRun(parseBoolean(properties.getProperty(RESTORE_DRY_RUN, "true")));


    builder.topicsAllowListRegex(properties.getProperty(RESTORE_TOPIC_ALLOW_LIST, ALL_TOPICS_REGEX));
    builder.topicsDenyListRegex(properties.getProperty(RESTORE_TOPIC_DENY_LIST, NONE_TOPICS_REGEX));

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

  public Optional<Long> getTimestampToRestore() {
    return Optional.ofNullable(this.timeToRestore)
        .map(timeToRestore -> timeToRestore.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli());
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
