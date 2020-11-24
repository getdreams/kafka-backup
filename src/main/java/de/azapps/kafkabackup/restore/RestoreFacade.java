package de.azapps.kafkabackup.restore;

import de.azapps.kafkabackup.common.AdminClientService;
import de.azapps.kafkabackup.restore.common.RestoreArgsWrapper;
import de.azapps.kafkabackup.restore.common.RestoreMode;
import de.azapps.kafkabackup.restore.message.RestoreMessageS3Service;
import de.azapps.kafkabackup.restore.message.RestoreMessageService;
import de.azapps.kafkabackup.restore.message.RestoreMessageService.TopicPartitionToRestore;
import de.azapps.kafkabackup.restore.offset.RestoreOffsetService;
import de.azapps.kafkabackup.restore.topic.RestoreTopicService;
import de.azapps.kafkabackup.storage.s3.AwsS3Service;
import java.util.Collections;
import java.util.List;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;

@Slf4j
@RequiredArgsConstructor
public class RestoreFacade {


  private final RestoreMessageService restoreMessageService;
  private final RestoreTopicService restoreTopicService;
  private final RestoreOffsetService restoreOffsetService;

  private static RestoreFacade initializedFacade;

  public static RestoreFacade initialize(RestoreArgsWrapper restoreArgsWrapper) {
    if (initializedFacade == null) {

      final AdminClientService adminClientService = new AdminClientService(
          AdminClient.create(restoreArgsWrapper.saslConfig()));

      final AwsS3Service awsS3Service = new AwsS3Service(restoreArgsWrapper.getAwsRegion(),
          restoreArgsWrapper.getAwsEndpoint(),
          restoreArgsWrapper.getPathStyleAccessEnabled());

      RestoreMessageS3Service restoreMessageS3Service = new RestoreMessageS3Service(awsS3Service,
          restoreArgsWrapper.getMessageBackupBucket());
      final RestoreMessageService restoreMessageService = new RestoreMessageService(awsS3Service, adminClientService,
          restoreArgsWrapper.getRestoreMessagesMaxThreads(), restoreMessageS3Service);
      final RestoreTopicService restoreTopicService = new RestoreTopicService(adminClientService, awsS3Service);
      final RestoreOffsetService restoreOffsetService = new RestoreOffsetService(awsS3Service,
          restoreArgsWrapper.getOffsetBackupBucket(), restoreArgsWrapper);

      RestoreFacade restoreFacade = new RestoreFacade(restoreMessageService, restoreTopicService, restoreOffsetService);
      initializedFacade = restoreFacade;
      
      return restoreFacade;
    }

    return initializedFacade;
  }

  public void runRestoreProcess(RestoreArgsWrapper restoreArgsWrapper) {
    if (restoreArgsWrapper.getRestoreMode().contains(RestoreMode.TOPICS)) {
      restoreTopicService.restoreTopics(restoreArgsWrapper);
    }
    List<TopicPartitionToRestore> topicPartitionToRestore = Collections.emptyList();
    if (restoreArgsWrapper.getRestoreMode().contains(RestoreMode.MESSAGES)) {
      topicPartitionToRestore = restoreMessageService
          .restoreMessages(restoreArgsWrapper);
    }
    if (restoreArgsWrapper.getRestoreMode().contains(RestoreMode.OFFSETS)) {
      restoreOffsetService.restoreOffsets(topicPartitionToRestore, restoreArgsWrapper.isDryRun());
    }
  }

}
