package de.azapps.kafkabackup.restore;

import de.azapps.kafkabackup.common.AdminClientService;
import de.azapps.kafkabackup.restore.common.RestoreArgsWrapper;
import de.azapps.kafkabackup.restore.common.RestoreConfigurationHelper;
import de.azapps.kafkabackup.restore.common.RestoreConfigurationHelper.TopicPartitionToRestore;
import de.azapps.kafkabackup.restore.common.RestoreMode;
import de.azapps.kafkabackup.restore.message.RestoreMessageS3Service;
import de.azapps.kafkabackup.restore.message.RestoreMessageService;
import de.azapps.kafkabackup.restore.offset.RestoreOffsetService;
import de.azapps.kafkabackup.restore.topic.RestoreTopicService;
import de.azapps.kafkabackup.storage.s3.AwsS3Service;
import java.util.Map;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;

@Slf4j
@RequiredArgsConstructor
public class RestoreFacade {


  private final RestoreMessageService restoreMessageService;
  private final RestoreTopicService restoreTopicService;
  private final RestoreOffsetService restoreOffsetService;
  private final RestoreConfigurationHelper restoreConfigurationHelper;

  private static RestoreFacade initializedFacade;
  private final OffsetMapFileService offsetMaoFileService = new OffsetMapFileService();

  public static RestoreFacade initialize(RestoreArgsWrapper restoreArgsWrapper) {
    if (initializedFacade == null) {

      final AdminClientService adminClientService = new AdminClientService(
          AdminClient.create(restoreArgsWrapper.saslConfig()));

      final AwsS3Service awsS3Service = new AwsS3Service(restoreArgsWrapper.getAwsRegion(),
          restoreArgsWrapper.getAwsEndpoint(),
          restoreArgsWrapper.getPathStyleAccessEnabled());

      RestoreMessageS3Service restoreMessageS3Service = new RestoreMessageS3Service(awsS3Service,
          restoreArgsWrapper.getMessageBackupBucket());
      final RestoreMessageService restoreMessageService = new RestoreMessageService(restoreArgsWrapper.getRestoreMessagesMaxThreads(), restoreMessageS3Service);
      final RestoreTopicService restoreTopicService = new RestoreTopicService(adminClientService, awsS3Service);
      final RestoreOffsetService restoreOffsetService = new RestoreOffsetService(awsS3Service,
          restoreArgsWrapper.getOffsetBackupBucket(), restoreArgsWrapper);

      final RestoreConfigurationHelper restoreConfigurationHelper = new RestoreConfigurationHelper(awsS3Service,
          adminClientService);
      RestoreFacade restoreFacade = new RestoreFacade(restoreMessageService, restoreTopicService, restoreOffsetService,
          restoreConfigurationHelper);
      initializedFacade = restoreFacade;

      return restoreFacade;
    }

    return initializedFacade;
  }

  public void runRestoreProcess(RestoreArgsWrapper restoreArgsWrapper) {
    if (restoreArgsWrapper.getRestoreMode().contains(RestoreMode.TOPICS)) {
      restoreTopicService.restoreTopics(restoreArgsWrapper);
    }
    Map<String, TopicPartitionToRestore> partitionsToRestore =  restoreConfigurationHelper.getPartitionsToRestore(restoreArgsWrapper);

    if (restoreArgsWrapper.getRestoreMode().contains(RestoreMode.MESSAGES)) {
      partitionsToRestore = restoreMessageService
          .restoreMessages(restoreArgsWrapper, partitionsToRestore);

      if (offsetMaoFileService.shouldUseFileForOffsetMap(restoreArgsWrapper)) {
        offsetMaoFileService.saveOffsetMaps(restoreArgsWrapper.getOffsetMapFileName(), partitionsToRestore);
      }
    }


    if (restoreArgsWrapper.getRestoreMode().contains(RestoreMode.OFFSETS)) {
      if (offsetMaoFileService.shouldUseFileForOffsetMap(restoreArgsWrapper)) {
        offsetMaoFileService.restoreOffsetMaps(restoreArgsWrapper.getOffsetMapFileName(), partitionsToRestore);
      }

      restoreOffsetService.restoreOffsets(partitionsToRestore, restoreArgsWrapper.isDryRun());
    }
  }
}
