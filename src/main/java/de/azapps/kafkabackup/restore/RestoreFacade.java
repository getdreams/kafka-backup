package de.azapps.kafkabackup.restore;

import de.azapps.kafkabackup.common.AdminClientService;
import de.azapps.kafkabackup.restore.common.RestoreArgsWrapper;
import de.azapps.kafkabackup.restore.message.RestoreMessageService;
import de.azapps.kafkabackup.restore.topic.RestoreTopicService;
import de.azapps.kafkabackup.storage.s3.AwsS3Service;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;

@Slf4j
public class RestoreFacade {


  private final RestoreMessageService restoreMessageService;
  private final RestoreTopicService restoreTopicService;
  private final RestoreOffsetService restoreOffsetService;
  private RestoreArgsWrapper restoreTopicsArgsWrapper;

  public RestoreFacade(RestoreArgsWrapper restoreTopicsArgsWrapper) {
    this.restoreTopicsArgsWrapper = restoreTopicsArgsWrapper;

    final AdminClientService adminClientService = new AdminClientService(
        AdminClient.create(restoreTopicsArgsWrapper.adminConfig()));

    final AwsS3Service awsS3Service = new AwsS3Service(restoreTopicsArgsWrapper.getAwsRegion(),
        restoreTopicsArgsWrapper.getAwsEndpoint(),
        restoreTopicsArgsWrapper.getPathStyleAccessEnabled());

    restoreTopicService = new RestoreTopicService(
        adminClientService,
        awsS3Service);

    restoreMessageService = new RestoreMessageService(awsS3Service, adminClientService, restoreTopicsArgsWrapper);
    restoreOffsetService = new RestoreOffsetService();
  }

  public void runRestoreProcess() {
    restoreTopicService.restoreTopics(this.restoreTopicsArgsWrapper);
    restoreMessageService.restoreMessages();
    restoreOffsetService.restoreOffsets();
  }

}
