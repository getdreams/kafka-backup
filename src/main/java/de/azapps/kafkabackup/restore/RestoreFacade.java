package de.azapps.kafkabackup.restore;

import de.azapps.kafkabackup.common.AdminClientService;
import de.azapps.kafkabackup.common.topic.restore.RestoreTopicsArgsWrapper;
import de.azapps.kafkabackup.storage.s3.AwsS3Service;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;

@Slf4j
public class RestoreFacade {


  private final RestoreMessageService restoreMessageService;
  private final RestoreTopicService restoreTopicService;
  private final RestoreOffsetService restoreOffsetService;
  private RestoreTopicsArgsWrapper restoreTopicsArgsWrapper;

  public RestoreFacade(RestoreTopicsArgsWrapper restoreTopicsArgsWrapper) {
    this.restoreTopicsArgsWrapper = restoreTopicsArgsWrapper;

    final AdminClientService adminClientService = new AdminClientService(
        AdminClient.create(restoreTopicsArgsWrapper.adminConfig()));

    final AwsS3Service awsS3Service = new AwsS3Service(restoreTopicsArgsWrapper.getAwsRegion(),
        restoreTopicsArgsWrapper.getAwsEndpoint(),
        restoreTopicsArgsWrapper.getPathStyleAccessEnabled());

    restoreTopicService = new RestoreTopicService(
        adminClientService,
        awsS3Service);

    restoreMessageService = new RestoreMessageService();
    restoreOffsetService = new RestoreOffsetService();
  }

  //TODO discuss - I would remove facade and create 3 separate gradle tasks to run each of those steps independently
  // but probably with common properties file
  public void runRestoreProcess() {
    restoreTopicService.restoreTopics(this.restoreTopicsArgsWrapper);
    restoreMessageService.restoreMessages();
    restoreOffsetService.restoreOffsets();
  }

}
