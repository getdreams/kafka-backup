package de.azapps.kafkabackup.restore;

import com.fasterxml.jackson.databind.ObjectMapper;
import de.azapps.kafkabackup.common.AdminClientService;
import de.azapps.kafkabackup.restore.common.RestoreArgsWrapper;
import de.azapps.kafkabackup.restore.common.RestoreMode;
import de.azapps.kafkabackup.restore.message.RestoreMessageS3Service;
import de.azapps.kafkabackup.restore.message.RestoreMessageService;
import de.azapps.kafkabackup.restore.message.RestoreMessageService.RestoredMessageInfo;
import de.azapps.kafkabackup.restore.message.RestoreMessageService.TopicPartitionToRestore;
import de.azapps.kafkabackup.restore.offset.RestoreOffsetService;
import de.azapps.kafkabackup.restore.topic.RestoreTopicService;
import de.azapps.kafkabackup.storage.s3.AwsS3Service;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import sun.jvm.hotspot.oops.OopUtilities;

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

    if (shouldUseFileForOffsetMap(restoreArgsWrapper)) {
      saveOffsetMaps(restoreArgsWrapper.getOffsetMapFileName(), topicPartitionToRestore);
    }

    if (restoreArgsWrapper.getRestoreMode().contains(RestoreMode.OFFSETS)) {
      if (shouldUseFileForOffsetMap(restoreArgsWrapper)) {
        restoreOffsetMaps(restoreArgsWrapper.getOffsetMapFileName(), topicPartitionToRestore);
      }

      restoreOffsetService.restoreOffsets(topicPartitionToRestore, restoreArgsWrapper.isDryRun());
    }
  }

  private boolean shouldUseFileForOffsetMap(RestoreArgsWrapper restoreArgsWrapper) {
    return restoreArgsWrapper.getOffsetMapFileName() != null && !restoreArgsWrapper.getOffsetMapFileName().isEmpty();
  }

  private void saveOffsetMaps(String filePath, List<TopicPartitionToRestore> topicPartitionToRestore) {
    Map<String, Map<Long, RestoredMessageInfo>> allOffsetMaps = new HashMap<>();

    topicPartitionToRestore.forEach(tp -> allOffsetMaps.put(tp.getTopicPartitionId(), tp.getRestoredMessageInfoMap()));

    ObjectMapper mapper = new ObjectMapper();
    try {
      mapper.writerWithDefaultPrettyPrinter().writeValue(Paths.get(filePath).toFile(), allOffsetMaps);
    } catch (IOException e) {
      log.error("Could not write offset map to file {}", filePath, e);
      throw new RuntimeException(e);
    }
  }

  private void restoreOffsetMaps(String fileName, List<TopicPartitionToRestore> topicPartitionsToRestore) {
   // TODO read

    topicPartitionsToRestore.forEach(
        topicPartitionToRestore ->
            topicPartitionToRestore.setRestoredMessageInfoMap()
    );
  }

}
