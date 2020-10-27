package de.azapps.kafkabackup.restore;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public class RestoreFacade {


  private final RestoreMessageService restoreMessageService;
  private final RestoreTopicService restoreTopicService;
  private final RestoreOffsetService restoreOffsetService;

  public void runRestoreProcess(long restorationTimestamp) {
    restoreTopicService.restoreTopics(restorationTimestamp);
    restoreMessageService.restoreMessages();
    restoreOffsetService.restoreOffsets();
  }

  public void runRestoreProcess(String configurationChecksum) {
    restoreTopicService.restoreTopics(configurationChecksum);
    restoreMessageService.restoreMessages();
    restoreOffsetService.restoreOffsets();
  }

}
