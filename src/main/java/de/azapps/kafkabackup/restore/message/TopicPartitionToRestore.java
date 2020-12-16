package de.azapps.kafkabackup.restore.message;

import de.azapps.kafkabackup.common.TopicConfiguration;
import java.util.HashMap;
import java.util.Map;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class TopicPartitionToRestore {
    final TopicConfiguration topicConfiguration;
    final int partitionNumber;
    private MessageRestorationStatus messageRestorationStatus;
    private Map<Long, Long> restoredMessageInfoMap;
    private long maxOriginalOffset = -1L;

    public TopicPartitionToRestore(TopicConfiguration topicConfiguration, int partitionNumber) {
      this.topicConfiguration = topicConfiguration;
      this.partitionNumber = partitionNumber;
      this.messageRestorationStatus = MessageRestorationStatus.WAITING;
      this.restoredMessageInfoMap = new HashMap<>();
    }

    public String getTopicPartitionId() {
      return topicConfiguration.getTopicName() + "." + partitionNumber;
    }

    public void addRestoredMessageInfo(long originalOffset, Long newOffset) {
      restoredMessageInfoMap.put(originalOffset,newOffset);
      maxOriginalOffset = Math.max(maxOriginalOffset, originalOffset);
    }
}
