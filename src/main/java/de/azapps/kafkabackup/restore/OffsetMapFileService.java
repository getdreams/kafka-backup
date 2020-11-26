package de.azapps.kafkabackup.restore;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import de.azapps.kafkabackup.restore.common.RestoreArgsWrapper;
import de.azapps.kafkabackup.restore.message.TopicPartitionToRestore;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class OffsetMapFileService {

  public OffsetMapFileService() {
  }

  boolean shouldUseFileForOffsetMap(RestoreArgsWrapper restoreArgsWrapper) {
    return restoreArgsWrapper.getOffsetMapFileName() != null && !restoreArgsWrapper.getOffsetMapFileName().isEmpty();
  }

  void saveOffsetMaps(String filePath, Map<String, TopicPartitionToRestore> topicPartitionToRestore) {

    ObjectMapper mapper = new ObjectMapper();

    try {
      BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(filePath));
      for (TopicPartitionToRestore tp : topicPartitionToRestore.values()) {
        String json = mapper.writeValueAsString(
            new OffsetMapInfo(tp.getTopicPartitionId(), tp.getRestoredMessageInfoMap()));
        bufferedWriter.write(json);
        bufferedWriter.newLine();
        bufferedWriter.flush();
      }
      bufferedWriter.close();
    } catch (IOException e) {
      log.error("Could not write offset map to file {}", filePath, e);
      throw new RuntimeException(e);
    }
  }

  void restoreOffsetMaps(String fileName, Map<String, TopicPartitionToRestore> topicPartitionsToRestore) {
    ObjectMapper mapper = new ObjectMapper();

    try {
      BufferedReader reader = new BufferedReader(new FileReader(fileName));
      String line = reader.readLine();

      while (line != null && !line.isEmpty()) {
        OffsetMapInfo offsetMapInfo = mapper.readValue(line, OffsetMapInfo.class);

        topicPartitionsToRestore.get(offsetMapInfo.topicPartitionId)
            .setRestoredMessageInfoMap(offsetMapInfo.offsetMap);

        line = reader.readLine();
      }

    } catch (IOException e) {
      log.error("Could not read offset map from file {}", fileName, e);
      throw new RuntimeException(e);
    }
  }

  @Data
  private static class OffsetMapInfo {

    @JsonCreator
    public OffsetMapInfo(@JsonProperty("topicPartitionId") String topicPartitionId,
        @JsonProperty("offsetMap") Map<Long, Long> offsetMap) {
      this.topicPartitionId = topicPartitionId;
      this.offsetMap = offsetMap;
    }

    String topicPartitionId;
    Map<Long, Long> offsetMap;
  }
}