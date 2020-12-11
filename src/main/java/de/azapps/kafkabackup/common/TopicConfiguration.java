package de.azapps.kafkabackup.common;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Map;
import java.util.TreeMap;
import lombok.Getter;
import lombok.ToString;

@Getter
@ToString
public class TopicConfiguration {

  @JsonCreator
  public TopicConfiguration(
      @JsonProperty("topicName") String topicName,
      @JsonProperty("partitionsNumber") int partitionsNumber,
      @JsonProperty("replicationFactor") int replicationFactor) {
    this.topicName = topicName;
    this.partitionsNumber = partitionsNumber;
    this.replicationFactor = replicationFactor;
  }

  private final String topicName;
  private final int partitionsNumber;
  private final int replicationFactor;
  private Map<String, String> configuration;

  public void setConfiguration(Map<String, String> configuration) {
    this.configuration = new TreeMap<>(configuration);
  }
}
