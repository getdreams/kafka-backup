package de.azapps.kafkabackup.restore;

import com.amazonaws.services.s3.model.S3Object;
import com.fasterxml.jackson.databind.ObjectMapper;
import de.azapps.kafkabackup.common.AdminClientService;
import de.azapps.kafkabackup.common.TopicsConfig;
import de.azapps.kafkabackup.storage.s3.AwsS3Service;
import java.io.IOException;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class RestoreConfigurationHelper {

  private final AwsS3Service awsS3Service;
  private final ObjectMapper objectMapper = new ObjectMapper();

  public TopicsConfig getTopicsConfig(String configChecksum, String bucketWithTopicConfigs) {
    S3Object file = awsS3Service.getFile(bucketWithTopicConfigs, configChecksum + ".json");
    try {
      return objectMapper.readValue(file.getObjectContent(), TopicsConfig.class);
    } catch (IOException e) {
      throw new RuntimeException("Unable to parse file: " + file.getKey(), e);
    }
  }
}
