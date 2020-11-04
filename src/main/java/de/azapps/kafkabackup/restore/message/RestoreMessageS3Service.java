package de.azapps.kafkabackup.restore.message;

import static de.azapps.kafkabackup.common.partition.cloud.S3BatchWriter.UTF8_UNIX_LINE_FEED;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import de.azapps.kafkabackup.common.partition.cloud.S3BatchDeserializer;
import de.azapps.kafkabackup.common.record.Record;
import de.azapps.kafkabackup.storage.s3.AwsS3Service;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class RestoreMessageS3Service {

  private final AwsS3Service awsS3Service;
  private final String bucketName;
  private final S3BatchDeserializer s3BatchDeserializer = new S3BatchDeserializer();

  public RestoreMessageS3Service(AwsS3Service awsS3Service, String bucketName) {
    this.awsS3Service = awsS3Service;
    this.bucketName = bucketName;
  }


  public List<String> getMessageBackupFileNames(String topicName, int partition) {
    final String prefix = String.format("%s/%03d/", topicName, partition);

    return awsS3Service.getBucketObjectKeys(bucketName, prefix, "/")
        .stream()
        .filter(name -> name.contains("msg_"))
        .sorted(Comparator.naturalOrder())
        .collect(Collectors.toList()); //TODO remove when offsets moved to separate bucket;
  }

  public List<Record> readBatchFile(String key) {
    log.debug("Downloading batch file {}.", key);
    S3Object file = awsS3Service.getFile(bucketName, key);
    try {
      S3ObjectInputStream is = file.getObjectContent();

      return s3BatchDeserializer.deserialize(is);

    } catch (IOException e) {
      throw new RuntimeException("Unable to parse file: " + file.getKey(), e);
    }
  }
}
