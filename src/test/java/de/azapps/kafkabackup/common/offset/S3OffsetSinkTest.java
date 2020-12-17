package de.azapps.kafkabackup.common.offset;

import com.google.common.collect.ImmutableMap;
import de.azapps.kafkabackup.storage.s3.AwsS3Service;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class S3OffsetSinkTest {

    private S3OffsetSink sut;
    private String bucketName = "random-bucket-name";

    @Mock
    AwsS3Service awsS3Service;

    Clock clock = Clock.fixed(Instant.ofEpochSecond(1234567890), ZoneId.systemDefault());

    @BeforeEach
    public void init() {
        sut = new S3OffsetSink(null, 1L, awsS3Service, bucketName, clock);
    }

    @Test
    public void shouldFlushOffsetToCorrectFile() throws IOException {
        // GIVEN
        String topicName = "abc";
        sut.writeOffsetsForGroup("S3OffsetSinkTest", ImmutableMap.of(new TopicPartition(topicName, 0), new OffsetAndMetadata(0)));

        // WHEN
        sut.flush();

        // THEN
        ArgumentCaptor<String> bucketName = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> fileKey = ArgumentCaptor.forClass(String.class);

        verify(awsS3Service, times(1)).saveFile(bucketName.capture(), fileKey.capture(), any(), any());

        assertEquals("abc/000/1234567890.json", fileKey.getValue());
    }

}
