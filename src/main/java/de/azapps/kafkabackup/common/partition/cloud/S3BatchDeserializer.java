package de.azapps.kafkabackup.common.partition.cloud;

import static de.azapps.kafkabackup.common.partition.cloud.S3BatchWriter.UTF8_UNIX_LINE_FEED;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import de.azapps.kafkabackup.common.record.Record;
import de.azapps.kafkabackup.common.record.RecordJSONSerde;
import de.azapps.kafkabackup.storage.s3.AwsS3Service;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import lombok.Getter;
import lombok.SneakyThrows;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.RetriableException;

public class S3BatchDeserializer {

    private final RecordJSONSerde recordJSONSerde = new RecordJSONSerde();

    public List<Record> deserialize(InputStream inputStream) throws IOException {
        byte[] batch = inputStream.readAllBytes();

        List<Record> records = new ArrayList<>();

        int lastRecordStart = 0;

        for (int i = 0; i < batch.length; i++) {
            if (batch[i] == UTF8_UNIX_LINE_FEED) {
                ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(batch, lastRecordStart, i);
                Record record = recordJSONSerde.read(byteArrayInputStream);
                records.add(record);

                byteArrayInputStream.close();
                lastRecordStart = i + 1;
            }
        }

        return records;
    }
}
