package de.azapps.kafkabackup.restore;

import static org.junit.jupiter.api.Assertions.*;
import de.azapps.kafkabackup.restore.common.RestoreArgsWrapper;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import org.junit.jupiter.api.Test;

class RestoreArgsWrapperTest {

  @Test
  public void shouldThrowExceptionIfSomeRequiredPropertyIsNotProvided() throws IOException {
    // given
    File test = File.createTempFile("test", ".config");
    Files.write(test.toPath(),
        ("aws.s3.region=region"
            + "\nkafka.bootstrap.servers=server1"
            + "\naws.s3.bucketNameForConfig=bucketName"
            + "\nrestore.hash=hash"
        ).getBytes());
    // when
    RuntimeException runtimeException = assertThrows(RuntimeException.class,
        () -> RestoreArgsWrapper.of(test.getPath()));

    // then
    assertEquals(runtimeException.getMessage(), "Missing required property: restore.mode");
  }

}