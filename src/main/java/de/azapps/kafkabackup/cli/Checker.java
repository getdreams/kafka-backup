package de.azapps.kafkabackup.cli;

import de.azapps.kafkabackup.cli.formatters.ListRecordFormatter;
import de.azapps.kafkabackup.cli.formatters.RawFormatter;
import de.azapps.kafkabackup.cli.formatters.RecordFormatter;
import de.azapps.kafkabackup.cli.formatters.UTF8Formatter;
import de.azapps.kafkabackup.common.record.Record;
import de.azapps.kafkabackup.common.segment.UnverifiedSegmentReader;
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

public class Checker {

  private static HttpClient client = HttpClient.newHttpClient();

  private static List<String> IGNORE_TOPICS = List.of(
      "connect.status",
      "connect.config",
      "connect-cluster-2-offsets",
      "connect-cluster-2-status",
      "connect-cluster-2-config"
  );
  public static void main(String[] args) throws Exception {

    final String backupFolder = "connect/tmp/backup";

    String[] directories = new File(backupFolder).list();

    for (String topic : directories) {
      if(IGNORE_TOPICS.contains(topic)) {
        continue;
      }

      try {
        System.out.print("Checking topic: " + topic);
        int partitionCount = 0;
        final List<String> partitions = List.of("000", "001", "002");

        List<WrappedResponse> checks = new ArrayList<>();
        for (String partition : partitions) {
          String fileName =
              backupFolder + "/" + topic + "/segment_partition_" + partition + "_from_offset_0000000000_records";

          Path recordFile = Paths.get(fileName);
          if (recordFile.toFile().isFile()) {
            partitionCount++;
            UnverifiedSegmentReader segmentReader = new UnverifiedSegmentReader(recordFile);
            checks.add(list(segmentReader, partition));
          }

        }
        System.out.print(" with " + partitionCount + " partitions");

        long totalRecords = checks.stream()
            .map(WrappedResponse::getCount)
            .reduce(0L, Long::sum);
        long shouldBe = getNumberOfRecords(topic);
        if (totalRecords != shouldBe) {
          System.out.println("\t\t\t\t\tWRONG");
          System.out.println(String.format("\nShould be %s records, but backed up %s\n", shouldBe, totalRecords));

          checks.forEach(c ->
              System.out.println(c.getOutputStream())
          );
        } else {
          System.out.println("\t\t\t\t\tOK");
        }
      } catch (Exception e) {
        throw e;
      }
    }
  }

  private static long getNumberOfRecords(String topic) throws IOException, InterruptedException {
    HttpRequest request = HttpRequest.newBuilder()
        .uri(URI.create("http://localhost:8094/kafka/topic/count?name=" + topic + "&profile=FIMS&server=FIMS-prod"))
        .build();

    HttpResponse<String> response = client.send(request,
        HttpResponse.BodyHandlers.ofString());
    return Long.parseLong(response.body());
  }


  private static WrappedResponse list(UnverifiedSegmentReader segmentReader, String partition) {
    final RecordFormatter formatter = new ListRecordFormatter(new UTF8Formatter(), new RawFormatter());
    final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    final PrintStream printStream = new PrintStream(byteArrayOutputStream);

    int cnt = 0;
    while (true) {
      try {
        Record record = segmentReader.read();
        printStream.print("Partition: " + partition + " ");
        formatter.writeTo(record, printStream);
        cnt++;
      } catch (EOFException e) {
        break;
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
    if (cnt > 0) {
      printStream.println("Total: " + cnt);
    }
    return new WrappedResponse(cnt, byteArrayOutputStream);
  }

  private static class WrappedResponse {

    private long count;
    private ByteArrayOutputStream outputStream;

    public WrappedResponse(long count, ByteArrayOutputStream outputStream) {
      this.count = count;
      this.outputStream = outputStream;
    }

    public ByteArrayOutputStream getOutputStream() {
      return outputStream;
    }

    public long getCount() {
      return count;
    }
  }

}
