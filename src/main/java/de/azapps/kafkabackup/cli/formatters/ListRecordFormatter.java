package de.azapps.kafkabackup.cli.formatters;

import de.azapps.kafkabackup.common.record.Record;

import java.io.PrintStream;
import java.text.DateFormat;
import java.text.SimpleDateFormat;

public class ListRecordFormatter extends RecordFormatter {
    private DateFormat timestampFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    public ListRecordFormatter(ByteFormatter keyFormatter, ByteFormatter valueFormatter) {
        super(keyFormatter, valueFormatter);
    }

    @Override
    public void writeTo(Record record, PrintStream outputStream) {
        long kafkaOffset = record.kafkaOffset();
        byte[] key = record.key();
        Long timestamp = record.timestamp();
        byte[] value = record.value();
        outputStream.println(
                "Offset: " + kafkaOffset
                + " Key: " + keyFormatter.toString(key)
                + " Timestamp: " + timestampFormat.format(timestamp)
                + " Data Length: " + (value == null ? "null" : value.length));
    }
}
