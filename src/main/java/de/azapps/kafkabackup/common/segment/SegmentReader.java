package de.azapps.kafkabackup.common.segment;

import de.azapps.kafkabackup.common.record.Record;
import de.azapps.kafkabackup.common.record.RecordSerde;

import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class SegmentReader {
    private String topic;
    private int partition;
    private String filePrefix;
    private SegmentIndex segmentIndex;
    private FileInputStream recordInputStream;
    private long lastValidStartPosition;

    public SegmentReader(String topic, int partition, Path topicDir, long startOffset) throws IOException, SegmentIndex.IndexException {
        this(topic, partition, topicDir, SegmentUtils.filePrefix(partition, startOffset));
    }

    public SegmentReader(String topic, int partition, Path topicDir, String filePrefix) throws IOException, SegmentIndex.IndexException {
        this.topic = topic;
        this.partition = partition;
        this.filePrefix = filePrefix;

        File indexFile = SegmentUtils.indexFile(topicDir, filePrefix);
        File recordFile = SegmentUtils.recordsFile(topicDir, filePrefix);
        if (!indexFile.exists()) {
            throw new RuntimeException("Index for Segment not found: " + indexFile.toString());
        }
        if (!recordFile.exists()) {
            throw new RuntimeException("Segment not found: " + recordFile.toString());
        }
        segmentIndex = new SegmentIndex(indexFile);
        recordInputStream = new FileInputStream(recordFile);
        lastValidStartPosition = segmentIndex.lastValidStartPosition();
    }

    public void seek(long offset) throws IndexOutOfBoundsException, IOException {
        Optional<Long> optionalPosition = segmentIndex.findByOffset(offset);
        if (optionalPosition.isEmpty()) {
            throw new IndexOutOfBoundsException("Could not find offset " + offset + " in topic " + topic + ", segment " + filePrefix);
        }
        recordInputStream.getChannel().position(optionalPosition.get());
    }

    public boolean hasMoreData() throws IOException {
        return recordInputStream.getChannel().position() <= lastValidStartPosition;
    }

    public Record read() throws IOException {
        if (!hasMoreData()) {
            throw new EOFException("Already read the last valid record in topic " + topic + ", segment " + filePrefix);
        }
        return RecordSerde.read(topic, partition, recordInputStream);
    }

    public List<Record> readN(int n) throws IOException {
        List<Record> records = new ArrayList<>(n);
        while (hasMoreData() && records.size() < n) {
            Record record = read();
            records.add(record);
        }
        return records;
    }

    public List<Record> readFully() throws IOException {
        List<Record> records = new ArrayList<>(segmentIndex.size());
        while (hasMoreData()) {
            Record record = read();
            records.add(record);
        }
        return records;
    }

    public void close() throws IOException {
        recordInputStream.close();
        segmentIndex.close();
    }
}