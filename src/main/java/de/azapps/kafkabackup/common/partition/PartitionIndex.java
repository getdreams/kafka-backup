package de.azapps.kafkabackup.common.partition;

import java.io.EOFException;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class PartitionIndex {
    private Path indexFile;
    private List<PartitionIndexEntry> index = new ArrayList<>();
    private FileOutputStream fileOutputStream;
    private FileInputStream fileInputStream;
    private int position = 0;
    private long latestStartOffset = -1;

    public PartitionIndex(Path indexFile) throws IOException, IndexException {
        this.indexFile = indexFile;
        this.fileInputStream = new FileInputStream(indexFile.toFile());
        this.fileOutputStream = new FileOutputStream(indexFile.toFile(), true);
        fileInputStream.getChannel().position(0);
        while (true) {
            try {
                PartitionIndexEntry partitionIndexEntry = PartitionIndexEntry.fromStream(fileInputStream);
                if (partitionIndexEntry.startOffset() <= latestStartOffset) {
                    throw new IndexException("Offsets must be always increasing! There is something terribly wrong in your index " + indexFile + "! Got " + partitionIndexEntry.startOffset() + " expected an offset larger than " + latestStartOffset);
                }
                index.add(partitionIndexEntry);
                latestStartOffset = partitionIndexEntry.startOffset();
            } catch (EOFException e) {
                // reached End of File
                break;
            }
        }
    }

    void appendSegment(String segmentFile, long startOffset) throws IOException, IndexException {
        if (startOffset <= latestStartOffset) {
            throw new IndexException("Offsets must be always increasing! There is something terribly wrong in your index " + indexFile + "! Got " + startOffset + " expected an offset larger than " + latestStartOffset);
        }
        PartitionIndexEntry indexEntry = new PartitionIndexEntry(fileOutputStream, segmentFile, startOffset);
        index.add(indexEntry);
        latestStartOffset = startOffset;
    }

    Optional<PartitionIndexEntry> latestSegmentFile() {
        if (index.isEmpty()) {
            return Optional.empty();
        } else {
            return Optional.of(index.get(index.size() - 1));
        }
    }

    long latestStartOffset() {
        return latestStartOffset;
    }

    void close() throws IOException {
        fileInputStream.close();
        fileOutputStream.close();
    }

    void flush() throws IOException {
        fileOutputStream.flush();
    }

    public static class IndexException extends Exception {
        IndexException(String message) {
            super(message);
        }
    }

    long firstOffset() throws IndexException {
        if (index.size() == 0) {
            throw new PartitionIndex.IndexException("Partition Index is empty. Something is wrong with your partition index. Try to rebuild the index " + indexFile);
        }
        return index.get(0).startOffset();
    }

    void seek(long offset) throws PartitionIndex.IndexException {
        int previousPosition = -1;
        // Iterate the index after the last element
        // Such that we can seek to an offset in the last index entry
        for (int i = 0; i <= index.size(); i++) {
            if (i == index.size()) {
                // Offset must be in the last index entry
                position = previousPosition;
            } else {
                PartitionIndexEntry current = index.get(i);
                if (current.startOffset() > offset) {
                    if (previousPosition >= 0) {
                        position = previousPosition;
                        //
                        return;
                    } else {
                        throw new PartitionIndex.IndexException("No Index file found matching the target offset in partition index " + indexFile + ". Search for offset " + offset + ", smallest offset in index: " + current.startOffset());
                    }
                } else {
                    previousPosition = i;
                }
            }
        }
    }

    boolean hasMoreData() {
        return position < index.size();
    }

    String readFileName() {
        String fileName = index.get(position).filename();
        position++;
        // allow the cursor to be one after the index size.
        // This way we can detect easier when we reached the end of the index
        if (position > index.size()) {
            throw new IndexOutOfBoundsException("Index " + indexFile + " out of bound");
        }
        return fileName;
    }

    public List<PartitionIndexEntry> index() {
        return index;
    }
}