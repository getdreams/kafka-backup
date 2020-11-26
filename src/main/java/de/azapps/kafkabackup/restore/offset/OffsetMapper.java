package de.azapps.kafkabackup.restore.offset;

import java.util.Map;

public class OffsetMapper {

  public Long getNewOffset(Map<Long, Long> offsetMap, long maxOriginalOffset, long oldOffset) {
    if (offsetMap.containsKey(oldOffset)) {
      return offsetMap.get(oldOffset);
    } else {
      // The high watermark will not match a restored message, so we need to find the *new* high watermark
      if (oldOffset > maxOriginalOffset) {
        if (offsetMap.containsKey(maxOriginalOffset)) {
          return offsetMap.get(maxOriginalOffset) + 1;
        } else {
          throw new RuntimeException(
              "Could not find mapped offset in restored cluster, and could not map to a new high watermark either");
        }
      } else {
        return getNewOffset(offsetMap, maxOriginalOffset, oldOffset + 1);
      }
    }
  }
}
