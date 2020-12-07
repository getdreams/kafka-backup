package de.azapps.kafkabackup.restore.offset;

import java.util.Map;

public class OffsetMapper {

  public Long getNewOffset(Map<Long, Long> offsetMap, long maxOriginalOffset, long oldOffset) {
    if (offsetMap.containsKey(oldOffset)) {
      return offsetMap.get(oldOffset);
    } else {
      if (oldOffset <= 0) {
        // Its always safe to map 0 -> 0, and needed for empty topics with commits to 0.
        return 0L;
      } else if (oldOffset > maxOriginalOffset) {
        // The high watermark will not match a restored message, so we need to find the *new* high watermark
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
