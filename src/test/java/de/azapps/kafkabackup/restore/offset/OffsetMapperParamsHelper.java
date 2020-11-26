package de.azapps.kafkabackup.restore.offset;

import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.Map;
import java.util.stream.Stream;
import org.junit.jupiter.params.provider.Arguments;

public class OffsetMapperParamsHelper {
  static Long maxOriginalOffset(long offset) {
    return offset;
  }

  static Long oldOffset(long offset) {
    return offset;
  }

  static Long expectedNewOffset(long offset) {
    return offset;
  }

  static SimpleImmutableEntry<Long, Long> entry(long offsetKey, long offsetValue) {
    return new SimpleImmutableEntry<>(offsetKey, offsetValue);
  }

  static Map<Long, Long> offsetMapping(SimpleImmutableEntry<Long, Long>... entries) {
    return Map.ofEntries(entries);
  }

  static Arguments singleTestCase(Map<Long, Long> offsetMap, Long maxOriginalOffset, Long oldOffset,
      Long expectedNewOffset) {
    return Arguments.of(offsetMap, maxOriginalOffset, oldOffset, expectedNewOffset);
  }

  static Stream<Arguments> testCases(Arguments... arguments) {
    return Stream.of(arguments);
  }
}
