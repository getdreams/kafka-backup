package de.azapps.kafkabackup.restore.offset;

import com.google.common.collect.ImmutableMap;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
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

  static Map<Long, Long> offsetMap(SimpleImmutableEntry<Long, Long>... entries) {
    List<SimpleImmutableEntry<Long, Long>> entryList = Stream.of(entries)
        .collect(Collectors.toList());
    return ImmutableMap.copyOf(entryList);
  }

  static Arguments singleTestCase(String description, Map<Long, Long> offsetMap, Long oldOffset,
      Long expectedNewOffset) {
    long maxOriginalOffset = offsetMap.keySet().stream().max(Comparator.naturalOrder()).orElse(-1L);

    return Arguments.of(description, offsetMap, maxOriginalOffset, oldOffset, expectedNewOffset, null);
  }

  static <T extends Throwable> Arguments singleTestCase(String description, Map<Long, Long> offsetMap,
      Long maxOriginalOffset, Long oldOffset, Long expectedNewOffset, Class<T> expectedError) {

    return Arguments.of(description, offsetMap, maxOriginalOffset, oldOffset, expectedNewOffset, expectedError);
  }

  static <T extends Throwable> Arguments singleTestCase(String description, Map<Long, Long> offsetMap, Long oldOffset,
      Long expectedNewOffset, Class<T> expectedError) {
    long maxOriginalOffset = offsetMap.keySet().stream().max(Comparator.naturalOrder()).orElse(-1L);

    return Arguments.of(description, offsetMap, maxOriginalOffset, oldOffset, expectedNewOffset, expectedError);
  }

  static Stream<Arguments> testCases(Arguments... arguments) {
    return Stream.of(arguments);
  }
}
