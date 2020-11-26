package de.azapps.kafkabackup.restore.offset;

import static de.azapps.kafkabackup.restore.offset.OffsetMapperParamsHelper.expectedNewOffset;
import static de.azapps.kafkabackup.restore.offset.OffsetMapperParamsHelper.maxOriginalOffset;
import static de.azapps.kafkabackup.restore.offset.OffsetMapperParamsHelper.offsetMap;
import static de.azapps.kafkabackup.restore.offset.OffsetMapperParamsHelper.entry;
import static de.azapps.kafkabackup.restore.offset.OffsetMapperParamsHelper.oldOffset;
import static de.azapps.kafkabackup.restore.offset.OffsetMapperParamsHelper.singleTestCase;
import static de.azapps.kafkabackup.restore.offset.OffsetMapperParamsHelper.testCases;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import java.util.Collections;
import java.util.Map;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class OffsetMapperTest {

  private final OffsetMapper sut = new OffsetMapper();


  private static Stream<Arguments> provideTestCases() {
    return testCases(
        singleTestCase(
            "clusters matching 1 to 1",
            offsetMap(entry(0,0), entry(2,2)),
            oldOffset(2),
            expectedNewOffset(2)),

        singleTestCase(
            "non transactional to transactional producer",
            offsetMap(entry(0,0), entry(1,2)),
            oldOffset(2),
            expectedNewOffset(3)),

        singleTestCase(
            "offset committed to not restored message",
            offsetMap(entry(10,20)),
            oldOffset(5),
            expectedNewOffset(20)),

        singleTestCase(
            "0 offset committed for empty topic",
            Collections.emptyMap(),
            oldOffset(0),
            expectedNewOffset(0)),

        singleTestCase(
            "0 offset committed to not empty topic",
            offsetMap(entry(0,0), entry(2,2)),
            oldOffset(0),
            expectedNewOffset(0)),

        singleTestCase(
            "Exception when map not contains max original offset",
            offsetMap(entry(0,0), entry(1,2)),
            maxOriginalOffset(5),
            oldOffset(4),
            expectedNewOffset(5),
            RuntimeException.class)
    );
  }

  @ParameterizedTest(name = "{0} EXPECTED NEW OFFSET: {4} - when offsetMapping: {1}, maxOriginalOffset: {2}, oldOffset: {3}")
  @MethodSource("provideTestCases")
  public <T extends Throwable> void shouldReturnNewOffset(String description, Map<Long, Long> offsetMap,
      Long maxOriginalOffset, Long oldOffset, Long expectedNewOffset, Class<T> expectedError) {
    // given

    // when/then
    if (expectedError != null) {
      assertThrows(expectedError, () -> sut.getNewOffset(offsetMap, maxOriginalOffset, oldOffset));
    }
    else {
      Long newOffsetResult = sut.getNewOffset(offsetMap, maxOriginalOffset, oldOffset);
      assertEquals(expectedNewOffset, newOffsetResult, "When testing: " + description);
    }
  }

}
