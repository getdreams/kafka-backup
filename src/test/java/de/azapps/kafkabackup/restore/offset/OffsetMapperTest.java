package de.azapps.kafkabackup.restore.offset;

import static de.azapps.kafkabackup.restore.offset.OffsetMapperParamsHelper.entry;
import static de.azapps.kafkabackup.restore.offset.OffsetMapperParamsHelper.expectedNewOffset;
import static de.azapps.kafkabackup.restore.offset.OffsetMapperParamsHelper.maxOriginalOffset;
import static de.azapps.kafkabackup.restore.offset.OffsetMapperParamsHelper.offsetMapping;
import static de.azapps.kafkabackup.restore.offset.OffsetMapperParamsHelper.oldOffset;
import static de.azapps.kafkabackup.restore.offset.OffsetMapperParamsHelper.singleTestCase;
import static de.azapps.kafkabackup.restore.offset.OffsetMapperParamsHelper.testCases;
import static org.junit.jupiter.api.Assertions.assertEquals;
import java.util.Map;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class OffsetMapperTest {

  private final OffsetMapper sut = new OffsetMapper();


  private static Stream<Arguments> provideTestCases() {
    return testCases(
        singleTestCase(offsetMapping(entry(1,1)), maxOriginalOffset(1), oldOffset(1), expectedNewOffset(1))
    );
  }

  @ParameterizedTest(name = "{index} EXPECTED NEW OFFSET: {3} - when offsetMapping: {0}, maxOriginalOffset: {1}, oldOffset: {2}")
  @MethodSource("provideTestCases")
  public void shouldReturnNewOffset(Map<Long, Long> offsetMap, Long maxOriginalOffset, Long oldOffset,
      Long expectedNewOffset) {
    // given

    // when
    Long newOffsetResult = sut.getNewOffset(offsetMap, maxOriginalOffset, oldOffset);

    // then
    assertEquals(newOffsetResult, expectedNewOffset);
  }

}
