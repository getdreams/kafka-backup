package de.azapps.kafkabackup.restore.common;

import com.google.common.collect.Lists;
import java.util.Arrays;
import java.util.List;
import lombok.Builder;
import lombok.Getter;

@Getter
@Builder
public class RestoreArg {

  private List<String> names;
  private List<String> allowedValues;
  private boolean isRequired;

  public static RestoreArg param(RestoreArgBuilder argBuilder) {
    return argBuilder.build();
  }

  public static RestoreArgBuilder singleParam(String name) {
    return RestoreArg.builder()
        .names(Lists.newArrayList(name));
  }

  public static RestoreArgBuilder optional() {
    return RestoreArg.builder()
        .isRequired(false);
  }

  public static RestoreArg optional(RestoreArgBuilder argBuilder) {
    return argBuilder
        .isRequired(false)
        .build();
  }
}
