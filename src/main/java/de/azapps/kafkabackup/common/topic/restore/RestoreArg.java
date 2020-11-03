package de.azapps.kafkabackup.common.topic.restore;

import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
@Getter
public class RestoreArg {

  private String name;
  private boolean isRequired;

  public static RestoreArg required(String paramName) {
    return new RestoreArg(paramName, true);
  }

  public static RestoreArg optional(String paramName) {
    return new RestoreArg(paramName, false);
  }
}
