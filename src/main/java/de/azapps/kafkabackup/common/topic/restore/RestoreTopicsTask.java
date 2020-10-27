package de.azapps.kafkabackup.common.topic.restore;

import com.sun.tools.javac.util.ArrayUtils;
import java.util.Arrays;

public class RestoreTopicsTask {

  public static void main(String[] args) {
    if (args.length != 1) {
      throw new RuntimeException(String.format("Found %d arguments. Expecting exactly one. Arguments: %s",
          args.length, Arrays.toString(args)));
    }

    System.out.println("Restoring topics with config file: " + args[0]);

    RestoreTopicsArgsWrapper restoreTopicsArgsWrapper = new RestoreTopicsArgsWrapper();
    restoreTopicsArgsWrapper.readProperties(args[0]);

    System.out.println("Restore configuration: " + restoreTopicsArgsWrapper.toString());

  }
}
