package de.azapps.kafkabackup.restore.topic;


import de.azapps.kafkabackup.restore.RestoreFacade;
import de.azapps.kafkabackup.restore.common.RestoreArgsWrapper;
import java.util.Arrays;

public class RestoreProcessTask {

  public static void main(String[] args) {
    if (args.length != 1) {
      throw new RuntimeException(String.format("Found %d arguments. Expecting exactly one. Arguments: %s",
          args.length, Arrays.toString(args)));
    }

    System.out.println("Restoring backup with config file: " + args[0]);

    RestoreArgsWrapper restoreArgsWrapper = RestoreArgsWrapper.of(args[0]);

    System.out.println("Restore configuration: " + restoreArgsWrapper.toString());

    RestoreFacade restoreFacade = RestoreFacade.initialize(restoreArgsWrapper);

    restoreFacade.runRestoreProcess(restoreArgsWrapper);
  }
}
