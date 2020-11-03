package de.azapps.kafkabackup.restore.topic;


import de.azapps.kafkabackup.restore.RestoreFacade;
import de.azapps.kafkabackup.restore.common.RestoreArgsWrapper;
import java.util.Arrays;

public class RestoreTopicsTask {

  public static void main(String[] args) {
    if (args.length != 1) {
      throw new RuntimeException(String.format("Found %d arguments. Expecting exactly one. Arguments: %s",
          args.length, Arrays.toString(args)));
    }

    System.out.println("Restoring topics with config file: " + args[0]);

    RestoreArgsWrapper restoreTopicsArgsWrapper = RestoreArgsWrapper.of(args[0]);

    System.out.println("Restore configuration: " + restoreTopicsArgsWrapper.toString());


    RestoreFacade restoreFacade = new RestoreFacade(restoreTopicsArgsWrapper);

    restoreFacade.runRestoreProcess();

  }
}
