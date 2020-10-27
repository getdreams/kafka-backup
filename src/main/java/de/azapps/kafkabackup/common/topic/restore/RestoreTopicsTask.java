package de.azapps.kafkabackup.common.topic.restore;

public class RestoreTopicsTask {

  public static void main(String[] args) {
    System.out.println("Restoring topics with config file: " + args[0]);

    RestoreTopicsArgsWrapper restoreTopicsArgsWrapper = new RestoreTopicsArgsWrapper();
    restoreTopicsArgsWrapper.readProperties(args[0]);

    System.out.println("Restore configuration: " + restoreTopicsArgsWrapper.toString());

  }
}
