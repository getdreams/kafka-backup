## Offset and message backup time discrepancies

### Problem description
Currently, kafka-backup process  uses two different mechanisms for backing up offsets and messages.

Message backup utilizes sink connector mechanism but offsets are read from cluster via an administrative client for kafka cluster.
This means that while messages are read continuously (though they are saved in batches based on a batch size and time), 
the offsets are being written to S3 with scheduled time intervals.

As a result, when the restoration process is finished, information regarding offsets for consumer groups may not be accurate if any of consumers was still consuming messages after last offset backup was created.

That in  turn  would introduce a need to examin system logs, and all accessible data states to verify whether some of the messages had already been consumed.

### Possible improvement 

There is another approach that could minimize the risk of offsets and messages back-ups being out of sync.

By implementing a simple consumer that reads the internal topic ``__consumer_offsets`` we can get a similarly continuous image of offset commits as for messages.
The problems to overcome would be:
- aggregating all consumer groups to a single file per topic partition - this would probably lead to changing offset backup structure because we are preserving the whole offsets history to allow restoring to a point in time
- under the heavy load with many consumers probably few nodes would be required to process all messages
- ``__consumer_offsets`` is an internal kafka topic and its format may change which could break the backup if the library is not updated.
   
 
