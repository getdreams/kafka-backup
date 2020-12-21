## Offset and message backup time discrepancies

### Problem description
Currently, kafka-backup process  uses two different mechanism for backing up offsets and messages.

Message backup utilizes sink connector mechanism and offsets are read from cluster via an administrative client for kafka cluster.
This means that while messages are read continuously (though tey are saved in batches based on batch size and time), 
the offsets are being written to S3 with scheduled time intervals.

As a result, after restoration process information regarding offsets for consumer groups me may not be accurate if any of consumers where working after last offset backup ws created.

That in  turn  would introduce a need to examin system logs, and all accessible data states to verify whether some of the messages had already been consumed.

### Possible improvement 

There is another approach that could minimize risk of offsets and messages back-ups being out of sync.

By implementing simple consumer that will read the internal topic ``__consumer_offsets`` we can get a similarly continuous image off offset commits as for messages.
The problems to overcome would be:
- aggregating all consumer groups to single file per topic partition
- under the heavy load with many consumers probably few nodes would be required to process all messages

   
 