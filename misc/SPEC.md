# Nested STAR Collector/Aggregator

Created: March - April 2022

Status: **Ready for review**

Prod: Alex, Ralph, Philipp, Pete\
Design/Eng: Darnell\
Privacy/Security: Pete\
QA: ?

## Background

The nested STAR protocol will be used to increase anonymity protections for users who opt-in to P3A. The server/aggregator will collect the protected/encrypted measurements and attempt to recover their decrypted values. This new P3A method will be deployed on all platforms.

## Summary

1. Brave users with P3A reporting enabled will encrypt analytics data using the STAR method. Each question report will result in one nested STAR message being sent to the server-side collector. Each message will contain one key share for each layer/attribute of the measurement. Messages will be encoded using bincode, and then encoded again in base64.
2. The server/collector will run in EKS/Fargate and will collect the messages over HTTPS. Messages will be pushed to a topic on an MSK/Kafka cluster. Optionally, we can run the server/collector in a Nitro Enclave so users can verify that we don't collect identifiable information such as the IP address.
3. The aggregator will run in an EKS cronjob on a high memory EC2 instance (16 vCPUs, 128GB memory). This job will run every 1 - 2 days. The EC2 instance will terminate after the process has completed to reduce cost. See "Aggregation process" for more information on this step.
4. The data lake sink process will run in EKS/Fargate, and continuously consume messages from the decrypted topic in MSK/Kafka, and store them in S3.
5. Data can be imported into Redshift from S3, or queried via Athena.

![Data flow](flow.drawio.png)

## External services required

- EKS/Fargate/EC2
- RDS/Postgres
- MSK/Kafka
- S3

## Aggregation process

1. The process starts by consuming & parsing many messages from the Kafka topic (max number of messages defined by CLI argument), and keeping them in memory. Messages with the same decrypted attribute for the outer layer will have the same 20-byte tag. This tag is included in the message and does not required decrypting. Messages will be grouped by their respective tags (the "tag/messages grouping").
2. For each tag...
    1. The recovered message from PG will be retrieved, if it exists. A recovered message includes the decrypted metric name/value, full key and unreported count. These recovered messages will have been inserted by previous invocations of the process.
    2. Pending messages from PG will be retrieved. A pending message is an unrecovered message that has been consumed from the Kafka topic in a previous invocation, but did not meet the _k_ threshold.
    3. If a recovered message does not exist, and total number of messages is greater than or equal to the _k_ threshold, key recovery will be attempted.
    4. Messages will be decrypted and the messages included in the next nested layer will be added to a new tag/messages grouping.
    4. A new recovered message will be created if it does not exist. The recovered message's unreported count will be increased by the number of new messages processed. This new or updated recovered message will be stored in-memory and not stored in PG until later.
3. Store new pending messages for tags that have not met _k_.
4. Repeat step 2 for the new tag/messages grouping. If there are no more tags that meet _k_, or have an existing recovered message, go to step 5.
5. Iterate through the recovered messages stored in memory, and report "full measurements" (measurements where all layers/attributes have been decrypted) to the decrypted Kafka topic.
6. Check for expired epochs. For each expired epoch, retrieve all recovered measurements, iterate through them and report "partial measurements" (measurements where some layers have been decrypted).
