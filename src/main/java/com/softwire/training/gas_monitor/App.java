package com.softwire.training.gas_monitor;


import com.amazonaws.services.sns.AmazonSNS;
import com.amazonaws.services.sns.AmazonSNSClientBuilder;
import com.amazonaws.services.sns.util.Topics;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.List;

/**
 * Hello world!
 *
 */
public class App {

    private static final Logger LOGGER = LogManager.getLogger(S3BucketFetcher.class);
    private static final String TOPIC_ARN = "arn:aws:sns:eu-west-2:099421490492:EventProcessing-snsTopicSensorDataPart1-QVDE0JIXZS1V";

    public static void main(String[] args) throws IOException {

        S3BucketFetcher s3BucketFetcher = new S3BucketFetcher();

        MonitorLocation[] locations = s3BucketFetcher.fileFetcher();

        AmazonSQS sqs = AmazonSQSClientBuilder.standard().build();
        AmazonSNS sns = AmazonSNSClientBuilder.standard().build();

        String myQueueUrl = sqs.createQueue(new CreateQueueRequest("jedQueue1")).getQueueUrl();
        String subscriptionArn = null;

        try {

            subscriptionArn = Topics.subscribeQueue(sns, sqs, TOPIC_ARN, myQueueUrl);
            long currentTime = System.currentTimeMillis();
            long endTime = currentTime + 15000;

            while(System.currentTimeMillis() < endTime) {

                ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(myQueueUrl).withWaitTimeSeconds(5);

                List<Message> messages = sqs.receiveMessage(receiveMessageRequest).getMessages();
                LOGGER.info(messages);
            }
        } finally {
            sqs.deleteQueue(myQueueUrl);
            LOGGER.info(String.format("Deleted queue %s",myQueueUrl));

            if (subscriptionArn != null) {
                sns.unsubscribe(subscriptionArn);
                LOGGER.info(String.format("Unsubscribed from topic %s", subscriptionArn));
            }
        }
    }
}
