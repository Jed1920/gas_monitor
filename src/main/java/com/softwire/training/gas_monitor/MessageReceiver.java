package com.softwire.training.gas_monitor;

import com.amazonaws.services.sns.AmazonSNS;
import com.amazonaws.services.sns.AmazonSNSClientBuilder;
import com.amazonaws.services.sns.util.Topics;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.softwire.training.gas_monitor.Models.MonitorLocation;
import com.softwire.training.gas_monitor.Models.SensorReading;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class MessageReceiver {
    private static final Logger LOGGER = LogManager.getLogger(MessageReceiver.class);
    private static final String TOPIC_ARN = "arn:aws:sns:eu-west-2:099421490492:EventProcessing-snsTopicSensorDataPart1-QVDE0JIXZS1V";

    private final AmazonSQS sqs;
    private final AmazonSNS sns;
    private final SensorReadingFetcher sensorReadingFetcher;

    public MessageReceiver(AmazonSQS sqsClient, AmazonSNS snsClient, SensorReadingFetcher sensorReadingFetcher) throws Exception {
        this.sqs = sqsClient;
        this.sns = snsClient;
        this.sensorReadingFetcher = sensorReadingFetcher;
    }

    public List<SensorReading> getListofSensorReadings(String myQueueUrl, List<String> validLocationIds) throws Exception {

        String subscriptionArn = null;
        List<SensorReading> matchedReadings = new ArrayList<>();


        LOGGER.info(validLocationIds);

        try {
            subscriptionArn = Topics.subscribeQueue(sns, sqs, TOPIC_ARN, myQueueUrl);

            long currentTime = Clock.systemDefaultZone().millis();
            long endTime = currentTime + 15000;

            while (Clock.systemDefaultZone().millis() < endTime) {
                matchedReadings.addAll(sensorReadingFetcher.fetchReadings(myQueueUrl,validLocationIds));
            }
        } finally {
            sqs.deleteQueue(myQueueUrl);
            LOGGER.info(String.format("Deleted queue %s", myQueueUrl));

            if (subscriptionArn != null) {
                sns.unsubscribe(subscriptionArn);
                LOGGER.info(String.format("Unsubscribed from topic %s", subscriptionArn));
            }
        }
        return matchedReadings;
    }


}
