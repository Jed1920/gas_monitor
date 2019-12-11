package com.softwire.training.gas_monitor;

import com.amazonaws.services.sns.AmazonSNS;
import com.amazonaws.services.sns.util.Topics;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.softwire.training.gas_monitor.Models.MonitorLocation;
import com.softwire.training.gas_monitor.Models.SensorReading;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.List;

public class MessageReceiver {
    private static final Logger LOGGER = LogManager.getLogger(MessageReceiver.class);
    private static final String TOPIC_ARN = "arn:aws:sns:eu-west-2:099421490492:EventProcessing-snsTopicSensorDataPart1-QVDE0JIXZS1V";

    private final AmazonSQS sqs;
    private final AmazonSNS sns;
    private final SensorReadingFetcher sensorReadingFetcher;

    public MessageReceiver(AmazonSQS sqs, AmazonSNS sns, SensorReadingFetcher sensorReadingFetcher) throws Exception {
        this.sqs = sqs;
        this.sns = sns;
        this.sensorReadingFetcher = sensorReadingFetcher;
    }

    public List<SensorReading> run() throws Exception {
        String myQueueUrl = sqs.createQueue(new CreateQueueRequest("jedQueue1")).getQueueUrl();
        String subscriptionArn = null;
        List<SensorReading> readings = new ArrayList<>();

        try {
            subscriptionArn = Topics.subscribeQueue(sns, sqs, TOPIC_ARN, myQueueUrl);

            long currentTime = System.currentTimeMillis();
            long endTime = currentTime + 15000;

            while (System.currentTimeMillis() < endTime) {
                readings = sensorReadingFetcher.fetchReadings(myQueueUrl);
            }
        } finally {
            sqs.deleteQueue(myQueueUrl);
            LOGGER.info(String.format("Deleted queue %s", myQueueUrl));

            if (subscriptionArn != null) {
                sns.unsubscribe(subscriptionArn);
                LOGGER.info(String.format("Unsubscribed from topic %s", subscriptionArn));
            }
        }
        return readings;
    }
}
