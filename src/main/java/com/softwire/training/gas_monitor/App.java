package com.softwire.training.gas_monitor;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.sns.AmazonSNS;
import com.amazonaws.services.sns.AmazonSNSClientBuilder;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.softwire.training.gas_monitor.Models.SensorReading;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.List;

public class App {

    private static final Logger LOGGER = LogManager.getLogger(App.class);

    public static void main(String[] args) throws Exception {

        AmazonSQS sqsClient = AmazonSQSClientBuilder.standard().build();
        AmazonSNS snsClient = AmazonSNSClientBuilder.standard().build();
        SensorReadingFetcher sensorReadingFetcher = new SensorReadingFetcher(sqsClient);

        AmazonS3 s3Client = AmazonS3ClientBuilder.standard().withRegion(Regions.EU_WEST_2).build();
        S3BucketFetcher s3BucketFetcher = new S3BucketFetcher(s3Client);

        MessageReceiver messageReceiver = new MessageReceiver(sqsClient,snsClient,sensorReadingFetcher, s3BucketFetcher);
        List<SensorReading> readings = messageReceiver.getListofSensorReadings();
        LOGGER.info("  Total number of matched sensor reading:    " + readings.size());

        for(SensorReading reading : readings) {
            System.out.println(reading.getEventId());
        }
    }
}
