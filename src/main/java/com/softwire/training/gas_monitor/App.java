package com.softwire.training.gas_monitor;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.sns.AmazonSNS;
import com.amazonaws.services.sns.AmazonSNSClientBuilder;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.softwire.training.gas_monitor.Models.MonitorLocation;
import com.softwire.training.gas_monitor.Models.SensorReading;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.time.Clock;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class App {

    private static final Logger LOGGER = LogManager.getLogger(App.class);


    public static void main(String[] args) throws Exception {

        AmazonSQS sqsClient = AmazonSQSClientBuilder.standard().build();
        AmazonSNS snsClient = AmazonSNSClientBuilder.standard().build();
        Clock clock = Clock.systemDefaultZone();
        SensorReadingFetcher sensorReadingFetcher = new SensorReadingFetcher(sqsClient,clock);

        AmazonS3 s3Client = AmazonS3ClientBuilder.standard().withRegion(Regions.EU_WEST_2).build();

        String myQueueUrl = sqsClient.createQueue(new CreateQueueRequest("jedQueue")).getQueueUrl();

        S3BucketFetcher s3BucketFetcher = new S3BucketFetcher(s3Client);
        List<String> validLocationIds = fetchImportantLocationIds(s3BucketFetcher);

        MessageReceiver messageReceiver = new MessageReceiver(sqsClient,snsClient,sensorReadingFetcher);
        List<SensorReading> readings = messageReceiver.getListofSensorReadings(myQueueUrl,validLocationIds);
        LOGGER.info("  Total number of matched sensor reading:    " + readings.size());

        for(SensorReading reading : readings) {
            System.out.println(reading.getEventId());
        }
    }

    public static List<String> fetchImportantLocationIds(S3BucketFetcher s3BucketFetcher) throws IOException {
        MonitorLocation[] locations = s3BucketFetcher.fileFetcher();
        return Arrays.stream(locations)
                .map(location -> location.getId())
                .collect(Collectors.toList());
    }
}
