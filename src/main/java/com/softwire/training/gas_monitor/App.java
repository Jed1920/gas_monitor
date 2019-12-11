package com.softwire.training.gas_monitor;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.sns.AmazonSNS;
import com.amazonaws.services.sns.AmazonSNSClientBuilder;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.softwire.training.gas_monitor.Models.MonitorLocation;
import com.softwire.training.gas_monitor.Models.SensorReading;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class App {

    private static final Logger LOGGER = LogManager.getLogger(App.class);

    public static void main(String[] args) throws Exception {

        AmazonS3 s3Client = AmazonS3ClientBuilder.standard().withRegion(Regions.EU_WEST_2).build();
        S3BucketFetcher s3BucketFetcher = new S3BucketFetcher(s3Client);

        MonitorLocation[] locations = s3BucketFetcher.fileFetcher();

        AmazonSQS sqs = AmazonSQSClientBuilder.standard().build();
        AmazonSNS sns = AmazonSNSClientBuilder.standard().build();
        SensorReadingFetcher sensorReadingFetcher = new SensorReadingFetcher(sqs);

        MessageReceiver messageReceiver = new MessageReceiver(sqs, sns, sensorReadingFetcher);
        List<SensorReading> readings = messageReceiver.run();

        List<String> matchedReadings = new ArrayList<>();

        for(MonitorLocation location : locations) {
             matchedReadings = readings.stream()
                                    .filter(reading -> reading
                                            .getLocationId()
                                            .contains(location.getId()))
                                    .map(SensorReading::getLocationId)
                                    .distinct()
                                    .collect(Collectors.toList());
        }
        System.out.println(matchedReadings);
    }
}
