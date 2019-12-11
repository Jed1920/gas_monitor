package com.softwire.training.gas_monitor;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.sns.AmazonSNS;
import com.amazonaws.services.sns.AmazonSNSClientBuilder;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

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
        messageReceiver.run();


    }
}
