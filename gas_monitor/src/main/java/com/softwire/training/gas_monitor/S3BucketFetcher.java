package com.softwire.training.gas_monitor;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.services.sns.AmazonSNS;
import com.amazonaws.services.sns.AmazonSNSClient;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.log4j.Logger;


public class S3BucketFetcher {

    private static Logger LOGGER = Logger.getLogger(S3BucketFetcher.class);


    public MonitorLocation[] fetchFile() throws Exception {
        String bucket_name = "eventprocessing-locationss3bucket-7mbrb9iiisk4";
        String key_name = "locations.json";

        LOGGER.info(String.format("Downloading %s from S3 bucket %s...\n", key_name, bucket_name));

        final AmazonS3 s3 = AmazonS3ClientBuilder.standard().withRegion(Regions.EU_WEST_2).build();

        S3Object s3Object = s3.getObject(bucket_name, key_name);
        S3ObjectInputStream inputStream = s3Object.getObjectContent();

        ObjectMapper mapper = new ObjectMapper();
        MonitorLocation[] locations = mapper.readValue(inputStream, MonitorLocation[].class);
        LOGGER.info(String.format("Received %d locations from S3 Bucket...\n", locations.length));

        AmazonSNS amazonSNS = new AmazonSNSClient();
        AmazonSQS amazonSQS = new AmazonSQSClient();


        return locations;
    }
}
