package com.softwire.training.gas_monitor;


import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.softwire.training.gas_monitor.Models.MonitorLocation;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;

public class S3BucketFetcher {

    private final AmazonS3 s3Client;

    public S3BucketFetcher(AmazonS3 s3Client) {
        this.s3Client = s3Client;
    }

    private static final Logger LOGGER = LogManager.getLogger(S3BucketFetcher.class);

    public MonitorLocation[] fileFetcher() throws IOException {
        String bucket_name = "eventprocessing-locationss3bucket-7mbrb9iiisk4";
        String key_name = "locations.json";

        LOGGER.info(String.format("Downloading %s from S3 bucket %s...\n", key_name, bucket_name));

        S3Object s3Object = s3Client.getObject(bucket_name, key_name);
        S3ObjectInputStream inputStream = s3Object.getObjectContent();

        ObjectMapper mapper = new ObjectMapper();
        MonitorLocation[] locations = mapper.readValue(inputStream, MonitorLocation[].class);
        LOGGER.info(String.format("Received %d locations from S3 Bucket...\n", locations.length));

        return locations;
    }
}
