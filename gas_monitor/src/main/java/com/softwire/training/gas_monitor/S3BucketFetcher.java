package com.softwire.training.gas_monitor;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;

public class S3BucketFetcher {

    public void S3BucketFetcher() {
        String bucket_name = "eventprocessing-locationss3bucket-7mbrb9iiisk4";
        String key_name = "locations.json";

        System.out.format("Downloading %s from S3 bucket %s...\n", key_name, bucket_name);
        final AmazonS3 s3 = AmazonS3ClientBuilder.standard().withRegion(Regions.EU_WEST_2).build();

        S3Object s3Object = s3.getObject(bucket_name, key_name);
        S3ObjectInputStream inputStream = s3Object.getObjectContent();

        ObjectMapper mapper = new ObjectMapper();
        MonitorLocation[] locations = mapper.readValue(inputStream, MonitorLocation[].class);
        LOGGER.info(String.format("Received %d locations from S3 Bucket...\n", locations.length));

        return locations;
    }
}
