package com.softwire.training.gas_monitor;


import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.S3Object;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SNSMessageTest {

    @Test
    public void testCreatesMonitorLocationArray() {

        AmazonS3 s3Client = mock(AmazonS3.class);
        S3BucketFetcher fetcher = new S3BucketFetcher(s3Client);

        String bucketName = "eventprocessing-locationss3bucket-7mbrb9iiisk4";
        String fileName = "locations.json";

        when(s3Client.getObject(bucketName, fileName)).thenReturn(new S3Object());
    }
}
