package com.softwire.training.gas_monitor;


import java.io.IOException;

/**
 * Hello world!
 *
 */
public class App {
    public static void main(String[] args) throws IOException {

        S3BucketFetcher s3BucketFetcher = new S3BucketFetcher();

        MonitorLocation[] locations = s3BucketFetcher.fileFetcher();

    }
}
