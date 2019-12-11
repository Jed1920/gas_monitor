package com.softwire.training.gas_monitor;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;

public class App {

    private static final Logger LOGGER = LogManager.getLogger(App.class);

    public static void main(String[] args) throws Exception {

        S3BucketFetcher s3BucketFetcher = new S3BucketFetcher();

        MonitorLocation[] locations = s3BucketFetcher.fileFetcher();

        MessageReceiver messageReceiver = new MessageReceiver();
        messageReceiver.receiveMessages();


    }
}
