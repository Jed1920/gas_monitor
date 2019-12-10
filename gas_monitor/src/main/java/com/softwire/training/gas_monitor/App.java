package com.softwire.training.gas_monitor;


import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

/**
 * Hello world!
 *
 */
public class App {
    static Logger logger = Logger.getLogger(App.class);

    public static void main(String[] args) throws Exception {
        //Log in console in and log file
        logger.info("Log4j appender configuration is successful !!");

        S3BucketFetcher s3Fetcher = new S3BucketFetcher();

        MonitorLocation[] locations = s3Fetcher.fetchFile();

    }
}
