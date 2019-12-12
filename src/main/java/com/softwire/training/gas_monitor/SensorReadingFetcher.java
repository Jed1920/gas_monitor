package com.softwire.training.gas_monitor;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.softwire.training.gas_monitor.Models.MonitorLocation;
import com.softwire.training.gas_monitor.Models.SensorReading;
import com.softwire.training.gas_monitor.Models.SnsMessage;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import javax.management.monitor.Monitor;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class SensorReadingFetcher {
    private final AmazonSQS sqs;

    private static final Logger LOGGER = LogManager.getLogger(MessageReceiver.class);

    public SensorReadingFetcher(AmazonSQS sqs) {
        this.sqs = sqs;
    }

    public List<SensorReading> fetchReadings(String myQueueUrl, List<String> validLocationIds) throws Exception {

        List<Message> messages = receiveMessagesFromQueue(myQueueUrl);
        List<SensorReading> matchedReadings = new ArrayList<>();

        LOGGER.info(String.format("    Received %d messages", messages.size()));

        for (Message message : messages) {
            SnsMessage snsMessage = (new ObjectMapper()).readValue(message.getBody(), SnsMessage.class);
            SensorReading reading = snsMessage.getMessage();
            LOGGER.info(message.getBody());

            if (validLocationIds.contains(reading.getLocationId())) {
                matchedReadings.add(reading);
            }
            deleteMessage(message, myQueueUrl, snsMessage);
        }

        return matchedReadings;
    }

    private List<Message> receiveMessagesFromQueue(String myQueueUrl){
        ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(myQueueUrl)
                .withWaitTimeSeconds(5)
                .withMaxNumberOfMessages(10);
        return sqs.receiveMessage(receiveMessageRequest).getMessages();
    }

    private void deleteMessage(Message message,String myQueueUrl,SnsMessage snsMessage){
        String messageReceiptHandle = message.getReceiptHandle();
        sqs.deleteMessage(new DeleteMessageRequest(myQueueUrl, messageReceiptHandle));
        LOGGER.info("   Deleted message:    " + snsMessage.getMessageId());
    }
}
