package com.softwire.training.gas_monitor;

import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.softwire.training.gas_monitor.Models.SensorReading;
import com.softwire.training.gas_monitor.Models.SnsMessage;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.time.Clock;
import java.util.*;

public class SensorReadingFetcher {
    private final AmazonSQS sqs;
    private final Clock clock;

    private static final Logger LOGGER = LogManager.getLogger(MessageReceiver.class);

    public SensorReadingFetcher(AmazonSQS sqs, Clock clock) {
        this.sqs = sqs;
        this.clock = clock;
    }

    public List<SensorReading> fetchReadings(String myQueueUrl, List<String> validLocationIds) throws Exception {

        List<Message> messages = receiveMessagesFromQueue(myQueueUrl);
        List<SensorReading> matchedReadings = new ArrayList<>();
        Map<String, Long> readingEventHolder = new HashMap<>();

        LOGGER.info(String.format("   Received %d messages", messages.size()));

        for (Message message : messages) {
            SnsMessage snsMessage = (new ObjectMapper()).readValue(message.getBody(), SnsMessage.class);
            SensorReading reading = snsMessage.getMessage();
            LOGGER.info(message.getBody());

            if (validLocationIds.contains(reading.getLocationId()) && !readingEventHolder.containsKey(reading.getEventId())) {
                matchedReadings.add(reading);
                readingEventHolder.put(reading.getEventId(),reading.getTimestamp());
                removeOldEventIds(readingEventHolder);
            }
            deleteMessage(message, myQueueUrl, snsMessage);
        }

        return matchedReadings;
    }

    private List<Message> receiveMessagesFromQueue(String myQueueUrl) {
        ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(myQueueUrl)
                .withWaitTimeSeconds(5)
                .withMaxNumberOfMessages(10);
        ReceiveMessageResult result = sqs.receiveMessage(receiveMessageRequest);
        return result.getMessages();
    }

    private void deleteMessage(Message message, String myQueueUrl, SnsMessage snsMessage) {
        String messageReceiptHandle = message.getReceiptHandle();
        sqs.deleteMessage(new DeleteMessageRequest(myQueueUrl, messageReceiptHandle));
        LOGGER.info("   Deleted message:    " + snsMessage.getMessageId());
    }

    private void removeOldEventIds(Map<String, Long> readingEventHolder){
        long currentTime = clock.millis();

        LOGGER.info(currentTime);

        for(Map.Entry<String,Long> entry : readingEventHolder.entrySet()){
            if (entry.getValue()+180000 <= currentTime){
                readingEventHolder.remove(entry.getKey());
            }
        }
    }


}
