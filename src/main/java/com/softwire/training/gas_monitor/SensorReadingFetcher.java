package com.softwire.training.gas_monitor;

import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.List;

public class SensorReadingFetcher {
    private final AmazonSQS sqs;

    private static final Logger LOGGER = LogManager.getLogger(MessageReceiver.class);

    public SensorReadingFetcher(AmazonSQS sqs) {
        this.sqs = sqs;
    }

    public List<SensorReading> fetchReadings(String myQueueUrl) throws Exception {
        ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(myQueueUrl)
                .withWaitTimeSeconds(5)
                .withMaxNumberOfMessages(10);
        List<Message> messages = sqs.receiveMessage(receiveMessageRequest).getMessages();
        List<SensorReading> allSensorReadings = new ArrayList<>();

        LOGGER.info(String.format("    Received %d messages", messages.size()));

        for (Message message : messages) {
            SnsMessage snsMessage = SnsMessage.fromSqsMessage(message);

            allSensorReadings.add(snsMessage.getMessage());

            String messageReceiptHandle = message.getReceiptHandle();
            sqs.deleteMessage(new DeleteMessageRequest(myQueueUrl, messageReceiptHandle));
            LOGGER.info("   Deleted message:    " + snsMessage.getMessageId());
        }

        return allSensorReadings;
    }
}
