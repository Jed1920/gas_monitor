package com.softwire.training.gas_monitor;

import com.amazonaws.services.sqs.model.Message;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

@JsonIgnoreProperties(ignoreUnknown = true)
public class SnsMessage {
    private static final Logger LOGGER = LogManager.getLogger(SnsMessage.class);

    @JsonProperty("MessageId")
    private String messageId;

    @JsonProperty("Timestamp")
    private String timeStamp;

    @JsonProperty("Message")
    private SensorReading message;

    public String getMessageId() {
        return messageId;
    }

    public void setMessageId(String messageId) {
        this.messageId = messageId;
    }

    public String getTimeStamp() {
        return timeStamp;
    }

    public void setTimeStamp(String timeStamp) {
        this.timeStamp = timeStamp;
    }

    public SensorReading getMessage() {
        return message;
    }

    public void setMessage(String message) {
        ObjectMapper mapper = new ObjectMapper();
        try {
            this.message = mapper.readValue(message, SensorReading.class);
        } catch (Exception e) {
            LOGGER.error(e);
        }
    }

    public static SnsMessage fromSqsMessage(Message message) throws Exception {
        return (new ObjectMapper()).readValue(message.getBody(), SnsMessage.class);
    }
}
