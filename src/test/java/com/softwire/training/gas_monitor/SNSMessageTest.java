package com.softwire.training.gas_monitor;

import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.softwire.training.gas_monitor.Models.SensorReading;
import com.softwire.training.gas_monitor.Models.SnsMessage;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SNSMessageTest {
    private SensorReadingFetcher fetcher;
    private AmazonSQS mockSqs;

    @Before
    public void setUp() {
        mockSqs = mock(AmazonSQS.class);
    }

    @Test
    public void testMatchSensorReadingsToLocations() throws Exception {
        List<Message> messages = List.of(dummyMessage());
        Clock clock = Clock.systemDefaultZone();

        List<SensorReading> matchedReadings = useSensorReadingFetcher(messages,clock);
        List<String> readingLocationIds = matchedReadings.stream()
                .map(SensorReading::getLocationId)
                .distinct()
                .collect(Collectors.toList());

        assertThat(readingLocationIds).contains("83969249-c227-4b5a-8e52-aa5b293cc3cc");
    }

    @Test
    public void testNoDuplicatesAdded() throws Exception {
        List<Message> messages = List.of(dummyMessage(), dummyMessage());
        Clock clock = Clock.fixed(
                Instant.ofEpochMilli(1576158250000L),
                ZoneOffset.UTC);

        List<SensorReading> matchedReadings = useSensorReadingFetcher(messages,clock);

        assertThat(matchedReadings.size()).isEqualTo(1);
    }

    @Test
    public void testOldEventsNotInList() throws Exception {
        List<Message> messages = List.of(dummyMessage(), olderDummyMessage());
        Clock clock = Clock.fixed(
                Instant.ofEpochMilli(1576158250000L),
                ZoneOffset.UTC);
        List<SensorReading> matchedReadings = useSensorReadingFetcher(messages,clock);

        List<String> readingEventIds = matchedReadings.stream()
                .map(SensorReading::getEventId)
                .distinct()
                .collect(Collectors.toList());

        assertThat(readingEventIds).contains("0dd73c18-0f36-4bba-b702-21bb1b8c3beb");
        assertThat(readingEventIds).doesNotContain("0dd73c18-0f36-4bba-b702-21bb1b8c3beb");
    }



    private List<SensorReading> useSensorReadingFetcher(List<Message> messages,Clock clock) throws Exception {
        fetcher = new SensorReadingFetcher(mockSqs, clock);
        ReceiveMessageResult result = new ReceiveMessageResult().withMessages(messages);
        when(mockSqs.receiveMessage(any(ReceiveMessageRequest.class))).thenReturn(result);

        String dummyQueue = "queue-url";
        List<String> validLocationIds = List.of("83969249-c227-4b5a-8e52-aa5b293cc3cc");



        return fetcher.fetchReadings(dummyQueue, validLocationIds);
    }

    private Message dummyMessage() {
        Message message = new Message();
        message.setBody("{" +
                "  \"Type\" : \"Notification\"," +
                "  \"MessageId\" : \"23cde0ae-a7cb-5186-aafe-b6c2ef0f270e\"," +
                "  \"TopicArn\" : \"arn:aws:sns:eu-west-2:099421490492:EventProcessing-snsTopicSensorDataPart1-QVDE0JIXZS1V\"," +
                "  \"Message\" : \"{\\\"locationId\\\":\\\"83969249-c227-4b5a-8e52-aa5b293cc3cc\\\",\\\"eventId\\\":\\\"0dd73c18-0f36-4bba-b702-21bb1b8c3beb\\\",\\\"value\\\":8.207485646722187,\\\"timestamp\\\":1576158200000}\"," +
                "  \"Timestamp\" : \"2019-12-12T13:54:00.000Z\"," +
                "  \"SignatureVersion\" : \"1\"," +
                "  \"Signature\" : \"BXfZGeYD8Fe9cMD+FRX1ltKy/EYWoZcCEmP+2BHpnT0P0Bob7/v49TOhU9s8s5rQESKS+ViGH3jqS85Oxr4eXRfcjqEm/IzzsJZknOW+7JtKv5YHO/TRTXdAvulR69LTcL1aB+79KWiCt9PK+4FRITvpvuVJsPMaWPySeFlDuQ6sg8ZuqbJxjFdySvMkUq1+pptFLMvTejuyEU+fxRIuRSETADFFUaEPcFDqqcokIfCky32BY0/7bq3COQP8Nez2a4Pp1nHc+9PJAgcgpcIGDNiZY/CwJkb7LBtGdCYj9p1juN8aSQmTCb/7w3oH9OFE520r3KqIGhiRxry/fGh8IQ==\"," +
                "  \"SigningCertURL\" : \"https://sns.eu-west-2.amazonaws.com/SimpleNotificationService-6aad65c2f9911b05cd53efda11f913f9.pem\"," +
                "  \"UnsubscribeURL\" : \"https://sns.eu-west-2.amazonaws.com/?Action=Unsubscribe&SubscriptionArn=arn:aws:sns:eu-west-2:099421490492:EventProcessing-snsTopicSensorDataPart1-QVDE0JIXZS1V:93bdb062-636f-4cf0-a86a-3347e380477e\"" +
                "}");
        return message;
    }

        private Message olderDummyMessage() {
        Message message = new Message();
        message.setBody("{" +
                "  \"Type\" : \"Notification\"," +
                "  \"MessageId\" : \"23cde0ae-a7cb-5186-aafe-b6c2ef0f270e\"," +
                "  \"TopicArn\" : \"arn:aws:sns:eu-west-2:099421490492:EventProcessing-snsTopicSensorDataPart1-QVDE0JIXZS1V\"," +
                "  \"Message\" : \"{\\\"locationId\\\":\\\"83969249-c227-4b5a-8e52-aa5b293cc3cc\\\",\\\"eventId\\\":\\\"0dd73c18-0f36-4bba-b702-21bb1b8c3beb\\\",\\\"value\\\":8.207485646722187,\\\"timestamp\\\":1576158000000}\"," +
                "  \"Timestamp\" : \"2019-12-12T13:54:00.000Z\"," +
                "  \"SignatureVersion\" : \"1\"," +
                "  \"Signature\" : \"BXfZGeYD8Fe9cMD+FRX1ltKy/EYWoZcCEmP+2BHpnT0P0Bob7/v49TOhU9s8s5rQESKS+ViGH3jqS85Oxr4eXRfcjqEm/IzzsJZknOW+7JtKv5YHO/TRTXdAvulR69LTcL1aB+79KWiCt9PK+4FRITvpvuVJsPMaWPySeFlDuQ6sg8ZuqbJxjFdySvMkUq1+pptFLMvTejuyEU+fxRIuRSETADFFUaEPcFDqqcokIfCky32BY0/7bq3COQP8Nez2a4Pp1nHc+9PJAgcgpcIGDNiZY/CwJkb7LBtGdCYj9p1juN8aSQmTCb/7w3oH9OFE520r3KqIGhiRxry/fGh8IQ==\"," +
                "  \"SigningCertURL\" : \"https://sns.eu-west-2.amazonaws.com/SimpleNotificationService-6aad65c2f9911b05cd53efda11f913f9.pem\"," +
                "  \"UnsubscribeURL\" : \"https://sns.eu-west-2.amazonaws.com/?Action=Unsubscribe&SubscriptionArn=arn:aws:sns:eu-west-2:099421490492:EventProcessing-snsTopicSensorDataPart1-QVDE0JIXZS1V:93bdb062-636f-4cf0-a86a-3347e380477e\"" +
                "}");
        return message;
    }
//        private Message olderDummyMessage() {
//        Message message = new Message();
//        message.setBody("{" +
//                "  \"Type\" : \"Notification\"," +
//                "  \"MessageId\" : \"90b15f43-445a-57d7-a016-092e22073cf3\"," +
//                "  \"TopicArn\" : \"arn:aws:sns:eu-west-2:099421490492:EventProcessing-snsTopicSensorDataPart1-QVDE0JIXZS1V\"," +
//                "  \"Message\" : \"{\\\"locationId\\\":\\\"b846968d-e7ad-4312-8c88-4f4c54cb07fe\\\",\\\"eventId\\\":\\\"764988dd-9d6f-4e23-8908-5e09c47d4d75\\\",\\\"value\\\":3.337376179273053,\\\"timestamp\\\":1576158000000}\"," +
//                "  \"Timestamp\" : \"2019-12-12T13:50:00.000Z\"," +
//                "  \"SignatureVersion\" : \"1\"," +
//                "  \"Signature\" : \"BXfZGeYD8Fe9cMD+FRX1ltKy/EYWoZcCEmP+2BHpnT0P0Bob7/v49TOhU9s8s5rQESKS+ViGH3jqS85Oxr4eXRfcjqEm/IzzsJZknOW+7JtKv5YHO/TRTXdAvulR69LTcL1aB+79KWiCt9PK+4FRITvpvuVJsPMaWPySeFlDuQ6sg8ZuqbJxjFdySvMkUq1+pptFLMvTejuyEU+fxRIuRSETADFFUaEPcFDqqcokIfCky32BY0/7bq3COQP8Nez2a4Pp1nHc+9PJAgcgpcIGDNiZY/CwJkb7LBtGdCYj9p1juN8aSQmTCb/7w3oH9OFE520r3KqIGhiRxry/fGh8IQ==\"," +
//                "  \"SigningCertURL\" : \"https://sns.eu-west-2.amazonaws.com/SimpleNotificationService-6aad65c2f9911b05cd53efda11f913f9.pem\"," +
//                "  \"UnsubscribeURL\" : \"https://sns.eu-west-2.amazonaws.com/?Action=Unsubscribe&SubscriptionArn=arn:aws:sns:eu-west-2:099421490492:EventProcessing-snsTopicSensorDataPart1-QVDE0JIXZS1V:93bdb062-636f-4cf0-a86a-3347e380477e\"" +
//                "}");
//        return message;
//    }
}
