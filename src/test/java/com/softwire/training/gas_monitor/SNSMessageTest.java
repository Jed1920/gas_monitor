package com.softwire.training.gas_monitor;

import com.amazonaws.services.sqs.model.Message;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.softwire.training.gas_monitor.Models.SnsMessage;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SNSMessageTest {
//
//    @Test
//    public void testCreatesMonitorLocationArray() {
//
//        AmazonS3 s3Client = mock(AmazonS3.class);
//        S3BucketFetcher fetcher = new S3BucketFetcher(s3Client);
//
//        String bucketName = "eventprocessing-locationss3bucket-7mbrb9iiisk4";
//        String fileName = "locations.json";
//
//        when(s3Client.getObject(bucketName, fileName)).thenReturn(new S3Object());
//    }

    @Test
    public void testMappingMessageToSensorReading(){
        String message = "{\"locationId\":\"1a88c751-4a9a-41b8-83e1-4f530cfa00e1\"," +
                "\"eventId\":\"b6a047a1-6fe4-4b3f-8514-5b45233ceec9\"," +
                "\"value\":3.5199409938097306," +
                "\"timestamp\":1576152531316}";
        SnsMessage snsMessage = new SnsMessage();
        snsMessage.setMessage(message);

        assertEquals(snsMessage.getMessage().getEventId(),"b6a047a1-6fe4-4b3f-8514-5b45233ceec9");
        assertEquals(snsMessage.getMessage().getLocationId(),"1a88c751-4a9a-41b8-83e1-4f530cfa00e1");
        assertEquals(snsMessage.getMessage().getValue(),"3.5199409938097306");
        assertEquals(snsMessage.getMessage().getTimestamp(),"1576152531316");
    }

    @Test
    public void testMappingMessageToSnsMessage() throws IOException {

        Message message = new Message();
        message.setBody("{" +
                "  \"Type\" : \"Notification\"," +
                "  \"MessageId\" : \"23cde0ae-a7cb-5186-aafe-b6c2ef0f270e\"," +
                "  \"TopicArn\" : \"arn:aws:sns:eu-west-2:099421490492:EventProcessing-snsTopicSensorDataPart1-QVDE0JIXZS1V\"," +
                "  \"Message\" : \"{\\\"locationId\\\":\\\"83969249-c227-4b5a-8e52-aa5b293cc3cc\\\",\\\"eventId\\\":\\\"0dd73c18-0f36-4bba-b702-21bb1b8c3beb\\\",\\\"value\\\":8.207485646722187,\\\"timestamp\\\":1576158852016}\"," +
                "  \"Timestamp\" : \"2019-12-12T13:54:12.019Z\"," +
                "  \"SignatureVersion\" : \"1\"," +
                "  \"Signature\" : \"BXfZGeYD8Fe9cMD+FRX1ltKy/EYWoZcCEmP+2BHpnT0P0Bob7/v49TOhU9s8s5rQESKS+ViGH3jqS85Oxr4eXRfcjqEm/IzzsJZknOW+7JtKv5YHO/TRTXdAvulR69LTcL1aB+79KWiCt9PK+4FRITvpvuVJsPMaWPySeFlDuQ6sg8ZuqbJxjFdySvMkUq1+pptFLMvTejuyEU+fxRIuRSETADFFUaEPcFDqqcokIfCky32BY0/7bq3COQP8Nez2a4Pp1nHc+9PJAgcgpcIGDNiZY/CwJkb7LBtGdCYj9p1juN8aSQmTCb/7w3oH9OFE520r3KqIGhiRxry/fGh8IQ==\"," +
                "  \"SigningCertURL\" : \"https://sns.eu-west-2.amazonaws.com/SimpleNotificationService-6aad65c2f9911b05cd53efda11f913f9.pem\"," +
                "  \"UnsubscribeURL\" : \"https://sns.eu-west-2.amazonaws.com/?Action=Unsubscribe&SubscriptionArn=arn:aws:sns:eu-west-2:099421490492:EventProcessing-snsTopicSensorDataPart1-QVDE0JIXZS1V:93bdb062-636f-4cf0-a86a-3347e380477e\"" +
                "}");
    SnsMessage snsMessage = (new ObjectMapper()).readValue(message.getBody(), SnsMessage.class);

        assertEquals(snsMessage.getMessageId(),"23cde0ae-a7cb-5186-aafe-b6c2ef0f270e");
        assertEquals(snsMessage.getTimeStamp(),"2019-12-12T13:54:12.019Z");
        assertEquals(snsMessage.getMessage().getEventId(),"0dd73c18-0f36-4bba-b702-21bb1b8c3beb");
        assertEquals(snsMessage.getMessage().getLocationId(),"83969249-c227-4b5a-8e52-aa5b293cc3cc");
        assertEquals(snsMessage.getMessage().getValue(),"8.207485646722187");
        assertEquals(snsMessage.getMessage().getTimestamp(),"1576158852016");
    }

    @Test
    public void testMatchSensorReadingsToLocations() {

    }


}
