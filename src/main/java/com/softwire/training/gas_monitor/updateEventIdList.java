package com.softwire.training.gas_monitor;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.time.Clock;
import java.util.Map;

public class updateEventIdList {

    //input reading as argument
    //check if already in the hashmap
    //check how long each item has been in the hashmap

    private Map<String,Long> eventIdHolder;
    private Clock clock;

    private static final Logger LOGGER = LogManager.getLogger(MessageReceiver.class);

    public updateEventIdList(Map<String, Long> eventIdHolder,Clock clock){
        this.eventIdHolder=eventIdHolder;
        this.clock=clock;
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
