package com.oreokitty.gateway.publisher;

import com.oreokitty.gateway.event.AudioProcessingRequestedEvent;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class AudioEventPublisher {

    private final KafkaTemplate<String, AudioProcessingRequestedEvent> kafkaTemplate;
    private final String topic;

    public AudioEventPublisher(KafkaTemplate<String, AudioProcessingRequestedEvent> kafkaTemplate,
                               @Value("${oreo.kafka.audio-topic:audio-processing-requested}") String topic) {
        this.kafkaTemplate = kafkaTemplate;
        this.topic = topic;
    }

    public void publish(AudioProcessingRequestedEvent event) {
        kafkaTemplate.send(topic, event.fileId(), event);
    }
}
