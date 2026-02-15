package com.enterprise.messaging.publisher;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class MessagePublisherTest {
    @Mock
    private KafkaTemplate<String, String> kafkaTemplate;
    
    @InjectMocks
    private MessagePublisher publisher;

    @Test
    void testPublish() {
        assertThatCode(() -> publisher.publish("topic", "key", "message"))
                .doesNotThrowAnyException();
        verify(kafkaTemplate, times(1)).send(eq("topic"), eq("key"), eq("message"));
    }
}
