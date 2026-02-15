package com.enterprise.messaging.controller;

import com.enterprise.messaging.model.IncomingMessage;
import com.enterprise.messaging.model.IncomingTask;
import com.enterprise.messaging.model.OutgoingMessage;
import com.enterprise.messaging.publisher.MessagePublisher;
import com.enterprise.messaging.repository.IncomingMessageRepository;
import com.enterprise.messaging.repository.OutgoingMessageRepository;
import com.enterprise.messaging.service.MessageProcessingOrchestrator;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;

@Slf4j
@RestController
@RequestMapping("/api/reprocess")
@RequiredArgsConstructor
public class ReprocessController {

    private final IncomingMessageRepository incomingMessageRepository;
    private final OutgoingMessageRepository outgoingMessageRepository;
    private final MessageProcessingOrchestrator orchestrator;
    private final MessagePublisher messagePublisher;

    @PostMapping("/incoming/{msgId}")
    public ResponseEntity<Map<String, Object>> reprocessIncomingMessage(@PathVariable BigDecimal msgId) {
        log.info("Reprocess request received for incoming message: msgId={}", msgId);
        
        try {
            var messageOpt = incomingMessageRepository.findById(msgId);
            
            if (messageOpt.isEmpty()) {
                return ResponseEntity.notFound().build();
            }
            
            IncomingMessage message = messageOpt.get();
            
            IncomingTask task = IncomingTask.builder()
                    .msgId(message.getMsgId())
                    .source(message.getSource())
                    .xCorrelationId(message.getXCorrelationId())
                    .internalSourceId(message.getInternalSourceId())
                    .eventType(message.getEventType())
                    .priority(message.getPriority())
                    .originalMessage(message.getOriginalMsg())
                    .build();
            
            orchestrator.processMessage(task);
            
            Map<String, Object> response = new HashMap<>();
            response.put("status", "SUCCESS");
            response.put("msgId", msgId);
            response.put("message", "Reprocessing started");
            
            return ResponseEntity.ok(response);
            
        } catch (Exception e) {
            log.error("Error reprocessing incoming message: msgId={}", msgId, e);
            Map<String, Object> response = new HashMap<>();
            response.put("status", "ERROR");
            response.put("msgId", msgId);
            response.put("error", e.getMessage());
            return ResponseEntity.internalServerError().body(response);
        }
    }

    @PostMapping("/outgoing/{msgId}")
    public ResponseEntity<Map<String, Object>> reprocessOutgoingMessage(@PathVariable BigDecimal msgId) {
        log.info("Reprocess request received for outgoing message: msgId={}", msgId);
        
        try {
            var messageOpt = outgoingMessageRepository.findById(msgId);
            
            if (messageOpt.isEmpty()) {
                return ResponseEntity.notFound().build();
            }
            
            OutgoingMessage message = messageOpt.get();
            
            String channel = determineChannel(message.getEventType());
            messagePublisher.publishToChannel(channel, message.getOriginalMsg(), message.getXCorrelationId());
            
            Map<String, Object> response = new HashMap<>();
            response.put("status", "SUCCESS");
            response.put("msgId", msgId);
            response.put("message", "Republishing started");
            
            return ResponseEntity.ok(response);
            
        } catch (Exception e) {
            log.error("Error reprocessing outgoing message: msgId={}", msgId, e);
            Map<String, Object> response = new HashMap<>();
            response.put("status", "ERROR");
            response.put("msgId", msgId);
            response.put("error", e.getMessage());
            return ResponseEntity.internalServerError().body(response);
        }
    }

    private String determineChannel(String eventType) {
        return "EMAIL";
    }
}
