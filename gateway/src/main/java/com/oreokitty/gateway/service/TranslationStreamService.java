package com.oreokitty.gateway.service;

import com.oreokitty.gateway.model.TranslationResult;
import org.springframework.stereotype.Service;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Service
public class TranslationStreamService {

    private final Map<String, SseEmitter> emitters = new ConcurrentHashMap<>();
    private final Map<String, TranslationResult> completedResults = new ConcurrentHashMap<>();

    public SseEmitter subscribe(String jobId) {
        SseEmitter emitter = new SseEmitter(120_000L);
        emitter.onCompletion(() -> emitters.remove(jobId, emitter));
        emitter.onTimeout(() -> emitters.remove(jobId, emitter));
        emitter.onError(t -> emitters.remove(jobId, emitter));

        TranslationResult cached = completedResults.get(jobId);
        if (cached != null) {
            sendResult(emitter, cached);
            emitter.complete();
            return emitter;
        }

        emitters.put(jobId, emitter);
        return emitter;
    }

    public void complete(String jobId, TranslationResult result) {
        SseEmitter emitter = emitters.get(jobId);
        if (emitter == null) {
            completedResults.put(jobId, result);
            return;
        }
        boolean sent = sendResult(emitter, result);
        if (sent) {
            emitter.complete();
        } else {
            completedResults.put(jobId, result);
        }
        emitters.remove(jobId, emitter);
    }

    private boolean sendResult(SseEmitter emitter, TranslationResult result) {
        try {
            emitter.send(SseEmitter.event()
                    .name("translation")
                    .data(Map.of(
                            "jobId", result.fileId(),
                            "status", "completed",
                            "intent", result.intent(),
                            "confidence", result.confidence())));
            return true;
        } catch (Exception e) {
            return false;
        }
    }
}
