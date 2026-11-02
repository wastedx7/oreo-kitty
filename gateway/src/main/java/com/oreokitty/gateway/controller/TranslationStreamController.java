package com.oreokitty.gateway.controller;

import com.oreokitty.gateway.service.TranslationStreamService;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

@RestController
public class TranslationStreamController {

    private final TranslationStreamService streamService;

    public TranslationStreamController(TranslationStreamService streamService) {
        this.streamService = streamService;
    }

    @GetMapping(value = "/api/translation/stream/{jobId}", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public SseEmitter stream(@PathVariable String jobId) {
        return streamService.subscribe(jobId);
    }
}
