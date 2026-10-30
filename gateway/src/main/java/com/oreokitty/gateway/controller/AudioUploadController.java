package com.oreokitty.gateway.controller;

import com.oreokitty.gateway.event.AudioProcessingRequestedEvent;
import com.oreokitty.gateway.publisher.AudioEventPublisher;
import com.oreokitty.gateway.service.AudioStorageService;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Map;

@RestController
@RequestMapping("/api/audio")
public class AudioUploadController {

    private final AudioStorageService storageService;
    private final AudioEventPublisher publisher;

    public AudioUploadController(AudioStorageService storageService, AudioEventPublisher publisher) {
        this.storageService = storageService;
        this.publisher = publisher;
    }

    @PostMapping(value = "/upload", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
    public ResponseEntity<?> upload(@RequestParam("file") MultipartFile file) throws IOException {
        if (file.isEmpty()) {
            return ResponseEntity.badRequest().body(Map.of("error", "Audio file is required"));
        }
        String contentType = file.getContentType() == null ? "" : file.getContentType();
        if (!contentType.contains("audio")) {
            return ResponseEntity.badRequest().body(Map.of("error", "Only audio files are accepted"));
        }
        Path saved = storageService.save(file);
        String fileId = saved.getFileName().toString();
        publisher.publish(new AudioProcessingRequestedEvent(fileId, saved.toAbsolutePath().toString(), Instant.now().toEpochMilli()));
        return ResponseEntity.accepted().body(Map.of(
                "jobId", fileId,
                "fileId", fileId,
                "status", "queued",
                "streamUrl", "/api/translation/stream/" + fileId));
    }
}
