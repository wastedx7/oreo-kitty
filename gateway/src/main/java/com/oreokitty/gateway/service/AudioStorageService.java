package com.oreokitty.gateway.service;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.UUID;

@Service
public class AudioStorageService {

    @Value("${oreo.storage.path:uploads}")
    private String storagePath;

    public Path save(MultipartFile file) throws IOException {
        String id = UUID.randomUUID().toString();
        String extension = safeExtension(file.getOriginalFilename());
        String fileName = id + extension;
        Path directory = Path.of(storagePath);
        Files.createDirectories(directory);
        Path destination = directory.resolve(fileName);
        Files.copy(file.getInputStream(), destination, StandardCopyOption.REPLACE_EXISTING);
        return destination;
    }

    private String safeExtension(String filename) {
        if (filename == null) {
            return ".bin";
        }
        String lower = filename.toLowerCase();
        if (lower.endsWith(".wav")) {
            return ".wav";
        }
        if (lower.endsWith(".mp3")) {
            return ".mp3";
        }
        return ".bin";
    }
}
