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
        String originalName = file.getOriginalFilename() == null ? "audio.bin" : file.getOriginalFilename();
        String fileName = id + "-" + originalName.replaceAll("[^a-zA-Z0-9._-]", "_");
        Path directory = Path.of(storagePath);
        Files.createDirectories(directory);
        Path destination = directory.resolve(fileName);
        Files.copy(file.getInputStream(), destination, StandardCopyOption.REPLACE_EXISTING);
        return destination;
    }
}
