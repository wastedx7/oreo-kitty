package com.oreokitty.gateway.model;

public record TranslationResult(
        String fileId,
        String intent,
        double confidence) {
}
