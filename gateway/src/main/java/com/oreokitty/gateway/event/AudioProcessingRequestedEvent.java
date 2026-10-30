package com.oreokitty.gateway.event;

public record AudioProcessingRequestedEvent(String fileId, String filePath, long uploadedAtEpochMs) {
}
