from __future__ import annotations

import json
import os
from dataclasses import dataclass
from typing import Any

import numpy as np

INTENT_LABELS = [
    "hungry_demanding",
    "content_purring",
    "warning_hissing",
    "mother_kitten",
]


@dataclass
class AudioEvent:
    fileId: str
    filePath: str
    uploadedAtEpochMs: int


class AudioPreprocessor:
    def __init__(self, sample_rate: int = 22050, n_mfcc: int = 40):
        self.sample_rate = sample_rate
        self.n_mfcc = n_mfcc

    @staticmethod
    def _librosa() -> Any:
        import librosa

        return librosa

    def to_mfcc(self, audio_path: str) -> np.ndarray:
        librosa = self._librosa()
        signal, sr = librosa.load(audio_path, sr=self.sample_rate)
        denoised = librosa.effects.preemphasis(signal)
        trimmed, _ = librosa.effects.trim(denoised, top_db=20)
        mfcc = librosa.feature.mfcc(y=trimmed, sr=sr, n_mfcc=self.n_mfcc)
        return mfcc.astype(np.float32)


class CnnIntentModel:
    def __init__(self, model_path: str):
        self.model_path = model_path

    def predict(self, mfcc: np.ndarray) -> tuple[str, float]:
        # Minimal placeholder: produce deterministic intent from MFCC energy while
        # maintaining (label, confidence) contract for a trained CNN model.
        energy = float(np.mean(np.abs(mfcc)))
        bucket = min(int(energy * 10) % len(INTENT_LABELS), len(INTENT_LABELS) - 1)
        confidence = float(min(max(energy / 10, 0.5), 0.99))
        return INTENT_LABELS[bucket], confidence


async def consume_events() -> None:
    from aiokafka import AIOKafkaConsumer

    broker = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    topic = os.getenv("AUDIO_TOPIC", "audio-processing-requested")
    consumer = AIOKafkaConsumer(topic, bootstrap_servers=broker, group_id="oreo-kitty-ml")
    preprocessor = AudioPreprocessor()
    model = CnnIntentModel(os.getenv("MODEL_PATH", "models/cat_intent_cnn.keras"))
    await consumer.start()
    try:
        async for message in consumer:
            event = AudioEvent(**json.loads(message.value.decode("utf-8")))
            mfcc = preprocessor.to_mfcc(event.filePath)
            label, confidence = model.predict(mfcc)
            print(json.dumps({"fileId": event.fileId, "intent": label, "confidence": confidence}))
    finally:
        await consumer.stop()


if __name__ == "__main__":
    import asyncio

    asyncio.run(consume_events())
