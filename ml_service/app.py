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


@dataclass
class PredictionResult:
    file_id: str
    intent: str
    confidence: float


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


class ResultPublisher:
    def __init__(self, postgres_dsn: str | None, grpc_target: str | None):
        self.postgres_dsn = postgres_dsn
        self.grpc_target = grpc_target

    def publish(self, result: PredictionResult) -> None:
        self._write_postgres(result)
        self._send_grpc(result)

    def _write_postgres(self, result: PredictionResult) -> None:
        if not self.postgres_dsn:
            return
        import psycopg

        with psycopg.connect(self.postgres_dsn) as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    CREATE TABLE IF NOT EXISTS cat_audio_predictions (
                        file_id TEXT PRIMARY KEY,
                        intent TEXT NOT NULL,
                        confidence DOUBLE PRECISION NOT NULL,
                        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                    )
                    """
                )
                cursor.execute(
                    """
                    INSERT INTO cat_audio_predictions (file_id, intent, confidence)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (file_id) DO UPDATE
                    SET intent = EXCLUDED.intent, confidence = EXCLUDED.confidence
                    """,
                    (result.file_id, result.intent, result.confidence),
                )
            conn.commit()

    def _send_grpc(self, result: PredictionResult) -> None:
        if not self.grpc_target:
            return
        import grpc

        payload = json.dumps(
            {"fileId": result.file_id, "intent": result.intent, "confidence": result.confidence}
        ).encode("utf-8")
        with grpc.insecure_channel(self.grpc_target) as channel:
            sender = channel.unary_unary("/oreo.gateway.ResultService/PublishResult")
            sender(payload, timeout=2)


async def consume_events() -> None:
    from aiokafka import AIOKafkaConsumer

    broker = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    topic = os.getenv("AUDIO_TOPIC", "audio-processing-requested")
    consumer = AIOKafkaConsumer(topic, bootstrap_servers=broker, group_id="oreo-kitty-ml")
    preprocessor = AudioPreprocessor()
    model = CnnIntentModel(os.getenv("MODEL_PATH", "models/cat_intent_cnn.keras"))
    sink = ResultPublisher(
        postgres_dsn=os.getenv("POSTGRES_DSN"),
        grpc_target=os.getenv("GATEWAY_GRPC_TARGET"),
    )
    await consumer.start()
    try:
        async for message in consumer:
            event = AudioEvent(**json.loads(message.value.decode("utf-8")))
            mfcc = preprocessor.to_mfcc(event.filePath)
            label, confidence = model.predict(mfcc)
            sink.publish(PredictionResult(file_id=event.fileId, intent=label, confidence=confidence))
    finally:
        await consumer.stop()


if __name__ == "__main__":
    import asyncio

    asyncio.run(consume_events())
