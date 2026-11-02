# oreo-kitty

Event-driven cat audio intent detection prototype:

- **Spring Boot API Gateway** (`/gateway`): accepts uploaded audio via `POST /api/audio/upload`, saves it to local storage, and publishes a Kafka event to `audio-processing-requested`.
- **Kafka**: queues audio-processing requests.
- **Python ML microservice** (`/ml_service`): consumes Kafka events, denoises and trims audio with `librosa`, converts to MFCC spectrogram features, and predicts one of core intents:
  - `hungry_demanding`
  - `content_purring`
  - `warning_hissing`
  - `mother_kitten`
  - Persists predictions to PostgreSQL (`POSTGRES_DSN`, optional) and sends result callbacks to the gateway over gRPC (`GATEWAY_GRPC_TARGET`).
- **Training script** (`ml_service/train_cnn.py`): defines a CNN training pipeline over MFCC inputs.

## Asynchronous result delivery (SSE)

The pipeline is fully asynchronous: the upload request returns immediately, long before the ML service finishes. Results are pushed to clients over Server-Sent Events:

1. `POST /api/audio/upload` (multipart `file`) returns `202 Accepted` with a `jobId` and the stream URL:

   ```json
   {
     "jobId": "1c4cded3-...-5f05-81af-1efb6a560263.wav",
     "fileId": "1c4cded3-...-5f05-81af-1efb6a560263.wav",
     "status": "queued",
     "streamUrl": "/api/translation/stream/1c4cded3-...-5f05-81af-1efb6a560263.wav"
   }
   ```

2. The client opens an SSE connection to `GET /api/translation/stream/{jobId}` (`text/event-stream`).
3. The gateway streams a `connected` event, then a final `translation` event:

   ```
   event: connected
   data: {"jobId":"...","status":"listening"}

   event: translation
   data: {"jobId":"...","status":"completed","intent":"content_purring","confidence":0.99}
   ```

4. When the Python service finishes inference it calls the gateway's gRPC server (`oreo.gateway.ResultService/PublishResult` on `localhost:9090`) with `{"fileId", "intent", "confidence"}`; the gateway routes the payload to the emitter registered for that `jobId` and closes the stream.

Results that arrive before the client subscribes are cached per `jobId` and delivered instantly on connect, so there is no race between `202 Accepted` and the ML service being fast. Emitters time out after 120s.

## Configuration

Copy `.env.example` to `.env` and adjust. Key variables:

| Variable | Used by | Default | Purpose |
| --- | --- | --- | --- |
| `KAFKA_BOOTSTRAP_SERVERS` | gateway + ML service | `localhost:9092` | Kafka broker |
| `AUDIO_TOPIC` | gateway + ML service | `audio-processing-requested` | Upload topic |
| `GATEWAY_GRPC_PORT` | gateway | `9090` | Port of the embedded gRPC prediction callback server (`oreo.grpc.port`) |
| `GATEWAY_GRPC_TARGET` | ML service | unset | `host:port` of the gateway gRPC server; unset disables callbacks |
| `POSTGRES_DSN` | ML service | unset | Prediction persistence; unset skips the database |
| `MODEL_PATH` | ML service | `models/cat_intent_cnn.keras` | CNN weights (autoloading not wired yet) |

The gateway's Kafka producer serializes events as JSON (`String` key / `JsonSerializer` value); the upload path also uses `spring.kafka.bootstrap-servers`, overridable via `KAFKA_BOOTSTRAP_SERVERS`.

## Running end-to-end

1. Kafka (single-node KRaft — replication factors must be 1 or `__consumer_offsets` creation will spin forever):

   ```bash
   docker run -d --name oreo-kafka -p 9092:9092 \
     -e KAFKA_NODE_ID=0 \
     -e KAFKA_PROCESS_ROLES=controller,broker \
     -e KAFKA_CONTROLLER_QUORUM_VOTERS=0@oreo-kafka:9093 \
     -e KAFKA_LISTENERS=PLAINTEXT://:9092,CONTROLLER://:9093 \
     -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:9092 \
     -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER \
     -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT \
     -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 \
     -e KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1 \
     -e KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1 \
     -e KAFKA_AUTO_CREATE_TOPICS_ENABLE=true \
     apache/kafka:4.0.0
   ```

2. Start the gateway (serves HTTP on 8080 and gRPC on 9090):

   ```bash
   cd gateway && mvn spring-boot:run
   ```

3. Start the ML service:

   ```bash
   GATEWAY_GRPC_TARGET=localhost:9090 python ml_service/app.py
   ```

4. Upload, then listen (e.g. two shells, or subscribe first and upload second — both work):

   ```bash
   curl -N http://localhost:8080/api/translation/stream/<jobId>
   curl -F "file=@meow.wav" http://localhost:8080/api/audio/upload
   ```

## Quick validation

Run focused unit tests for preprocessing/inference contract:

```bash
python -m unittest discover -s tests -p "test_*.py"
```

## Known limitations

- `POST /api/audio/upload` blocks up to ~60s (`max.block.ms`) when Kafka is unreachable before failing; fail-fast (503) is not wired yet.
- `ml_service/app.py` has no per-message error handling — one bad message can kill the consumer loop.
- The gateway's `CnnIntentModel` is a placeholder energy-based classifier; `train_cnn.py` output is not loaded automatically.
