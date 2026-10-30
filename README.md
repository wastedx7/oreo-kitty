# oreo-kitty

Event-driven cat audio intent detection prototype:

- **Spring Boot API Gateway** (`/gateway`): accepts uploaded audio via `POST /api/audio/upload`, saves it to local storage, and publishes a Kafka event to `audio-processing-requested`.
- **Kafka**: queues audio-processing requests.
- **Python ML microservice** (`/ml_service`): consumes Kafka events, denoises and trims audio with `librosa`, converts to MFCC spectrogram features, and predicts one of core intents:
  - `hungry_demanding`
  - `content_purring`
  - `warning_hissing`
  - `mother_kitten`
  - Persists predictions to PostgreSQL (`POSTGRES_DSN`) and sends result callbacks to the gateway over gRPC (`GATEWAY_GRPC_TARGET`).
- **Training script** (`ml_service/train_cnn.py`): defines a CNN training pipeline over MFCC inputs.

## Quick validation

Run focused unit tests for preprocessing/inference contract:

```bash
python -m unittest discover -s tests -p 'test_*.py'
```
