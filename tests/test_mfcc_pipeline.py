import unittest
from unittest.mock import Mock, patch

import numpy as np

from ml_service.app import AudioPreprocessor, CnnIntentModel, INTENT_LABELS


class TestAudioPipeline(unittest.TestCase):
    @patch("ml_service.app.AudioPreprocessor._librosa")
    def test_to_mfcc_pipeline(self, mock_librosa):
        fake_librosa = Mock()
        fake_librosa.load.return_value = (np.array([0.1, 0.2, 0.3]), 22050)
        fake_librosa.effects.preemphasis.return_value = np.array([0.2, 0.3])
        fake_librosa.effects.trim.return_value = (np.array([0.2, 0.3]), None)
        fake_librosa.feature.mfcc.return_value = np.ones((40, 16))
        mock_librosa.return_value = fake_librosa

        preprocessor = AudioPreprocessor()
        result = preprocessor.to_mfcc("sample.wav")

        self.assertEqual(result.shape, (40, 16))
        self.assertEqual(result.dtype, np.float32)

    def test_model_predict_contract(self):
        model = CnnIntentModel("unused")
        label, confidence = model.predict(np.ones((40, 16), dtype=np.float32))

        self.assertIn(label, INTENT_LABELS)
        self.assertGreaterEqual(confidence, 0.5)
        self.assertLessEqual(confidence, 0.99)


if __name__ == "__main__":
    unittest.main()
