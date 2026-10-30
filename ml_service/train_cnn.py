from __future__ import annotations

import numpy as np
from tensorflow import keras

INTENT_LABELS = [
    "hungry_demanding",
    "content_purring",
    "warning_hissing",
    "mother_kitten",
]


def build_model(input_shape: tuple[int, int, int], classes: int) -> keras.Model:
    model = keras.Sequential(
        [
            keras.layers.Input(shape=input_shape),
            keras.layers.Conv2D(16, (3, 3), activation="relu"),
            keras.layers.MaxPooling2D((2, 2)),
            keras.layers.Conv2D(32, (3, 3), activation="relu"),
            keras.layers.MaxPooling2D((2, 2)),
            keras.layers.Flatten(),
            keras.layers.Dense(64, activation="relu"),
            keras.layers.Dense(classes, activation="softmax"),
        ]
    )
    model.compile(optimizer="adam", loss="sparse_categorical_crossentropy", metrics=["accuracy"])
    return model


def train_and_save(x_train: np.ndarray, y_train: np.ndarray, save_path: str) -> None:
    model = build_model(x_train.shape[1:], len(INTENT_LABELS))
    model.fit(x_train, y_train, epochs=10, batch_size=16, validation_split=0.2)
    model.save(save_path)
