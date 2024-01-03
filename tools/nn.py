#!/usr/bin/env python3
"""

Usage:
 nn.py params_file

  Formatted by black (https://pypi.org/project/black/)

  Copyright (c) 2021-2024, Hironobu Suzuki @ interdb.jp
"""

import argparse
import sys
import os
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import tensorflow as tf
from tensorflow import keras
from tensorflow.keras import layers
from tensorflow.keras.layers.experimental import preprocessing

from analyze import NN

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
from pgpi import *

if __name__ == "__main__":

    def usage():
        print("Usage: nn.py param_file")
        sys.exit(1)

    class PrintDot(keras.callbacks.Callback):
        def on_epoch_end(self, epoch, logs):
            if epoch % 100 == 0:
                print("")
            print(".", end="")

    def build_model(train_data, train_labels):
        model = keras.Sequential(
            [
                layers.Dense(10, activation="relu", input_shape=[train_data.shape[1]]),
                layers.Dense(10, activation="relu"),
                layers.Dense(1),
            ]
        )
        optimizer = tf.keras.optimizers.RMSprop(0.001)
        model.compile(loss="mse", optimizer=optimizer, metrics=["mae", "mse"])
        return model

    def merge_lists(xouter, xinner):
        _x = []
        for i in range(0, len(xouter)):
            _x.append([xouter[i], xinner[i]])
        return _x

    LOG_LEVEL = Log.info
    REPOSITORY = Common.REPOSITORY_DIR

    # Parse arguments
    parser = argparse.ArgumentParser(description="TODO ********")
    parser.add_argument("params_file", help="TODO *****", default=None)

    """
    Main procedure.
    """

    # Check args.
    args = parser.parse_args()

    params_file = args.params_file
    if params_file == None:
        usage()
    _path = params_file
    if os.path.exists(_path) == False:
        print("Error: {} Not Found.".format(params_file))
        sys.exit(1)

    # Read params.
    cm = Common()
    _dict = cm.read_plan_json(_path)
    del cm

    _depth = _dict["Depth"]
    _node_type = _dict["Node Type"]
    _Y = _dict["Y"]
    _Xouter = _dict["Xouter"]
    _Xinner = _dict["Xinner"]

    # Normalize
    _max = max(_Y)
    if _max < max(_Xouter):
        _max = max(_Xouter)
    if _max < max(_Xinner):
        _max = max(_Xinner)
    if _max != 0:
        _Y = list(map(lambda x: x / _max, _Y))
        _Xouter = list(map(lambda x: x / _max, _Xouter))
        _Xinner = list(map(lambda x: x / _max, _Xinner))

    # Make data
    _X = merge_lists(_Xouter, _Xinner)
    train_data = np.array(_X)
    train_labels = np.array(_Y)

    # Make model
    model = build_model(train_data, train_labels)
    model.summary

    #
    EPOCHS = 1000
    early_stop = keras.callbacks.EarlyStopping(monitor="val_loss", patience=1)
    history = model.fit(
        train_data,
        train_labels,
        epochs=EPOCHS,
        validation_split=0.2,
        verbose=0,
        callbacks=[early_stop, PrintDot()],
    )

    loss, mae, mse = model.evaluate(train_data, train_labels, verbose=2)
    print("Testing set Mean Abs Error: {:5.2f}".format(mae))
    test_predictions = model.predict(train_data).flatten()

    # Make predictions
    a = plt.axes(aspect="equal")
    plt.scatter(train_labels, test_predictions)
    plt.xlabel("True Values")
    plt.ylabel("Predictions")
    plt.axis("equal")
    plt.axis("square")
    plt.xlim([0, plt.xlim()[1]])
    plt.ylim([0, plt.ylim()[1]])
    _ = plt.plot([-1, 1], [-1, 1])
    plt.legend()
    plt.show()

    error = test_predictions - train_labels
    plt.hist(error, bins=25)
    plt.xlabel("Prediction Error")
    _ = plt.ylabel("Count")
    plt.legend()
    plt.show()
