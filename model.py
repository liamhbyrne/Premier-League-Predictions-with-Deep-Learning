import os
from numpy.core._multiarray_umath import ndarray
os.environ['TF_CPP_MIN_LOG_LEVEL'] = '3'
import tensorflow as tf
from tensorflow import keras
from typing import *
from player import *
from tensorflow.python.keras.callbacks import TensorBoard
from time import time
import matplotlib.pyplot as plt


class NeuralNet:
    """
    The class Neural Net encapsulates all methods needed to utilise TensorFlow with
    Keras to create a Neural Network Model. Acts as a wrapper to create, compile,
    test, store and load models.
    """
    def __init__(self):
        self._model : object = None

    def createNN(self) -> None:
        """
        Creates the schematics of the multilayer perceptron with the hyperparameters being
        units/neurons per layer and their respective activation functions.
        Acts as a setter for the only initilised attribute, model.
        """
        nn_model = tf.keras.Sequential([
            keras.layers.Dense(units=10, input_shape=(10,)),
            keras.layers.Dense(units=20, activation=tf.nn.leaky_relu),
            keras.layers.Dense(units=15, activation=tf.nn.leaky_relu),
            keras.layers.Dense(units=3, activation=tf.nn.softmax)
        ])
        nn_model.summary()  #Outputs schema of model to console
        self._model = nn_model


    def compileModel(self, loss_function : str = 'sparse_categorical_crossentropy', metrics : str = 'accuracy') -> None:
        """
        Compiles the schematics set out by createNN. Sets the final hyperparameters
        used, such as the type of optimizer, the loss/cost function and choice of
        metrics for testing phase
        :param loss_function: String of the name of the cost function that will be compiled
        :param metrics: String of the name of the performance metric which will be calculated
        """
        optimizer = keras.optimizers.Adam()
        self._model.compile(optimizer=optimizer,
                            loss=loss_function,
                            metrics=[metrics])

    def fitModel(self, training_features : np.ndarray, training_results_labels : np.ndarray, epochs : int) -> None:
        """
        Takes the training set and its corresponding labels of the match outcome.
        Uses Keras to perform propagation and backpropagation to train the network.
        The final hyperparameters are set - batch size, epoch and callback to TensorBoard
        for further visualisation.
        :param training_features: NumPy array of the feature vectors (shape [10,])
        :param training_results_labels: NumPy array of all the corresponding outcomes
        :param epochs: Integer of the epoch count training will be carried through
        """
        tensorboard = TensorBoard(log_dir="logs\{}".format(time()))
        try:
            self._model.fit(training_features, training_results_labels, epochs=epochs, callbacks=[tensorboard], batch_size=32)
        except ValueError as e:
            print("FAILED TRAINING, bad vector shape? not using NumPy array? :", e)
            exit(1)

    def saveModel(self, file_name :  str) -> None:
        """
        Saves the weights and hyperparameters with the current iteration of the network
        :param file_name: String of the .h5 file the contents of the network will be
        saved to.
        """
        try:
            self._model.save(file_name)
        except OSError as e:
            print("failed creating file:", e)

    def loadModel(self, model_path_dir : str) -> None:
        """
        Loads a pre-trained Neural Network with all weights and hyperparameters.
        Used in GUI.
        :param model_path_dir: String of the location of the .h5 file
        """
        try:
            self._model = keras.models.load_model(model_path_dir,
                                                  custom_objects={'leaky_relu' : tf.nn.leaky_relu})  #Custom object used to use correct activation function
        except OSError as e:
            print("failed opening file, maybe doesn't exist", e)

    def predictOutcome(self, features : np.ndarray) -> int:
        """
        Feeds the model a feature vector to make a prediction on
        :param features: NumPy array [10,] feature vector
        :return: Integer of the result [0,1,2]
        """
        prediction = self._model.predict_classes(features)
        print(self._model.predict(features))
        return prediction

    def evaluateAccuracy(self, test_data : np.ndarray, test_labels : np.ndarray) -> float:
        """
        Performs the testing phase over the testing set, returns the final loss and the
        test accuracy of the model
        :param test_data: NumPy feature vectors of each match in the testing set
        :param test_labels: Corresponding NumPy array of match outcomes
        :return: float value of the test accuracy
        """
        loss, test_statistic = self._model.evaluate(test_data, test_labels)
        return test_statistic


def getTrainingTesting(train_test_ratio : float, shuffle : bool) -> Tuple[np.ndarray,
                                                np.ndarray, np.ndarray, np.ndarray]:
    """
    Collates a training and testing set for the format of a TensorFlow model
    :param train_test_ratio: float value of the ratio training : testing
    :param shuffle: the match data will be shuffled before split
    :return: Tuple of NumPy Arrays
    """
    matches: List[List,] = buildSets(shuffle)
    training_set : List
    testing_set  : List
    training_set, testing_set = splitTrainingTesting(matches, train_test_ratio)
    """
    Feature sets take each element in the feature vector excluding the final column
    relating to the actual result.
    Label sets take only the final column related to the actual result
    """
    training_features : ndarray = np.array([feature[:-1] for feature in training_set])
    training_labels   : ndarray = np.array([feature[-1] for feature in training_set])

    testing_features  : ndarray = np.array([feature[:-1] for feature in testing_set])
    testing_labels    : ndarray = np.array([feature[-1] for feature in testing_set])

    return (training_features, training_labels, testing_features, testing_labels)

def trainModel(training_features : np.ndarray, training_labels : np.ndarray, epoch_count : int) -> NeuralNet:
    """
    Instantiates the class NeuralNet to create a trained model
    :param training_features: NumPy array of feature vectors [10,]
    :param training_labels: NumPy array of feature vectors [10,]
    :param epoch_count: Integer of number of complete dataset iterations in training
    :return: NeuralNet object with trained model
    """
    nn = NeuralNet()
    nn.createNN()
    nn.compileModel()
    nn.fitModel(training_features=training_features, training_results_labels=training_labels, epochs=epoch_count)
    return nn

def testModel(testing_features : np.ndarray, testing_labels : np.ndarray, model : NeuralNet,
                                                        save_dir : str = '') -> float:
    """
    Tests a pre-trained model and offers option to save checkpoint of model
    :param testing_features:NumPy array of feature vectors [10,]
    :param testing_labels: NumPy array of feature vectors [10,]
    :param model: Pre-trained NeuralNet object
    :param save_dir: String of location the model can be saved to, defaults to empty
    :return: Float value of the test statistic during testing
    """
    test_stat = model.evaluateAccuracy(testing_features, testing_labels)
    print("Test statistic: ", test_stat)
    if save_dir:
        model.saveModel(save_dir)
    return test_stat

def plotEpochAccuracy(epoch_gap : int, number_of_points : int):
    """
    Subroutine to create multiple iterations of the model, the accuracy is plotted
    to visualise epoch against accuracy
    :param epoch_gap: integer epoch step size between the next iteration
    :param number_of_points: integer of number of model iterations
    """
    epoch_counter = 1
    epoch_number  = []
    accuracy      = []
    training_features, training_labels, testing_features, testing_labels = getTrainingTesting(train_test_ratio=0.7, shuffle=True)
    for model_number in range(number_of_points):
        current_model = trainModel(training_features=training_features, training_labels=training_labels, epoch_count=epoch_counter)
        accuracy.append(testModel(testing_features=testing_features, testing_labels=testing_labels, model=current_model))
        epoch_number.append(epoch_counter)
        epoch_counter += epoch_gap
    plt.style.use('Solarize_Light2')
    plt.title('Accuracy by Epoch')
    plt.xlabel('Epoch Count')
    plt.ylabel('Test Accuracy')
    print(epoch_number, accuracy)
    plt.plot(epoch_number, accuracy)
    plt.show()

def loadExistingModel(save_dir : str) -> NeuralNet:
    """
    Subroutine used by gui to load an existing model
    :param save_dir: string of path of .h5 checkpoint file
    :return:
    """
    existing_model = NeuralNet()
    existing_model.loadModel(model_path_dir=save_dir)
    return existing_model

if __name__ == '__main__':
    existing = loadExistingModel('my_model.h5')
    test_features, test_labels = getTrainingTesting(0.7, True)[-2:]
    testModel(testing_features=test_features, testing_labels=test_labels, model=existing)
    training_features, training_labels, testing_features, testing_labels = getTrainingTesting(train_test_ratio=0.7, shuffle=True)
    new_model = trainModel(training_features=training_features, training_labels=training_labels, epoch_count=40)
    new_model.saveModel(file_name='payOutModel.h5')
    testModel(testing_features=testing_features, testing_labels=testing_labels, model=new_model)
    plotEpochAccuracy(epoch_gap=1, number_of_points=40)
