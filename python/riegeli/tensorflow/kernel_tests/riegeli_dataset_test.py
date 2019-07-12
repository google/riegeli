# Copyright 2019 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Tests for RiegeliDataset."""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os

from builtins import range  # pylint: disable=redefined-builtin
import riegeli
from riegeli.tensorflow.ops import riegeli_dataset_ops
import tensorflow as tf

from tensorflow.python.data.ops import dataset_ops
from tensorflow.python.data.util import nest
from tensorflow.python.eager import context
from tensorflow.python.framework import errors
from tensorflow.python.framework import sparse_tensor
from tensorflow.python.framework import test_util
from tensorflow.python.ops import tensor_array_ops
from tensorflow.python.platform import test


# Adapted from tensorflow/python/data/kernel_tests/test_base.py
# which has restricted visibility.
class DatasetTestBase(test.TestCase):
  """Base class for dataset tests."""

  def getNext(self, dataset, requires_initialization=False, shared_name=None):
    """Returns a callable that returns the next element of the dataset.

    Example use:
    ```python
    # In both graph and eager modes
    dataset = ...
    get_next = self.getNext(dataset)
    result = self.evaluate(get_next())
    ```

    Args:
      dataset: A dataset whose elements will be returned.
      requires_initialization: Indicates that when the test is executed in graph
        mode, it should use an initializable iterator to iterate through the
        dataset (e.g. when it contains stateful nodes). Defaults to False.
      shared_name: (Optional.) If non-empty, the returned iterator will be
        shared under the given name across multiple sessions that share the same
        devices (e.g. when using a remote server).

    Returns:
      A callable that returns the next element of `dataset`. Any `TensorArray`
      objects `dataset` outputs are stacked.
    """

    def ta_wrapper(gn):

      def _wrapper():
        r = gn()
        if isinstance(r, tensor_array_ops.TensorArray):
          return r.stack()
        else:
          return r

      return _wrapper

    if context.executing_eagerly():
      iterator = iter(dataset)
      return ta_wrapper(iterator._next_internal)  # pylint: disable=protected-access
    else:
      if requires_initialization:
        iterator = dataset_ops.make_initializable_iterator(dataset, shared_name)
        self.evaluate(iterator.initializer)
      else:
        iterator = dataset_ops.make_one_shot_iterator(dataset)
      get_next = iterator.get_next()
      return ta_wrapper(lambda: get_next)

  def _compareOutputToExpected(self, result_values, expected_values,
                               assert_items_equal):
    if assert_items_equal:
      self.assertItemsEqual(result_values, expected_values)
      return
    for i in range(len(result_values)):
      nest.assert_same_structure(result_values[i], expected_values[i])
      for result_value, expected_value in zip(
          nest.flatten(result_values[i]), nest.flatten(expected_values[i])):
        self.assertValuesEqual(expected_value, result_value)

  def assertDatasetProduces(self,
                            dataset,
                            expected_output=None,
                            expected_shapes=None,
                            expected_error=None,
                            requires_initialization=False,
                            num_test_iterations=1,
                            assert_items_equal=False,
                            expected_error_iter=1):
    """Asserts that a dataset produces the expected output / error.

    Args:
      dataset: A dataset to check for the expected output / error.
      expected_output: A list of elements that the dataset is expected to
        produce.
      expected_shapes: A list of TensorShapes which is expected to match
        output_shapes of dataset.
      expected_error: A tuple `(type, predicate)` identifying the expected error
        `dataset` should raise. The `type` should match the expected exception
        type, while `predicate` should either be 1) a unary function that inputs
        the raised exception and returns a boolean indicator of success or 2) a
        regular expression that is expected to match the error message
        partially.
      requires_initialization: Indicates that when the test is executed in graph
        mode, it should use an initializable iterator to iterate through the
        dataset (e.g. when it contains stateful nodes). Defaults to False.
      num_test_iterations: Number of times `dataset` will be iterated. Defaults
        to 2.
      assert_items_equal: Tests expected_output has (only) the same elements
        regardless of order.
      expected_error_iter: How many times to iterate before expecting an error,
        if an error is expected.
    """
    self.assertTrue(
        expected_error is not None or expected_output is not None,
        'Exactly one of expected_output or expected error should be provided.')
    if expected_error:
      self.assertTrue(
          expected_output is None,
          'Exactly one of expected_output or expected error should be provided.'
      )
      with self.assertRaisesWithPredicateMatch(expected_error[0],
                                               expected_error[1]):
        get_next = self.getNext(
            dataset, requires_initialization=requires_initialization)
        for _ in range(expected_error_iter):
          self.evaluate(get_next())
      return
    if expected_shapes:
      self.assertEqual(expected_shapes,
                       dataset_ops.get_legacy_output_shapes(dataset))
    self.assertGreater(num_test_iterations, 0)
    for _ in range(num_test_iterations):
      get_next = self.getNext(
          dataset, requires_initialization=requires_initialization)
      result = []
      for _ in range(len(expected_output)):
        result.append(self.evaluate(get_next()))
      self._compareOutputToExpected(result, expected_output, assert_items_equal)
      with self.assertRaises(errors.OutOfRangeError):
        self.evaluate(get_next())
      with self.assertRaises(errors.OutOfRangeError):
        self.evaluate(get_next())


@test_util.run_all_in_graph_and_eager_modes
class RiegeliDatasetTest(DatasetTestBase):

  def setUp(self):
    super(RiegeliDatasetTest, self).setUp()
    self._num_files = 2
    self._num_records = 7

    self.test_filenames = self._create_files()

  def dataset_fn(self, filenames, num_epochs=1, batch_size=None):
    repeat_dataset = riegeli_dataset_ops.RiegeliDataset(filenames).repeat(
        num_epochs)
    if batch_size:
      return repeat_dataset.batch(batch_size)
    return repeat_dataset

  def _record(self, f, r):
    return 'Record {} of file {}'.format(r, f).encode()

  def _create_files(self):
    filenames = []
    for i in range(self._num_files):
      filename = os.path.join(self.get_temp_dir(), 'riegeli.{}'.format(i))
      filenames.append(filename)

      # Note: if records were serialized proto messages, passing
      # options='transpose' to RecordWriter would make compression better.
      with riegeli.RecordWriter(tf.gfile.GFile(filename, 'wb')) as writer:
        for j in range(self._num_records):
          writer.write_record(self._record(i, j))
    return filenames

  def test_read_one_epoch(self):
    # Basic test: read from file 0.
    dataset = self.dataset_fn(self.test_filenames[0])
    self.assertDatasetProduces(
        dataset,
        expected_output=[self._record(0, i) for i in range(self._num_records)])

    # Basic test: read from file 1.
    dataset = self.dataset_fn(self.test_filenames[1])
    self.assertDatasetProduces(
        dataset,
        expected_output=[self._record(1, i) for i in range(self._num_records)])

    # Basic test: read from both files.
    dataset = self.dataset_fn(self.test_filenames)
    expected_output = []
    for j in range(self._num_files):
      expected_output.extend(
          [self._record(j, i) for i in range(self._num_records)])
    self.assertDatasetProduces(dataset, expected_output=expected_output)

  def test_read_ten_epochs(self):
    dataset = self.dataset_fn(self.test_filenames, num_epochs=10)
    expected_output = []
    for j in range(self._num_files):
      expected_output.extend(
          [self._record(j, i) for i in range(self._num_records)])
    self.assertDatasetProduces(dataset, expected_output=expected_output * 10)

  def test_read_ten_epochs_of_batches(self):
    dataset = self.dataset_fn(
        self.test_filenames, num_epochs=10, batch_size=self._num_records)
    expected_output = []
    for j in range(self._num_files):
      expected_output.append(
          [self._record(j, i) for i in range(self._num_records)])
    self.assertDatasetProduces(dataset, expected_output=expected_output * 10)


if __name__ == '__main__':
  tf.test.main()
