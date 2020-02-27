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
"""TensorFlow dataset for Riegeli/records files."""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import tensorflow as tf

from tensorflow.python.data.ops import dataset_ops
from tensorflow.python.data.util import convert
from tensorflow.python.framework import load_library
from tensorflow.python.platform import resource_loader

gen_riegeli_dataset_ops = load_library.load_op_library(
    resource_loader.get_path_to_datafile('_riegeli_dataset_ops.so'))

__all__ = ('RiegeliDataset',)

_DEFAULT_BUFFER_SIZE = 64 << 10


class RiegeliDataset(dataset_ops.DatasetSource):
  """A `Dataset` comprising records from one or more Riegeli/records files."""

  __slots__ = ('_filenames', '_buffer_size')

  def __init__(self, filenames, buffer_size=None):
    """Creates a `RiegeliDataset`.

    Args:
      filenames: A `tf.string` tensor containing one or more filenames.
      buffer_size: A `tf.int64` scalar which tunes how much data is buffered
        after reading from the file. Default: 64K.
    """
    self._filenames = tf.convert_to_tensor(filenames, name='filenames')
    self._buffer_size = convert.optional_param_to_tensor(
        'buffer_size', buffer_size, argument_default=_DEFAULT_BUFFER_SIZE)
    variant_tensor = gen_riegeli_dataset_ops.riegeli_dataset(
        self._filenames, self._buffer_size)
    super(RiegeliDataset, self).__init__(variant_tensor)

  @property
  def element_spec(self):
    return tf.TensorSpec([], tf.dtypes.string)
