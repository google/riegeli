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

import tensorflow as tf

from tensorflow.python.data.ops import dataset_ops
from tensorflow.python.data.util import convert
from tensorflow.python.framework import load_library
from tensorflow.python.platform import resource_loader

gen_riegeli_dataset_ops = load_library.load_op_library(
    resource_loader.get_path_to_datafile('_riegeli_dataset_ops.so'))

__all__ = ('RiegeliDataset',)

_DEFAULT_MIN_BUFFER_SIZE = 4 << 10
_DEFAULT_MAX_BUFFER_SIZE = 64 << 10


class RiegeliDataset(dataset_ops.DatasetSource):
  """A `Dataset` comprising records from one or more Riegeli/records files."""

  __slots__ = ('_filenames', '_min_buffer_size', '_max_buffer_size')

  def __init__(self,
               filenames,
               min_buffer_size=None,
               max_buffer_size=None,
               buffer_size=None):
    """Creates a `RiegeliDataset`.

    Args:
      filenames: A `tf.string` tensor containing one or more filenames.
      min_buffer_size: A `tf.int64` scalar which tunes the minimal buffer size,
        which determines how much data at a time is typically read from the
        file. The actual buffer size changes between min_buffer_size and
        max_buffer_size depending on the access pattern. Default: 4K.
      max_buffer_size: A `tf.int64` scalar which tunes the maximal buffer size,
        which determines how much data at a time is typically read from the
        file. The actual buffer size changes between min_buffer_size and
        max_buffer_size depending on the access pattern. Default: 64K.
      buffer_size: If not None, a shortcut for setting min_buffer_size and
        max_buffer_size to the same value.
    """
    if buffer_size is not None:
      min_buffer_size = buffer_size
      max_buffer_size = buffer_size
    self._filenames = tf.convert_to_tensor(filenames, name='filenames')
    self._min_buffer_size = convert.optional_param_to_tensor(
        'min_buffer_size',
        min_buffer_size,
        argument_default=_DEFAULT_MIN_BUFFER_SIZE)
    self._max_buffer_size = convert.optional_param_to_tensor(
        'max_buffer_size',
        max_buffer_size,
        argument_default=_DEFAULT_MAX_BUFFER_SIZE)
    variant_tensor = gen_riegeli_dataset_ops.riegeli_dataset(
        self._filenames, self._min_buffer_size, self._max_buffer_size)
    super(RiegeliDataset, self).__init__(variant_tensor)

  @property
  def element_spec(self):
    return tf.TensorSpec([], tf.dtypes.string)
