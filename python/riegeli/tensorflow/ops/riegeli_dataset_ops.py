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

from riegeli.tensorflow.ops import gen_riegeli_dataset_ops
import tensorflow as tf

from tensorflow.python.data.ops import dataset_ops
from tensorflow.python.data.util import structure
from tensorflow.python.framework import load_library
from tensorflow.python.platform import resource_loader

load_library.load_op_library(
    resource_loader.get_path_to_datafile('_riegeli_dataset_ops.so'))

__all__ = ('RiegeliDataset',)


class RiegeliDataset(dataset_ops.DatasetSource):
  """A `Dataset` comprising records from one or more Riegeli/records files."""

  __slots__ = ('_filenames',)

  def __init__(self, filenames):
    """Creates a `RiegeliDataset`.

    Args:
      filenames: A `tf.string` tensor containing one or more filenames.
    """
    self._filenames = tf.convert_to_tensor(filenames, name='filenames')
    variant_tensor = gen_riegeli_dataset_ops.riegeli_dataset(self._filenames)
    super(RiegeliDataset, self).__init__(variant_tensor)

  @property
  def element_spec(self):
    return structure.TensorStructure(tf.dtypes.string, [])
