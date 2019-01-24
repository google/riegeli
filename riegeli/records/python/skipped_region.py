# Copyright 2018 Google LLC
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
"""Details about a skipped region of invalid file contents."""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

__all__ = ('SkippedRegion',)


class SkippedRegion(object):
  """Details about a skipped region of invalid file contents.

  Attributes:
      begin: File position of the beginning of the skipped region, inclusive.
      end: File position of the end of the skipped region, exclusive.
      length: Length of the skipped region, in bytes.
      message: Message explaining why the region is invalid.
  """

  __slots__ = ('begin', 'end', 'message')

  def __init__(self, begin, end, message):
    if begin > end:
      raise ValueError('Positions in the wrong order: {} > {}'.format(
          begin, end))
    self.begin = begin
    self.end = end
    self.message = message

  @property
  def length(self):
    return self.end - self.begin

  def __str__(self):
    return '[{}, {}): {}'.format(self.begin, self.end, self.message)

  def __repr__(self):
    return 'SkippedRegion({}, {}, {!r})'.format(self.begin, self.end,
                                                self.message)
