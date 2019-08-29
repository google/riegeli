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
"""PIP package setup for Riegeli."""

import setuptools
from setuptools import dist

with open('README.md', 'r') as fh:
  long_description = fh.read()


class BinaryDistribution(dist.Distribution):
  """This class is needed in order to create OS specific wheels."""

  def has_ext_modules(self):
    return True


setuptools.setup(
    name='riegeli',
    version='0.0.1',
    description='File format for storing a sequence of records',
    long_description=long_description,
    long_description_content_type='text/markdown',
    url='https://github.com/google/riegeli',
    author='Google LLC',
    author_email='compression-dev@google.com',
    license='Apache License, Version 2.0',
    python_requires='>=2.7,!=3.0.*,!=3.1.*,!=3.2.*,!=3.3.*,!=3.4.*,<4',
    install_requires=[
        'enum34;python_version<"3.4"',
        'protobuf>=3.8.0,<4',
    ],
    extras_require={
        'tensorflow': ['tensorflow>=1.15,<3'],
    },
    packages=setuptools.find_packages(),
    include_package_data=True,
    package_data={'': ['**/*.so']},
    distclass=BinaryDistribution,
    classifiers=[
        'Programming Language :: Python',
        'Intended Audience :: Developers',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'License :: OSI Approved :: Apache Software License',
        'Operating System :: OS Independent',
        'Topic :: Software Development :: Libraries',
        'Topic :: Software Development :: Libraries :: Python Modules',
    ],
)
