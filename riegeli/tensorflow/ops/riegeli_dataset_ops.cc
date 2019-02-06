// Copyright 2019 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "tensorflow/core/framework/common_shape_fns.h"
#include "tensorflow/core/framework/op.h"

namespace riegeli {
namespace tensorflow {

REGISTER_OP("RiegeliDataset")
    .Input("filenames: string")
    .Output("handle: variant")
    .SetIsStateful()
    .SetShapeFn(::tensorflow::shape_inference::ScalarShape)
    .Doc(R"doc(
Creates a dataset that emits the records from one or more Riegeli/records files.

filenames: A scalar or vector containing the name(s) of the file(s) to be
  read.
)doc");

}  // namespace tensorflow
}  // namespace riegeli
