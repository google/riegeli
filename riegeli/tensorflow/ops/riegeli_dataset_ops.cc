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
#include "tensorflow/core/framework/shape_inference.h"

namespace riegeli {
namespace tensorflow {

REGISTER_OP("RiegeliDataset")
    .Input("filenames: string")
    .Input("min_buffer_size: int64")
    .Input("max_buffer_size: int64")
    .Output("handle: variant")
    .SetIsStateful()
    .SetShapeFn([](::tensorflow::shape_inference::InferenceContext* c) {
      ::tensorflow::shape_inference::ShapeHandle unused;
      // `filenames` must be a scalar or a vector.
      TF_RETURN_IF_ERROR(c->WithRankAtMost(c->input(0), 1, &unused));
      // `min_buffer_size` could only be a scalar.
      TF_RETURN_IF_ERROR(c->WithRank(c->input(1), 0, &unused));
      // `max_buffer_size` could only be a scalar.
      TF_RETURN_IF_ERROR(c->WithRank(c->input(2), 0, &unused));
      return ::tensorflow::shape_inference::ScalarShape(c);
    })
    .Doc(R"doc(
Creates a dataset that emits the records from one or more Riegeli/records files.

filenames: A scalar or vector containing the name(s) of the file(s) to be
  read.
min_buffer_size: Tunes the minimal buffer size, which determines how much data
  at a time is typically read from the file. The actual buffer size changes
  between min_buffer_size and max_buffer_size depending on the access pattern.
max_buffer_size: Tunes the maximal buffer size, which determines how much data
  at a time is typically read from the file. The actual buffer size changes
  between min_buffer_size and max_buffer_size depending on the access pattern.
)doc");

}  // namespace tensorflow
}  // namespace riegeli
