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

#include <stddef.h>
#include <stdint.h>

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "absl/base/thread_annotations.h"
#include "absl/status/status.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "absl/synchronization/mutex.h"
#include "absl/types/optional.h"
#include "riegeli/base/arithmetic.h"
#include "riegeli/base/maker.h"
#include "riegeli/records/record_position.h"
#include "riegeli/records/record_reader.h"
#include "riegeli/records/skipped_region.h"
#include "riegeli/tensorflow/io/file_reader.h"
#include "tensorflow/core/framework/allocator.h"
#include "tensorflow/core/framework/dataset.h"
#include "tensorflow/core/framework/op_kernel.h"
#include "tensorflow/core/framework/op_requires.h"
#include "tensorflow/core/framework/tensor.h"
#include "tensorflow/core/framework/tensor_shape.h"
#include "tensorflow/core/framework/tensor_types.h"
#include "tensorflow/core/framework/types.h"
#include "tensorflow/core/framework/types.pb.h"
#include "tensorflow/core/graph/graph.h"
#include "tensorflow/core/platform/macros.h"
#include "tensorflow/core/platform/tstring.h"
#include "util/task/status_macros.h"

namespace riegeli {
namespace tensorflow {
namespace {

class RiegeliDatasetOp : public ::tensorflow::data::DatasetOpKernel {
 public:
  using DatasetOpKernel::DatasetOpKernel;

  void MakeDataset(::tensorflow::OpKernelContext* ctx,
                   ::tensorflow::data::DatasetBase** output) override {
    const ::tensorflow::Tensor* filenames_tensor;
    OP_REQUIRES_OK(ctx, ctx->input("filenames", &filenames_tensor));
    OP_REQUIRES(ctx, filenames_tensor->dims() <= 1,
                absl::InvalidArgumentError(
                    "`filenames` must be a scalar or a vector."));

    std::vector<std::string> filenames;
    filenames.reserve(IntCast<size_t>(filenames_tensor->NumElements()));
    for (int i = 0; i < filenames_tensor->NumElements(); ++i) {
      filenames.emplace_back(
          filenames_tensor->flat<::tensorflow::tstring>()(i));
    }

    int64_t min_buffer_size;
    OP_REQUIRES_OK(ctx, ::tensorflow::data::ParseScalarArgument<int64_t>(
                            ctx, "min_buffer_size", &min_buffer_size));
    int64_t max_buffer_size;
    OP_REQUIRES_OK(ctx, ::tensorflow::data::ParseScalarArgument<int64_t>(
                            ctx, "max_buffer_size", &max_buffer_size));

    *output = new Dataset(ctx, std::move(filenames), min_buffer_size,
                          max_buffer_size);
  }

 private:
  class Dataset : public ::tensorflow::data::DatasetBase {
   public:
    explicit Dataset(::tensorflow::OpKernelContext* ctx,
                     std::vector<std::string> filenames,
                     int64_t min_buffer_size, int64_t max_buffer_size)
        : DatasetBase(::tensorflow::data::DatasetContext(ctx)),
          filenames_(std::move(filenames)),
          min_buffer_size_(min_buffer_size),
          max_buffer_size_(max_buffer_size) {}

    std::unique_ptr<::tensorflow::data::IteratorBase> MakeIteratorInternal(
        const std::string& prefix) const override {
      return std::unique_ptr<::tensorflow::data::IteratorBase>(
          new Iterator({this, absl::StrCat(prefix, "::Riegeli")}));
    }

    const ::tensorflow::DataTypeVector& output_dtypes() const override {
      static const ::tensorflow::DataTypeVector* const dtypes =
          new ::tensorflow::DataTypeVector({::tensorflow::DT_STRING});
      return *dtypes;
    }

    const std::vector<::tensorflow::PartialTensorShape>& output_shapes()
        const override {
      static const std::vector<::tensorflow::PartialTensorShape>* const shapes =
          new std::vector<::tensorflow::PartialTensorShape>({{}});
      return *shapes;
    }

    std::string DebugString() const override {
      return "RiegeliDatasetOp::Dataset";
    }

    absl::Status CheckExternalState() const override {
      return absl::OkStatus();
    }

    absl::Status InputDatasets(
        std::vector<const ::tensorflow::data::DatasetBase*>* inputs)
        const override {
      inputs->clear();
      return absl::OkStatus();
    }

   protected:
    absl::Status AsGraphDefInternal(
        ::tensorflow::data::SerializationContext* ctx,
        DatasetGraphDefBuilder* b, ::tensorflow::Node** output) const override {
      ::tensorflow::Node* filenames = nullptr;
      RETURN_IF_ERROR(b->AddVector(filenames_, &filenames));
      ::tensorflow::Node* min_buffer_size = nullptr;
      RETURN_IF_ERROR(b->AddScalar(min_buffer_size_, &min_buffer_size));
      ::tensorflow::Node* max_buffer_size = nullptr;
      RETURN_IF_ERROR(b->AddScalar(max_buffer_size_, &max_buffer_size));
      RETURN_IF_ERROR(b->AddDataset(
          this, {filenames, min_buffer_size, max_buffer_size}, output));
      return absl::OkStatus();
    }

   private:
    class Iterator : public ::tensorflow::data::DatasetIterator<Dataset> {
     public:
      explicit Iterator(const Params& params) : DatasetIterator(params) {}

      absl::Status GetNextInternal(
          ::tensorflow::data::IteratorContext* ctx,
          std::vector<::tensorflow::Tensor>* out_tensors,
          bool* end_of_sequence) override ABSL_LOCKS_EXCLUDED(mu_) {
        absl::MutexLock lock(&mu_);
        for (;;) {
          if (reader_ != absl::nullopt) {
            // We are currently processing a file, so try to read the next
            // record.
            ::tensorflow::Tensor result_tensor(::tensorflow::cpu_allocator(),
                                               ::tensorflow::DT_STRING, {});
            absl::string_view value;
            if (TF_PREDICT_TRUE(reader_->ReadRecord(value))) {
              result_tensor.scalar<::tensorflow::tstring>()().assign(
                  value.data(), value.size());
              out_tensors->push_back(std::move(result_tensor));
              *end_of_sequence = false;
              return absl::OkStatus();
            }
            SkippedRegion skipped_region;
            if (reader_->Recover(&skipped_region)) {
              // File has invalid contents: return an error. Further iteration
              // will resume reading the file after the invalid region has been
              // skipped.
              *end_of_sequence = false;
              return absl::InvalidArgumentError(absl::StrCat(
                  "Skipping invalid region of a Riegeli/records file: ",
                  skipped_region));
            }
            if (TF_PREDICT_FALSE(!reader_->Close())) {
              // Failed to read the file: return an error.
              absl::Status status = reader_->status();
              // Further iteration will move on to the next file, if any.
              reader_.reset();
              ++current_file_index_;
              *end_of_sequence =
                  current_file_index_ == dataset()->filenames_.size();
              return status;
            }
            // We have reached the end of the current file, so move on to the
            // next file, if any.
            reader_.reset();
            ++current_file_index_;
          }

          // Iteration ends when there are no more files to process.
          if (current_file_index_ == dataset()->filenames_.size()) {
            *end_of_sequence = true;
            return absl::OkStatus();
          }

          // Actually move on to next file.
          OpenFile(ctx);
        }
      }

     protected:
      absl::Status SaveInternal(::tensorflow::data::SerializationContext* ctx,
                                ::tensorflow::data::IteratorStateWriter* writer)
          override ABSL_LOCKS_EXCLUDED(mu_) {
        absl::MutexLock lock(&mu_);
        RETURN_IF_ERROR(
            writer->WriteScalar(full_name("current_file_index"),
                                IntCast<int64_t>(current_file_index_)));
        if (reader_ != absl::nullopt) {
          RETURN_IF_ERROR(writer->WriteScalar(full_name("current_pos"),
                                              reader_->pos().ToBytes()));
        }
        return absl::OkStatus();
      }

      absl::Status RestoreInternal(
          ::tensorflow::data::IteratorContext* ctx,
          ::tensorflow::data::IteratorStateReader* reader) override
          ABSL_LOCKS_EXCLUDED(mu_) {
        absl::MutexLock lock(&mu_);
        current_file_index_ = 0;
        reader_.reset();

        int64_t current_file_index;
        RETURN_IF_ERROR(reader->ReadScalar(full_name("current_file_index"),
                                           &current_file_index));
        if (TF_PREDICT_FALSE(current_file_index < 0 ||
                             IntCast<uint64_t>(current_file_index) >
                                 dataset()->filenames_.size())) {
          return absl::InternalError("current_file_index out of range");
        }
        current_file_index_ = IntCast<size_t>(current_file_index);

        if (reader->Contains(full_name("current_pos"))) {
          if (TF_PREDICT_FALSE(current_file_index_ ==
                               dataset()->filenames_.size())) {
            return absl::InternalError("current_file_index out of range");
          }
          ::tensorflow::tstring current_pos;
          RETURN_IF_ERROR(
              reader->ReadScalar(full_name("current_pos"), &current_pos));
          RecordPosition pos;
          if (TF_PREDICT_FALSE(!pos.FromBytes(current_pos))) {
            return absl::InternalError(
                "current_pos is not a valid RecordPosition");
          }
          OpenFile(ctx);
          reader_->Seek(pos);
          // Any errors from seeking will be reported during reading.
        }
        return absl::OkStatus();
      }

     private:
      void OpenFile(::tensorflow::data::IteratorContext* ctx)
          ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_) {
        reader_.emplace(riegeli::Maker(
            dataset()->filenames_[current_file_index_],
            tensorflow::FileReaderBase::Options()
                .set_env(ctx->env())
                .set_min_buffer_size(
                    IntCast<size_t>(dataset()->min_buffer_size_))
                .set_max_buffer_size(
                    IntCast<size_t>(dataset()->max_buffer_size_))));
      }

      // Invariants:
      //   `current_file_index_ <= dataset()->filenames_.size()`
      //   if `current_file_index_ == dataset()->filenames_.size()` then
      //       `reader_ == absl::nullopt`

      absl::Mutex mu_;
      size_t current_file_index_ ABSL_GUARDED_BY(mu_) = 0;
      // `absl::nullopt` means not open yet.
      absl::optional<RecordReader<tensorflow::FileReader<>>> reader_
          ABSL_GUARDED_BY(mu_);
    };

    const std::vector<std::string> filenames_;
    const int64_t min_buffer_size_;
    const int64_t max_buffer_size_;
  };
};

REGISTER_KERNEL_BUILDER(Name("RiegeliDataset").Device(::tensorflow::DEVICE_CPU),
                        RiegeliDatasetOp);

}  // namespace
}  // namespace tensorflow
}  // namespace riegeli
