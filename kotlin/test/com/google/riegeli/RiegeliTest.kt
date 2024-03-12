// Copyright 2022 The Cross-Media Measurement Authors
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
// limitations under the \License.

package com.google.riegeli

import kotlin.test.assertEquals
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4

private const val DATA_PATH = "kotlin/test/test_data"

@RunWith(JUnit4::class)
class RiegeliTest {

    @Test
    fun `test toy model`() {
        val filename = "$DATA_PATH/toy_model_riegeli_list"
        val records = Riegeli().readCompressedFileWithRecords(filename)

        assertEquals(records.size, 140)
    }

    @Test
    fun `test single id model`() {
        val filename = "$DATA_PATH/single_id_model_riegeli_list"
        val records = Riegeli().readCompressedFileWithRecords(filename)

        assertEquals(records.size, 2)
    }

}
