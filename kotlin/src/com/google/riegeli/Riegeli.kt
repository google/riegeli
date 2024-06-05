// Copyright 2021 The Cross-Media Measurement Authors
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

package com.google.riegeli

import com.google.highwayhash.HighwayHash
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.File
import java.io.InputStream
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.util.LinkedList
import org.apache.commons.compress.compressors.CompressorInputStream
import org.apache.commons.compress.compressors.brotli.BrotliCompressorInputStream
import org.apache.commons.compress.compressors.snappy.SnappyCompressorInputStream
import org.apache.commons.compress.compressors.zstandard.ZstdCompressorInputStream
import org.apache.commons.compress.utils.CountingInputStream


// Package-level constant representing the fixed-size of a block within a Riegeli compressed file.
const val kBlockSize = 1 shl 16

/**
 * Converts a bytearray representing a little-endian, 64-bit number into a long.
 *
 * @param byteArray A ByteArray with 8 elements which represents a little-endian, 64-bit number.
 * @return A long representation of the provided ByteArray.
 * @throws IndexOutOfBoundsException if the size of the provided ByteArray is not 8.
 */
fun readLittleEndian64Long(byteArray: ByteArray): Long {
    if (byteArray.size != 8) {
        throw IndexOutOfBoundsException("Byte array provided is not the correct length")
    }

    return ByteBuffer.wrap(byteArray).order(ByteOrder.LITTLE_ENDIAN).long
}

/**
 * Converts a bytearray representing a little-endian, 64-bit number into an int.
 *
 * @param byteArray A ByteArray with 8 elements which represents a little-endian, 64-bit number.
 * @return An integer representation of the provided ByteArray.
 * @throws IndexOutOfBoundsException if the size of the provided ByteArray is not 8.
 */
fun readLittleEndian64Int(byteArray: ByteArray): Int {
    return readLittleEndian64Long(byteArray).toInt()
}

/**
 * Converts a bytearray representing a little-endian, 56-bit number into an int.
 *
 * @param byteArray A ByteArray with 7 elements which represents a little-endian, 56-bit number.
 * @return An integer representation of the provided ByteArray.
 * @throws IndexOutOfBoundsException if the size of the provided ByteArray is not 7.
 */
fun readLittleEndian56Int(byteArray: ByteArray): Int {
    if (byteArray.size != 7) {
        throw IndexOutOfBoundsException("Byte array provided is not the correct length")
    }

    val tempOutputStream = ByteArrayOutputStream()
    tempOutputStream.write(byteArray)
    tempOutputStream.write(0x00)
    return readLittleEndian64Int(tempOutputStream.toByteArray())
}

/**
 * Reads a varint from an InputStream.
 * Varints are variable-width integers.
 *
 * Code snippet taken from
 *   https://github.com/bazelbuild/bazel/blob/master/src/main/java/com/google/devtools/build/lib/util/VarInt.java
 *
 * @param inputStream The input stream from which the varint should be read.
 * @return The varint which has been read from the input stream.
 */
fun getVarInt(inputStream: InputStream): Int {
    var result = 0
    var shift = 0
    var b: Int
    do {
        if (shift >= 32) {
            // Out of range
            throw IndexOutOfBoundsException("varint too long")
        }
        // Get 7 bits from next byte
        b = inputStream.read()
        result = result or (b and 0x7F shl shift)
        shift += 7
    } while (b and 0x80 != 0)
    return result
}

/**
 * Generates a HighwayHash object with the key
 * 'Riegeli/', 'records\n', 'Riegeli/', 'records\n'
 *
 * @return A HighwayHash object with specified key.
 */
fun generateHighwayHash(): HighwayHash {
    return HighwayHash(0x2f696c6567656952, 0x0a7364726f636572, 0x2f696c6567656952, 0x0a7364726f636572)
}

/**
 * Native kotlin implementation of a Riegeli decompressor.
 *
 * Follows the Riegeli/Records standard as specified here:
 * https://github.com/google/riegeli/blob/master/doc/riegeli_records_file_format.md
 */
class Riegeli {

    /**
     * Represents the contents of a chunk within a Riegeli compressed file.
     *
     * The full specification of a chunk is provided here:
     *   https://github.com/google/riegeli/blob/master/doc/riegeli_records_file_format.md#chunk
     *
     * Chunks contain a chunk header (40 bytes) followed by data.
     *
     * Chunks are interrupted by block headers at every multiple of the block size (which is 64 KiB).
     *
     * @param headerHash      64-bit ByteArray containing a HighwayHash of the other chunk header elements
     *                        (dataSize, dataHash, chunkType, numRecords, and decodedDataSize).
     * @param dataSize        ByteArray representing the size of the chunk data, excluding intervening block headers,
     *                        as a 64-bit, little-endian long.
     * @param dataHash        ByteArray containing a HighwayHash of the data within the chunk.
     * @param chunkType       ByteArray containing a single byte which represents the chunk type.
     *                          - If chunkType is 0x6d, the chunk is a file metadata chunk.
     *                          - If chunkType is 0x70, the chunk is a padding chunk.
     *                          - If chunkType is 0x72, the chunk is a simple chunk with records.
     *                          - if chunkType is 0x74, the chunk is a transposed chunk with records.
     * @param numRecords      ByteArray representing the number of records inside the chunk as a 64-bit,
     *                        little-endian long.
     * @param decodedDataSize ByteArray representing the size of the chunk's data after it has been decoded as a 64-bit,
     *                        little-endian long
     * @param data            ByteArray with the chunk's compressed data. The array's size is represented by dataSize.
     */
    private class Chunk private constructor(val headerHash: ByteArray, val dataSize: ByteArray, val dataHash: ByteArray, val chunkType: ByteArray, val numRecords: ByteArray, val decodedDataSize: ByteArray, val data: ByteArray) {


        companion object {

            /**
             * Constructs and returns a new Chunk object from an input stream.
             *
             * Validates the chunk header and data against their respective hashes.
             *
             * Identifies and skips intervening block headers.
             *
             * @param inputStream Counting input stream from which the chunk should be read. The input stream's count must
             *                    have started at the beginning of a file.
             * @throws Exception if the chunk's header does not hash properly, meaning that the chunk header is corrupted.
             * @throws Exception if the hash of the chunk's data does not match dataHash within the chunk header, meaning
             *         the chunk's data is corrupted.
             * @return A new chunk object as read from the inputStream.
             */
            fun fromInputStream(inputStream: CountingInputStream): Chunk? {

                val headerHash = ByteArray(8)
                val dataSize = ByteArray(8)
                val dataHash = ByteArray(8)
                val chunkType = ByteArray(1)
                val numRecords = ByteArray(7)
                val decodedDataSize = ByteArray(8)

                if (inputStream.read(headerHash) == -1) {
                    //There is no more to read
                    return null
                }
                inputStream.read(dataSize)
                inputStream.read(dataHash)
                inputStream.read(chunkType)
                inputStream.read(numRecords)
                inputStream.read(decodedDataSize)

                if (!isValidHeader(headerHash, dataSize, dataHash, chunkType, numRecords, decodedDataSize)) {
                    throw Exception("Riegeli: Chunk header is invalid")
                }

                val dataStream = ByteArrayOutputStream()
                val dataSizeInt = readLittleEndian64Int(dataSize)

                repeat(dataSizeInt) {

                    if (inputStream.bytesRead % kBlockSize == 0L) {
                        BlockHeader.fromInputStream(inputStream)
                    }

                    dataStream.write(inputStream.read())
                }

                val data = dataStream.toByteArray()

                if (!isValidData(dataSize, dataHash, data)) {
                    throw Exception("Riegeli: Chunk data is invalid")
                }

                return Chunk(headerHash, dataSize, dataHash, chunkType, numRecords, decodedDataSize, data)
            }

            /**
             * Helper function for the chunk builder function.
             * Compares the headerHash element of the chunk's header to a highway hash of the chunk's other header elements.
             *
             * @param headerHash      64-bit ByteArray containing a HighwayHash of the other chunk header elements
             *                        (dataSize, dataHash, chunkType, numRecords, and decodedDataSize).
             * @param dataSize        ByteArray representing the size of the chunk data, excluding intervening block headers,
             *                        as a 64-bit, little-endian long.
             * @param dataHash        ByteArray containing a HighwayHash of the data within the chunk.
             * @param chunkType       ByteArray containing a single byte which represents the chunk type.
             * @param numRecords      ByteArray representing the number of records inside the chunk as a 64-bit,
             *                        little-endian long.
             * @param decodedDataSize ByteArray representing the size of the chunk's data after it has been decoded as a
             *                        64-bit, little-endian long
             * @return True if the chunk's header is valid, false if it is not.
             */
            private fun isValidHeader(headerHash: ByteArray, dataSize: ByteArray, dataHash: ByteArray, chunkType: ByteArray, numRecords: ByteArray, decodedDataSize: ByteArray): Boolean {
                val highwayHash = generateHighwayHash()

                val headerData = dataSize + dataHash + chunkType + numRecords + decodedDataSize

                highwayHash.updatePacket(headerData, 0)

                val hashedDataString = java.lang.Long.toHexString(highwayHash.finalize64())

                val headerHashString = java.lang.Long.toHexString(readLittleEndian64Long(headerHash))

                return hashedDataString.equals(headerHashString)
            }

            /**
             * Helper function for the chunk builder function.
             * Compares the dataHash element of the chunk's header to a highway hash of the chunk's data.
             *
             * @param dataSize  ByteArray representing the size of the chunk data, excluding intervening block headers,
             *                  as a 64-bit, little-endian long.
             * @param dataHash  ByteArray containing a HighwayHash of the data within the chunk.
             * @param data      ByteArray with the chunk's compressed data. The array's size is represented by dataSize.
             * @return True if the chunk's header is valid, false if it is not.
             */
            private fun isValidData(dataSize: ByteArray, dataHash: ByteArray, data: ByteArray): Boolean {
                val highwayHash = generateHighwayHash()

                var position = 0
                val dataSizeInt = readLittleEndian64Int(dataSize)

                while (dataSizeInt - position >= 32) {
                    highwayHash.updatePacket(data, position)
                    position += 32
                }

                if (dataSizeInt - position > 0) {
                    highwayHash.updateRemainder(data, position, dataSizeInt - position)
                }

                val dataHashString = java.lang.Long.toHexString(readLittleEndian64Long(dataHash))
                val hashedDataString = java.lang.Long.toHexString(highwayHash.finalize64())

                return dataHashString.equals(hashedDataString)
            }
        }

        /**
         * Creates and returns a ByteArrayInputStream of the chunk's compressed data.
         *
         * @return A ByteArrayInputStream of the chunk's compressed data.
         */
        fun dataInputStream(): InputStream {
            return ByteArrayInputStream(this.data)
        }

        /**
         * Reads a compressed buffer from a chunk with records.
         *
         * Compressed buffers, if compressionType is not 0, are prefixed with a varint containing their decompressed size.
         *
         * @param inputStream     The input stream from which the buffer should be read.
         * @param compressedSize  The size of the buffer, the number of bytes that should be read from the input stream.
         * @param compressionType A Byte representing the type of compression used in the compressed buffer.
         *                          - 0x00: none
         *                          - 0x62: Brotli
         *                          - 0x7a: Zstd
         *                          - 0x73: Snappy
         * @return A ByteArray containing the decompressed data from the buffer.
         */
        private fun readCompressedBuffer(inputStream: CountingInputStream, compressedSize: Int, compressionType: Byte): ByteArray {
            val startingPoint = inputStream.bytesRead

            //If compression type is 0, there is not a varint at the beginning of the buffer so do not read it.
            val decompressedSize = if (compressionType == 0x00.toByte()) -1 else getVarInt(inputStream)

            val sizeOfVarInt = (inputStream.bytesRead - startingPoint).toInt()

            val compressedByteArray = ByteArray(compressedSize - sizeOfVarInt)

            inputStream.read(compressedByteArray)

            // If compression type is 0, the output stream is already decompressed
            if (compressionType == 0x00.toByte()) {
                return compressedByteArray
            }

            val tempInputStream = ByteArrayInputStream(compressedByteArray)

            val compressedInputStream: CompressorInputStream = when (compressionType) {
                0x62.toByte() -> BrotliCompressorInputStream(tempInputStream)
                0x7a.toByte() -> ZstdCompressorInputStream(tempInputStream)
                0x73.toByte() -> SnappyCompressorInputStream(tempInputStream)
                else -> throw Exception("Invalid Compression Type for Compressed Buffer")
            }

            val byteArr = ByteArray(decompressedSize)

            compressedInputStream.read(byteArr)

            return byteArr
        }

        /**
         * Gets the records stored within the chunk as a list of ByteArrays.
         *
         * The full specification for a chunk with records can be found here:
         *  https://github.com/google/riegeli/blob/master/doc/riegeli_records_file_format.md#simple-chunk-with-records
         *
         * A chunk with record's data contains the following:
         *  - compressionType:      A single byte representing which type of compression was used within the chunk.
         *  - compressedSizesSize:  A varint representing the size of compressedSizes
         *  - compressedSizes:      A compressed buffer of size compressedSizesSize. Contains numRecords varints, the size
         *                          of each record.
         *  - compressedValues:     A compressed buffer with the record values. After decompression,
         *                          contains decodedDataSize bytes.
         *
         * @return  A list where each element is a ByteArray representing a decompressed record. If there are no records
         *          within the chunk, an empty list is returned.
         */
        fun getRecords(): LinkedList<ByteArray> {

            //If the chunk is not a chunk with records, return an empty list
            if (this.chunkType[0] != 0x72.toByte()) {
                return LinkedList<ByteArray>()
            }

            val inputStream = CountingInputStream(this.dataInputStream())

            val startingPoint = inputStream.bytesRead

            val compressionType = ByteArray(1)
            inputStream.read(compressionType)

            val compressedSizesSizeInt = getVarInt(inputStream)

            val numRecords = readLittleEndian56Int(this.numRecords)

            val sizesByteArr = readCompressedBuffer(inputStream, compressedSizesSizeInt, compressionType[0])
            val sizesStream = ByteArrayInputStream(sizesByteArr)
            val sizes = LinkedList<Int>()

            repeat(numRecords) {
                sizes.add(getVarInt(sizesStream))
            }

            val chunkBeginningSize = inputStream.bytesRead - startingPoint
            val encodedDataSize = (readLittleEndian64Long(this.dataSize) - chunkBeginningSize).toInt()

            val valuesByteArr = readCompressedBuffer(inputStream, encodedDataSize, compressionType[0])

            val valuesByteBuffer = ByteBuffer.wrap(valuesByteArr)

            val records = LinkedList<ByteArray>()

            for (size in sizes) {
                val arr = ByteArray(size)
                valuesByteBuffer.get(arr)
                records.add(arr)
            }

            return records
        }
    }

    /**
     * Represents the contents of a block header within a Riegeli compressed file.
     *
     * The full specification of a block header is provided here:
     *   https://github.com/google/riegeli/blob/master/doc/riegeli_records_file_format.md#block-header
     *
     * @param headerHash      64-bit ByteArray containing a HighwayHash of the other block header elements
     *                        (previousChunk and nextChunk).
     * @param previousChunk   ByteArray representing the distance from the beginning of the chunk interrupted by this
     *                        block header to the beginning of the block as a 64-bit, little-endian long.
     * @param nextChunk       ByteArray representing the distance from the beginning of the block to the end of the chunk
     *                        interrupted by this block header as a 64-bit, little-endian long.
     */
    private class BlockHeader private constructor(val headerHash: ByteArray, val previousChunk: ByteArray, val nextChunk: ByteArray) {

        companion object {

            /**
             * Constructs and returns a new BlockHeader object from an input stream.
             *
             * Validates the block header its hash.
             *
             * @param inputStream Counting input stream from which the chunk should be read. The input stream's count must
             *                    have started at the beginning of a file.
             * @throws Exception if the block header does not hash properly, meaning that the block header is corrupted.
             * @return A new BlockHeader object as read from the inputStream.
             */
            fun fromInputStream(inputStream: InputStream): BlockHeader {
                val headerHash = ByteArray(8)
                val previousChunk = ByteArray(8)
                val nextChunk = ByteArray(8)

                inputStream.read(headerHash)
                inputStream.read(previousChunk)
                inputStream.read(nextChunk)

                if (!isValid(headerHash, previousChunk, nextChunk)) {
                    throw Exception("Riegeli: Block header is invalid")
                }

                return BlockHeader(headerHash, previousChunk, nextChunk)
            }

            /**
             * Helper function for the block header builder function.
             * Compares the headerHash element of the block's header to a highway hash of the block's other header elements.
             *
             * @param headerHash      64-bit ByteArray containing a HighwayHash of the other chunk header elements
             *                        (dataSize, dataHash, chunkType, numRecords, and decodedDataSize).
             * @param previousChunk   ByteArray representing the distance from the beginning of the chunk interrupted by this
             *                        block header to the beginning of the block as a 64-bit, little-endian long.
             * @param nextChunk       ByteArray representing the distance from the beginning of the block to the end of the chunk
             *                        interrupted by this block header as a 64-bit, little-endian long.
             * @return True if the block's header is valid, false if it is not.
             */
            private fun isValid(headerHash: ByteArray, previousChunk: ByteArray, nextChunk: ByteArray): Boolean {
                val highwayHash = generateHighwayHash()

                val headerData = previousChunk + nextChunk

                highwayHash.updateRemainder(headerData, 0, headerData.size)

                val hashedDataString = java.lang.Long.toHexString(highwayHash.finalize64())

                val headerHashString = java.lang.Long.toHexString(readLittleEndian64Long(headerHash))

                return hashedDataString.equals(headerHashString)
            }
        }
    }

    /**
     * Reads and decompresses a Riegeli compressed input stream with records.
     *
     * @param incomingInputStream An input stream which contains data which should be decompressed using Riegeli.
     * @return A list where each element is a ByteArray containing the bytes of a record.
     */
    fun readCompressedInputStreamWithRecords(incomingInputStream: InputStream): List<ByteArray> {
        val inputStream = CountingInputStream(incomingInputStream)
        BlockHeader.fromInputStream(inputStream)

        Chunk.fromInputStream(inputStream)
        val records = LinkedList<ByteArray>()

        while (true) {
            val chunk = Chunk.fromInputStream(inputStream) ?: break

            //Chunk type is simple chunk with records
            if (chunk.chunkType[0] == 0x72.toByte()) {
                records.addAll(chunk.getRecords())
            } else {
                println("NON RECORD CHUNK -- Chunk type: ${chunk.chunkType[0]}")
            }
        }

        return records
    }

    /**
     * Reads and decompresses a Riegeli compressed file with records.
     *
     * @param filename The path of the file which should be decompressed using Riegeli.
     * @return A list where each element is a ByteArray containing the bytes of a record.
     */
    fun readCompressedFileWithRecords(filename: String): List<ByteArray> {
        val inputStream = File(filename).inputStream()

        val list = readCompressedInputStreamWithRecords(inputStream)

        inputStream.close()

        return list
    }
}
