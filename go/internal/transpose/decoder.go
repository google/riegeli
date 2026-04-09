package transpose

import (
	"bytes"
	"fmt"

	"github.com/google/riegeli-go/internal/chunk"
	"github.com/google/riegeli-go/internal/compression"
	"github.com/google/riegeli-go/internal/varint"
)

// stateMachineNode represents one state in the transpose state machine.
type stateMachineNode struct {
	tag        uint32  // proto tag
	tagData    []byte  // varint-encoded tag (1-5 bytes) + inline value byte
	subtype    uint8   // subtype for varint fields
	bufferIdx  int     // index into buffers array (-1 if none)
	nextNodeID int     // index of next node (after subtracting state machine size for implicit)
	isImplicit bool    // whether transition is implicit (no transition byte consumed)
	messageID  uint32  // one of MessageID* constants, or a tag value
}

const (
	maxDecodedDataSize = 1 << 30  // 1 GiB
	maxNumRecords      = 100 << 20 // ~100 million
)

// Decode decodes a transposed chunk into individual records.
// data is the chunk data (after the 40-byte chunk header).
// numRecords and decodedDataSize come from the chunk header.
func Decode(data []byte, numRecords, decodedDataSize uint64) ([][]byte, error) {
	if numRecords > maxNumRecords {
		return nil, fmt.Errorf("transpose: num_records %d exceeds maximum %d", numRecords, maxNumRecords)
	}
	if decodedDataSize > maxDecodedDataSize {
		return nil, fmt.Errorf("transpose: decoded_data_size %d exceeds maximum %d", decodedDataSize, maxDecodedDataSize)
	}
	if len(data) == 0 {
		if numRecords == 0 {
			return nil, nil
		}
		return nil, fmt.Errorf("transpose: empty data for non-empty chunk")
	}

	r := newByteReader(data)

	// Read compression type.
	ctByte, err := r.ReadByte()
	if err != nil {
		return nil, fmt.Errorf("transpose: reading compression type: %w", err)
	}
	ct := chunk.CompressionType(ctByte)

	// Read header: varint64(header_length), then header_length bytes of (possibly compressed) header.
	headerSize, err := varint.ReadUvarint64(r)
	if err != nil {
		return nil, fmt.Errorf("transpose: reading header size: %w", err)
	}
	headerCompressed := make([]byte, headerSize)
	if _, err := r.Read(headerCompressed); err != nil {
		return nil, fmt.Errorf("transpose: reading header data: %w", err)
	}

	headerData, err := compression.Decompress(ct, headerCompressed)
	if err != nil {
		return nil, fmt.Errorf("transpose: decompressing header: %w", err)
	}
	hr := newByteReader(headerData)

	// Parse buckets and buffers.
	const maxCountPerChunk = 1 << 20 // 1 million — caps slice allocations from untrusted header fields
	numBuckets, err := varint.ReadUvarint32(hr)
	if err != nil {
		return nil, fmt.Errorf("transpose: reading num_buckets: %w", err)
	}
	if numBuckets > maxCountPerChunk {
		return nil, fmt.Errorf("transpose: num_buckets %d exceeds maximum %d", numBuckets, maxCountPerChunk)
	}
	numBuffers, err := varint.ReadUvarint32(hr)
	if err != nil {
		return nil, fmt.Errorf("transpose: reading num_buffers: %w", err)
	}
	if numBuffers > maxCountPerChunk {
		return nil, fmt.Errorf("transpose: num_buffers %d exceeds maximum %d", numBuffers, maxCountPerChunk)
	}

	// Read bucket lengths from header.
	bucketLengths := make([]uint64, numBuckets)
	for i := uint32(0); i < numBuckets; i++ {
		bl, err := varint.ReadUvarint64(hr)
		if err != nil {
			return nil, fmt.Errorf("transpose: reading bucket length %d: %w", i, err)
		}
		bucketLengths[i] = bl
	}

	// Read compressed bucket data from the remaining data in r (after the header).
	bucketData := make([][]byte, numBuckets)
	for i := uint32(0); i < numBuckets; i++ {
		bd := make([]byte, bucketLengths[i])
		if _, err := r.Read(bd); err != nil {
			return nil, fmt.Errorf("transpose: reading bucket %d data: %w", i, err)
		}
		bucketData[i] = bd
	}

	// Read buffer lengths from header and assign buffers to buckets.
	type bufferInfo struct {
		length    uint64
		bucketIdx uint32
	}
	bufInfos := make([]bufferInfo, numBuffers)

	// Decompress buckets to create buffer readers.
	decompressedBuckets := make([][]byte, numBuckets)
	for i := uint32(0); i < numBuckets; i++ {
		dec, err := compression.Decompress(ct, bucketData[i])
		if err != nil {
			return nil, fmt.Errorf("transpose: decompressing bucket %d: %w", i, err)
		}
		decompressedBuckets[i] = dec
	}

	// Assign buffers to buckets by reading buffer lengths.
	bucketIdx := uint32(0)
	bucketOffset := uint64(0)
	for i := uint32(0); i < numBuffers; i++ {
		bl, err := varint.ReadUvarint64(hr)
		if err != nil {
			return nil, fmt.Errorf("transpose: reading buffer length %d: %w", i, err)
		}
		bufInfos[i] = bufferInfo{length: bl, bucketIdx: bucketIdx}
		bucketOffset += bl
		// Move to next bucket if this one is exhausted.
		for bucketIdx < numBuckets && bucketOffset >= uint64(len(decompressedBuckets[bucketIdx])) {
			if bucketOffset > uint64(len(decompressedBuckets[bucketIdx])) && bucketIdx+1 < numBuckets {
				// This shouldn't happen in well-formed data.
				break
			}
			if bucketOffset == uint64(len(decompressedBuckets[bucketIdx])) {
				bucketIdx++
				bucketOffset = 0
			} else {
				break
			}
		}
	}

	// Split decompressed buckets into individual buffers.
	buffers := make([]*bytes.Reader, numBuffers)
	bucketIdx = 0
	bucketOffset = 0
	for i := uint32(0); i < numBuffers; i++ {
		bi := bufInfos[i]
		bkt := bi.bucketIdx
		start := bucketOffset
		end := start + bi.length
		if end > uint64(len(decompressedBuckets[bkt])) {
			return nil, fmt.Errorf("transpose: buffer %d exceeds bucket %d", i, bkt)
		}
		buffers[i] = bytes.NewReader(decompressedBuckets[bkt][start:end])
		bucketOffset = end
		// Check if we've exhausted this bucket.
		if bucketOffset == uint64(len(decompressedBuckets[bkt])) && bkt+1 < numBuckets {
			bucketIdx = bkt + 1
			bucketOffset = 0
		}
	}

	// Parse state machine.
	stateMachineSize, err := varint.ReadUvarint32(hr)
	if err != nil {
		return nil, fmt.Errorf("transpose: reading state_machine_size: %w", err)
	}
	if stateMachineSize > maxCountPerChunk {
		return nil, fmt.Errorf("transpose: state_machine_size %d exceeds maximum %d", stateMachineSize, maxCountPerChunk)
	}

	// Read tags array.
	tags := make([]uint32, stateMachineSize)
	for i := uint32(0); i < stateMachineSize; i++ {
		t, err := varint.ReadUvarint32(hr)
		if err != nil {
			return nil, fmt.Errorf("transpose: reading tag %d: %w", i, err)
		}
		tags[i] = t
	}

	// Read next_node (base) array.
	nextNodes := make([]uint32, stateMachineSize)
	for i := uint32(0); i < stateMachineSize; i++ {
		nn, err := varint.ReadUvarint32(hr)
		if err != nil {
			return nil, fmt.Errorf("transpose: reading next_node %d: %w", i, err)
		}
		nextNodes[i] = nn
	}

	// Count subtypes needed.
	numSubtypes := 0
	for i := uint32(0); i < stateMachineSize; i++ {
		tag := tags[i]
		if ValidTag(tag) && HasSubtype(tag) {
			numSubtypes++
		}
	}

	// Read subtypes.
	subtypeBytes := make([]byte, numSubtypes)
	if numSubtypes > 0 {
		if _, err := hr.Read(subtypeBytes); err != nil {
			return nil, fmt.Errorf("transpose: reading subtypes: %w", err)
		}
	}

	// Build state machine nodes.
	nodes := make([]stateMachineNode, stateMachineSize)
	hasNonProto := false
	subtypeIdx := 0

	for i := uint32(0); i < stateMachineSize; i++ {
		tag := tags[i]
		node := &nodes[i]
		node.messageID = tag
		node.bufferIdx = -1

		nextNodeID := nextNodes[i]
		if nextNodeID >= stateMachineSize {
			node.isImplicit = true
			nextNodeID -= stateMachineSize
		}
		if nextNodeID >= stateMachineSize {
			return nil, fmt.Errorf("transpose: next_node %d too large", nextNodeID)
		}
		node.nextNodeID = int(nextNodeID)

		switch tag {
		case MessageIDNoOp:
			// No-op, no buffer.
		case MessageIDNonProto:
			var bufIdx uint32
			bufIdx, err = varint.ReadUvarint32(hr)
			if err != nil {
				return nil, fmt.Errorf("transpose: reading non-proto buffer index: %w", err)
			}
			if bufIdx >= numBuffers {
				return nil, fmt.Errorf("transpose: non-proto buffer index %d >= %d", bufIdx, numBuffers)
			}
			node.bufferIdx = int(bufIdx)
			hasNonProto = true
		case MessageIDStartOfSubmessage:
			// No buffer.
		case MessageIDStartOfMessage:
			// No buffer.
		default:
			// Regular proto tag.
			actualTag := tag
			subtype := uint8(0)

			// Check for submessage end marker (wire type 6).
			if GetTagWireType(actualTag) == WireSubmessageEnd {
				actualTag = actualTag - WireSubmessageEnd + WireLengthDelimited
				subtype = SubtypeLengthDelimitedEndOfSubmessage
			}

			if !ValidTag(actualTag) {
				return nil, fmt.Errorf("transpose: invalid tag %d at state %d", actualTag, i)
			}

			// Encode tag as varint.
			var tagBuf [varint.MaxLenVarint32 + 1]byte
			tagLen := varint.PutUvarint32(tagBuf[:varint.MaxLenVarint32], actualTag)

			if HasSubtype(actualTag) {
				subtype = subtypeBytes[subtypeIdx]
				subtypeIdx++
			}

			// Store inline value for varint inline subtypes.
			if GetTagWireType(actualTag) == WireVarint && subtype >= SubtypeVarintInline0 {
				tagBuf[tagLen] = subtype - SubtypeVarintInline0
			} else {
				tagBuf[tagLen] = 0
			}

			node.tag = actualTag
			node.tagData = make([]byte, tagLen+1)
			copy(node.tagData, tagBuf[:tagLen+1])
			node.subtype = subtype

			if HasDataBuffer(actualTag, subtype) {
				var bufIdx uint32
				bufIdx, err = varint.ReadUvarint32(hr)
				if err != nil {
					return nil, fmt.Errorf("transpose: reading buffer index for state %d: %w", i, err)
				}
				if bufIdx >= numBuffers {
					return nil, fmt.Errorf("transpose: buffer index %d >= %d at state %d", bufIdx, numBuffers, i)
				}
				node.bufferIdx = int(bufIdx)
			}
		}
	}

	// Read first_node.
	firstNode, err := varint.ReadUvarint32(hr)
	if err != nil {
		return nil, fmt.Errorf("transpose: reading first_node: %w", err)
	}
	if firstNode >= stateMachineSize {
		return nil, fmt.Errorf("transpose: first_node %d >= state_machine_size %d", firstNode, stateMachineSize)
	}

	// Non-proto lengths buffer.
	var nonProtoLengthsBuf *bytes.Reader
	if hasNonProto {
		if numBuffers == 0 {
			return nil, fmt.Errorf("transpose: has non-proto but no buffers")
		}
		nonProtoLengthsBuf = buffers[numBuffers-1]
	}

	// Read transitions (remaining data in r, possibly compressed).
	transitionsCompressed := r.Remaining()
	transitionsData, err := compression.Decompress(ct, transitionsCompressed)
	if err != nil {
		return nil, fmt.Errorf("transpose: decompressing transitions: %w", err)
	}
	transReader := newByteReader(transitionsData)

	// Run the state machine to reconstruct records.
	return runStateMachine(nodes, int(firstNode), int(stateMachineSize), buffers, nonProtoLengthsBuf, transReader, numRecords, decodedDataSize)
}

// runStateMachine executes the transpose state machine to reconstruct records.
// The C++ implementation writes records backwards into a BackwardWriter, then
// reverses limits. We build records forward using a stack of bytes.Buffer.
func runStateMachine(
	nodes []stateMachineNode,
	firstNode int,
	smSize int,
	buffers []*bytes.Reader,
	nonProtoLengths *bytes.Reader,
	transitions *byteReader,
	numRecords, decodedDataSize uint64,
) ([][]byte, error) {

	// The decode output. We write bytes backward as the C++ impl does,
	// using a byte slice we fill from the end.
	dest := make([]byte, decodedDataSize)
	destPos := decodedDataSize // write position (goes backward)

	// limits tracks end positions of records (from the end).
	limits := make([]uint64, 0, numRecords)

	// Submessage stack: tracks (position, node) for submessage nesting.
	type submessageEntry struct {
		endPos uint64 // destPos at the time submessage end was seen
		node   *stateMachineNode
	}
	var submessageStack []submessageEntry

	nodeIdx := firstNode
	node := &nodes[nodeIdx]
	numIters := int8(0)
	if node.isImplicit {
		numIters = 1
	}

	for {
		// Handle the current node.
		switch node.messageID {
		case MessageIDNoOp:
			// Nothing.

		case MessageIDStartOfMessage:
			if len(submessageStack) != 0 {
				return nil, fmt.Errorf("transpose: submessages still open at message start")
			}
			if uint64(len(limits)) >= numRecords {
				return nil, fmt.Errorf("transpose: too many records")
			}
			// Record the current write position as a record boundary.
			limits = append(limits, destPos)

		case MessageIDStartOfSubmessage:
			if len(submessageStack) == 0 {
				return nil, fmt.Errorf("transpose: submessage stack underflow")
			}
			entry := submessageStack[len(submessageStack)-1]
			submessageStack = submessageStack[:len(submessageStack)-1]

			// The submessage length is the data written between now and when
			// the end-of-submessage was pushed.
			length := entry.endPos - destPos
			if length > 0xFFFFFFFF {
				return nil, fmt.Errorf("transpose: submessage too large")
			}

			// Write the tag + length prefix before the submessage data.
			tagData := entry.node.tagData
			tagLen := len(tagData) - 1 // exclude the inline value byte
			var lenBuf [varint.MaxLenVarint32]byte
			lenN := varint.PutUvarint32(lenBuf[:], uint32(length))

			headerLen := uint64(tagLen + lenN)
			if destPos < headerLen {
				return nil, fmt.Errorf("transpose: not enough space for submessage header")
			}
			destPos -= headerLen
			copy(dest[destPos:], tagData[:tagLen])
			copy(dest[destPos+uint64(tagLen):], lenBuf[:lenN])

		case MessageIDNonProto:
			if nonProtoLengths == nil {
				return nil, fmt.Errorf("transpose: non-proto record but no lengths buffer")
			}
			// Read length from nonproto_lengths buffer.
			length, err := varint.ReadUvarint32(nonProtoLengths)
			if err != nil {
				return nil, fmt.Errorf("transpose: reading non-proto length: %w", err)
			}
			if node.bufferIdx < 0 {
				return nil, fmt.Errorf("transpose: non-proto node has no buffer")
			}
			buf := buffers[node.bufferIdx]
			data := make([]byte, length)
			if _, err := buf.Read(data); err != nil {
				return nil, fmt.Errorf("transpose: reading non-proto data: %w", err)
			}
			if destPos < uint64(length) {
				return nil, fmt.Errorf("transpose: not enough space for non-proto data")
			}
			destPos -= uint64(length)
			copy(dest[destPos:], data)

			// Non-proto record acts as a message start too.
			if len(submessageStack) != 0 {
				return nil, fmt.Errorf("transpose: submessages still open at non-proto")
			}
			if uint64(len(limits)) >= numRecords {
				return nil, fmt.Errorf("transpose: too many records")
			}
			limits = append(limits, destPos)

		default:
			// Regular proto field.
			tag := node.tag
			wt := GetTagWireType(tag)
			tagData := node.tagData
			tagLen := len(tagData) - 1 // length of varint-encoded tag

			switch {
			case wt == WireLengthDelimited && node.subtype == SubtypeLengthDelimitedEndOfSubmessage:
				// End of submessage: push onto stack.
				submessageStack = append(submessageStack, submessageEntry{
					endPos: destPos,
					node:   node,
				})

			case wt == WireLengthDelimited && node.subtype == SubtypeLengthDelimitedString:
				// String/bytes field: read length-prefixed data from buffer.
				if node.bufferIdx < 0 {
					return nil, fmt.Errorf("transpose: string node has no buffer")
				}
				buf := buffers[node.bufferIdx]
				// Read length varint from buffer.
				strLen, err := varint.ReadUvarint32(buf)
				if err != nil {
					return nil, fmt.Errorf("transpose: reading string length: %w", err)
				}
				strData := make([]byte, strLen)
				if strLen > 0 {
					if _, err := buf.Read(strData); err != nil {
						return nil, fmt.Errorf("transpose: reading string data: %w", err)
					}
				}
				// Write: length_varint + string_data + tag
				var lenBuf [varint.MaxLenVarint32]byte
				lenN := varint.PutUvarint32(lenBuf[:], strLen)
				totalLen := uint64(tagLen + lenN + int(strLen))
				if destPos < totalLen {
					return nil, fmt.Errorf("transpose: not enough space for string field")
				}
				destPos -= totalLen
				copy(dest[destPos:], tagData[:tagLen])
				copy(dest[destPos+uint64(tagLen):], lenBuf[:lenN])
				copy(dest[destPos+uint64(tagLen)+uint64(lenN):], strData)

			case wt == WireVarint && node.subtype >= SubtypeVarintInline0:
				// Inline varint: the value is encoded in the tag_data.
				// tag_data is: varint_tag_bytes + inline_value_byte
				// We write tag + single-byte varint (value = subtype - SubtypeVarintInline0)
				totalLen := uint64(tagLen + 1)
				if destPos < totalLen {
					return nil, fmt.Errorf("transpose: not enough space for inline varint")
				}
				destPos -= totalLen
				copy(dest[destPos:], tagData[:tagLen+1])

			case wt == WireVarint:
				// Buffered varint: subtype indicates the varint byte length (1 to 10).
				varintLen := int(node.subtype - SubtypeVarint1 + 1)
				if node.bufferIdx < 0 {
					return nil, fmt.Errorf("transpose: varint node has no buffer")
				}
				buf := buffers[node.bufferIdx]
				rawBytes := make([]byte, varintLen)
				if _, err := buf.Read(rawBytes); err != nil {
					return nil, fmt.Errorf("transpose: reading varint data: %w", err)
				}
				// Add continuation bits: all bytes except last get MSB set.
				for j := 0; j < varintLen-1; j++ {
					rawBytes[j] |= 0x80
				}
				totalLen := uint64(tagLen + varintLen)
				if destPos < totalLen {
					return nil, fmt.Errorf("transpose: not enough space for varint field")
				}
				destPos -= totalLen
				copy(dest[destPos:], tagData[:tagLen])
				copy(dest[destPos+uint64(tagLen):], rawBytes)

			case wt == WireFixed32:
				if node.bufferIdx < 0 {
					return nil, fmt.Errorf("transpose: fixed32 node has no buffer")
				}
				buf := buffers[node.bufferIdx]
				fixedData := make([]byte, 4)
				if _, err := buf.Read(fixedData); err != nil {
					return nil, fmt.Errorf("transpose: reading fixed32 data: %w", err)
				}
				totalLen := uint64(tagLen + 4)
				if destPos < totalLen {
					return nil, fmt.Errorf("transpose: not enough space for fixed32")
				}
				destPos -= totalLen
				copy(dest[destPos:], tagData[:tagLen])
				copy(dest[destPos+uint64(tagLen):], fixedData)

			case wt == WireFixed64:
				if node.bufferIdx < 0 {
					return nil, fmt.Errorf("transpose: fixed64 node has no buffer")
				}
				buf := buffers[node.bufferIdx]
				fixedData := make([]byte, 8)
				if _, err := buf.Read(fixedData); err != nil {
					return nil, fmt.Errorf("transpose: reading fixed64 data: %w", err)
				}
				totalLen := uint64(tagLen + 8)
				if destPos < totalLen {
					return nil, fmt.Errorf("transpose: not enough space for fixed64")
				}
				destPos -= totalLen
				copy(dest[destPos:], tagData[:tagLen])
				copy(dest[destPos+uint64(tagLen):], fixedData)

			case wt == WireStartGroup:
				totalLen := uint64(tagLen)
				if destPos < totalLen {
					return nil, fmt.Errorf("transpose: not enough space for start group")
				}
				destPos -= totalLen
				copy(dest[destPos:], tagData[:tagLen])

			case wt == WireEndGroup:
				totalLen := uint64(tagLen)
				if destPos < totalLen {
					return nil, fmt.Errorf("transpose: not enough space for end group")
				}
				destPos -= totalLen
				copy(dest[destPos:], tagData[:tagLen])

			default:
				return nil, fmt.Errorf("transpose: unhandled wire type %d at tag %d", wt, tag)
			}
		}

		// Transition to next node.
		nodeIdx = node.nextNodeID
		node = &nodes[nodeIdx]

		if numIters == 0 {
			// Read transition byte.
			tb, err := transitions.ReadByte()
			if err != nil {
				// End of transitions = end of decoding.
				break
			}
			offset := int(tb >> 2)
			numIters = int8(tb&3)
			nodeIdx += offset
			if nodeIdx >= smSize {
				return nil, fmt.Errorf("transpose: node index %d out of range", nodeIdx)
			}
			node = &nodes[nodeIdx]
			if node.isImplicit {
				numIters++
			}
		} else {
			if !node.isImplicit {
				numIters--
			}
		}
	}

	// Verify.
	if len(submessageStack) != 0 {
		return nil, fmt.Errorf("transpose: submessages still open at end")
	}
	if uint64(len(limits)) != numRecords {
		return nil, fmt.Errorf("transpose: got %d records, expected %d", len(limits), numRecords)
	}
	if destPos != 0 {
		return nil, fmt.Errorf("transpose: %d bytes remaining in dest", destPos)
	}

	// Reverse limits and compute record boundaries.
	// limits contains write positions in reverse order.
	// They need to be reversed and complemented.
	// limits[i] is the destPos at the start of record i (written backward).
	// Records were written from the end, so the last limit is 0 and the first is decodedDataSize.
	// We need to reverse and compute proper offsets.
	totalSize := decodedDataSize
	for i, j := 0, len(limits)-1; i < j; i, j = i+1, j-1 {
		limits[i], limits[j] = limits[j], limits[i]
	}

	// Convert from backward positions to forward record limits.
	// limits[i] is the backward position where record i starts.
	// In forward order: record i starts at limits[i] and ends at limits[i+1] (or totalSize).
	records := make([][]byte, numRecords)
	for i := uint64(0); i < numRecords; i++ {
		start := limits[i]
		var end uint64
		if i+1 < numRecords {
			end = limits[i+1]
		} else {
			end = totalSize
		}
		records[i] = dest[start:end]
	}

	return records, nil
}

// byteReader wraps a byte slice for sequential reading with ReadByte support.
type byteReader struct {
	data []byte
	pos  int
}

func newByteReader(data []byte) *byteReader {
	return &byteReader{data: data}
}

func (r *byteReader) ReadByte() (byte, error) {
	if r.pos >= len(r.data) {
		return 0, fmt.Errorf("unexpected end of data")
	}
	b := r.data[r.pos]
	r.pos++
	return b, nil
}

func (r *byteReader) Read(p []byte) (int, error) {
	if r.pos >= len(r.data) {
		return 0, fmt.Errorf("unexpected end of data")
	}
	n := copy(p, r.data[r.pos:])
	r.pos += n
	if n < len(p) {
		return n, fmt.Errorf("short read: got %d, want %d", n, len(p))
	}
	return n, nil
}

func (r *byteReader) Remaining() []byte {
	if r.pos >= len(r.data) {
		return nil
	}
	return r.data[r.pos:]
}
