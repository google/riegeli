# Purpose

Riegeli/transpose transforms protocol buffer byte streams into a custom
data format that can be compressed around 20% more densely. The additional
transform slows down both compression and decompression by around 50%. Often
this is still a desirable trade-off, but in the end it depends on the
compressed data and other system requirements such as latency vs. resource use.

# Detailed design

Transposition of a set of protocol buffers means that we associate a container
with each tag. Then all the values corresponding to a specific tag are stored in
the container associated with it. Invocation of a general purpose compression
algorithm on the concatenation of these containers offers better compression
ratios than it's invocation on the concatenation of the original binary encoding
of the protocol buffers.

