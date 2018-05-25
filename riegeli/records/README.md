# Summary

Riegeli/records is a file format for storing a sequence of records.

The format supports sequential writing, appending to a previously created file,
sequential reading, and seeking while reading. Data are optionally compressed,
with special support for the case when records are proto messages. Data
corruption is detected and a reading can be resumed after skipping over a
corrupted region.
