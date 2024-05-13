package com.google.riegeli;

import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

public class RecordReadWriteTest {

    @Before
    public void setUp() {
    }

    private String createTestString(int length) {
        // String.repeat is only available from Java 11, so create a
        // helper method instead.
        return String.join("", java.util.Collections.nCopies(length, "a"));
    }

    @Test
    public void writeWriteString() throws IOException {
        // TODO: create a random file on TEST_TEMP directory.
        final String filename = "/tmp/test.rg";
        RecordWriter writer = Loader.newWriter();
        writer.open(filename, "default");
        final int kNumRecords = 4096;
        for (int i = 0; i < kNumRecords; i++) {
            final String s = createTestString(i+1);
            writer.writeRecord(s);
        }
        writer.close();

        RecordReader reader = Loader.newReader();
        reader.open(filename);
        for (int i = 0; i < kNumRecords; i++) {
            byte[] record = reader.readRecord();
            final String s = createTestString(i+1);
            assertEquals(new String(record), s);
        }
        byte[] record = reader.readRecord();
        assertEquals(null, record);
        reader.close();
    }
}
