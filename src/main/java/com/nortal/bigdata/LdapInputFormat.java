package com.nortal.bigdata;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapred.*;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;

public class LdapInputFormat extends TextInputFormat {

    @Override
    public RecordReader<LongWritable, Text> getRecordReader(InputSplit inputSplit, JobConf jobConf, Reporter reporter) throws IOException {
        reporter.setStatus(inputSplit.toString());
        return new LdapRecordReader((FileSplit) inputSplit, jobConf);
    }

    public static class LdapRecordReader implements RecordReader<LongWritable, Text> {
        private final byte[] startTag;
        private final byte[] endTag;
        private final long start;
        private final long end;
        private final FSDataInputStream fsin;
        private final DataOutputBuffer buffer = new DataOutputBuffer();

        private InputStream dcin;
        private DataInputStream in;

        private long pos;
        private long recordStartPos;


         public LdapRecordReader(FileSplit split, JobConf jobConf) throws IOException {
            startTag = "dn".getBytes("utf-8");
            endTag = "\n\n".getBytes("utf-8");

            start = split.getStart();
            Path file = split.getPath();
            FileSystem fs = file.getFileSystem(jobConf);
            fsin = fs.open(split.getPath());
            Path path = split.getPath();
            CompressionCodecFactory compressionCodecs = new CompressionCodecFactory(jobConf);
            final CompressionCodec codec = compressionCodecs.getCodec(path);
            if (codec != null) {
                dcin = codec.createInputStream(fsin);
                in = new DataInputStream(dcin);
                end = Long.MAX_VALUE;
            } else {
                dcin = null;
                fsin.seek(start);
                in = fsin;
                end = start + split.getLength();
            }
            recordStartPos = start;
            pos = start;

        }


        public boolean next(LongWritable key, Text value) throws IOException {

            if (pos < end) {
                if (readUntilMatch(startTag, false)) {
                    recordStartPos = pos - startTag.length;
                    try {
                        buffer.write(startTag);
                        if (readUntilMatch(endTag, true)) {
                            key.set(recordStartPos);
                            value.set(buffer.getData(), 0, buffer.getLength());
                            return true;
                        }
                    } finally {
                        if (in instanceof Seekable) {
                            if (pos != ((Seekable) in).getPos()) {
                                throw new RuntimeException("bytes consumed error!");
                            }
                        }

                        buffer.reset();
                    }
                }
            }
            return false;
        }

        public LongWritable createKey() {
            return new LongWritable();
        }

        public Text createValue() {
            return new Text();
        }

        public long getPos() throws IOException {
            return pos;
        }

        public void close() throws IOException {
            in.close();
        }

        public float getProgress() throws IOException {
            return (pos - start) / (float) (end - start);
        }

        private boolean readUntilMatch(byte[] match, boolean withinBlock) throws IOException {
            int i = 0;
            while (true) {
                int b = in.read();
                pos++;

                // end of file:
                if (b == -1) {
                    return false;
                }
                // save to buffer:
                if (withinBlock) {
                    buffer.write(b);
                }

                // check if we're matching:
                if (b == match[i]) {
                    i++;
                    if (i >= match.length) {
                        return true;
                    }
                } else {
                    i = 0;
                }
                // see if we've passed the stop point:
                if (!withinBlock && i == 0 && pos >= end) {
                    return false;
                }
            }
        }

    }
}