package edu.rutgers.ess.crs.utility;

/**
Copyright 2005 Bytecode Pty Ltd.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.input.KeyValueLineRecordReader;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.JobContext;
import java.io.IOException;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

public class KeyValueCSVInputFormat extends FileInputFormat<Text, TextArrayWritable>
{
    public static String CSV_TOKEN_SEPARATOR_CONFIG;
    public static String CSV_KEYVALUE_SEPARATOR_CONFIG;
    
    public RecordReader<Text, TextArrayWritable> createRecordReader(final InputSplit split, final TaskAttemptContext context) throws IOException {
        final String tokenDelimiter = context.getConfiguration().get(KeyValueCSVInputFormat.CSV_TOKEN_SEPARATOR_CONFIG);
        final String keyValueDelimiter = context.getConfiguration().get(KeyValueCSVInputFormat.CSV_KEYVALUE_SEPARATOR_CONFIG);
        if (tokenDelimiter.equals(keyValueDelimiter)) {
            throw new IllegalArgumentException("CSV_TOKEN_SEPARATOR_CONFIG is the same as CSV_KEYVALUE_SEPARATOR_CONFIG");
        }
        return new KeyValueCSVRecordReader(tokenDelimiter, context.getConfiguration());
    }
    
    protected boolean isSplitable(final JobContext context, final Path file) {
        final CompressionCodec codec = new CompressionCodecFactory(context.getConfiguration()).getCodec(file);
        return codec == null;
    }
    
    static {
        KeyValueCSVInputFormat.CSV_TOKEN_SEPARATOR_CONFIG = "csvinputformat.token.delimiter";
        KeyValueCSVInputFormat.CSV_KEYVALUE_SEPARATOR_CONFIG = "mapreduce.input.keyvaluelinerecordreader.key.value.separator";
    }
    
    public static class KeyValueCSVRecordReader extends RecordReader<Text, TextArrayWritable>
    {
        private KeyValueLineRecordReader reader;
        private TextArrayWritable value;
        private String tokenDelimiter;
        
        public KeyValueCSVRecordReader(final String tokenDelimiter, final Configuration conf) throws IOException {
            super();
            this.reader = new KeyValueLineRecordReader(conf);
            this.tokenDelimiter = tokenDelimiter;
        }
        
        public void initialize(final InputSplit split, final TaskAttemptContext context) throws IOException, InterruptedException {
            this.reader.initialize(split, context);
        }
        
        public boolean nextKeyValue() throws IOException, InterruptedException {
            if (this.reader.nextKeyValue()) {
                this.loadCSV();
                return true;
            }
            this.value = null;
            return false;
        }
        
        private void loadCSV() throws IOException {
            final String line = this.reader.getCurrentValue().toString();
            final String[] tokens = line.split(this.tokenDelimiter);
            this.value = new TextArrayWritable(this.convert(tokens));
        }
        
        private Text[] convert(final String[] s) {
            final Text[] t = new Text[s.length];
            for (int i = 0; i < t.length; ++i) {
                t[i] = new Text(s[i]);
            }
            return t;
        }
        
        public Text getCurrentKey() throws IOException, InterruptedException {
            return this.reader.getCurrentKey();
        }
        
        public TextArrayWritable getCurrentValue() throws IOException, InterruptedException {
            return this.value;
        }
        
        public float getProgress() throws IOException, InterruptedException {
            return this.reader.getProgress();
        }
        
        public void close() throws IOException {
            this.reader.close();
        }
    }
}
