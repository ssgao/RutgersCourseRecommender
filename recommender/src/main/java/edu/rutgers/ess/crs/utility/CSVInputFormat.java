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


import org.apache.hadoop.io.Text;
import java.io.IOException;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

public class CSVInputFormat extends FileInputFormat<LongWritable, TextArrayWritable>
{
    public static String CSV_TOKEN_SEPARATOR_CONFIG;
    
    public RecordReader<LongWritable, TextArrayWritable> createRecordReader(final InputSplit split, final TaskAttemptContext context) {
        final String csvDelimiter = context.getConfiguration().get(CSVInputFormat.CSV_TOKEN_SEPARATOR_CONFIG);
        Character separator = null;
        if (csvDelimiter != null && csvDelimiter.length() == 1) {
            separator = csvDelimiter.charAt(0);
        }
        return new CSVRecordReader(separator);
    }
    
    protected boolean isSplitable(final JobContext context, final Path file) {
        final CompressionCodec codec = new CompressionCodecFactory(context.getConfiguration()).getCodec(file);
        return codec == null;
    }
    
    static {
        CSVInputFormat.CSV_TOKEN_SEPARATOR_CONFIG = "csvinputformat.token.delimiter";
    }
    
    public static class CSVRecordReader extends RecordReader<LongWritable, TextArrayWritable>
    {
        private LineRecordReader reader;
        private TextArrayWritable value;
        private final CSVParser parser;
        
        public CSVRecordReader(final Character csvDelimiter) {
            super();
            this.reader = new LineRecordReader();
            if (csvDelimiter == null) {
                this.parser = new CSVParser();
            }
            else {
                this.parser = new CSVParser(csvDelimiter);
            }
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
            final String[] tokens = this.parser.parseLine(line);
            this.value = new TextArrayWritable(this.convert(tokens));
        }
        
        private Text[] convert(final String[] s) {
            final Text[] t = new Text[s.length];
            for (int i = 0; i < t.length; ++i) {
                t[i] = new Text(s[i]);
            }
            return t;
        }
        
        public LongWritable getCurrentKey() throws IOException, InterruptedException {
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
