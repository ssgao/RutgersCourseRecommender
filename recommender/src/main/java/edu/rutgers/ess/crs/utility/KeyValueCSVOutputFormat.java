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


import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.ReflectionUtils;

public class KeyValueCSVOutputFormat extends TextOutputFormat<Text, TextArrayWritable> {
	public static String CSV_TOKEN_SEPARATOR_CONFIG;
	public static String CSV_KEYVALUE_SEPARATOR_CONFIG;

	public RecordWriter<Text, TextArrayWritable> getRecordWriter(final TaskAttemptContext context) throws IOException,
			InterruptedException {
		final Configuration conf = context.getConfiguration();
		final boolean isCompressed = getCompressOutput((JobContext) context);
		final String tokenSeparator = conf.get(KeyValueCSVOutputFormat.CSV_TOKEN_SEPARATOR_CONFIG);
		final String keyValueSeparator = conf.get(KeyValueCSVOutputFormat.CSV_KEYVALUE_SEPARATOR_CONFIG);
		CompressionCodec codec = null;
		String extension = "";
		if (isCompressed) {
			final Class<? extends CompressionCodec> codecClass = (Class<? extends CompressionCodec>) getOutputCompressorClass(
					(JobContext) context, GzipCodec.class);
			codec = ReflectionUtils.newInstance(codecClass, conf);
			extension = codec.getDefaultExtension();
		}
		final Path file = this.getDefaultWorkFile(context, extension);
		final FileSystem fs = file.getFileSystem(conf);
		if (!isCompressed) {
			final FSDataOutputStream fileOut = fs.create(file, false);
			return new KeyValueCSVRecordWriter((DataOutputStream) fileOut, tokenSeparator, keyValueSeparator);
		}
		final FSDataOutputStream fileOut = fs.create(file, false);
		return new KeyValueCSVRecordWriter(new DataOutputStream(
				(OutputStream) codec.createOutputStream((OutputStream) fileOut)), tokenSeparator, keyValueSeparator);
	}

	static {
		KeyValueCSVOutputFormat.CSV_TOKEN_SEPARATOR_CONFIG = "csvoutputformat.token.delimiter";
		KeyValueCSVOutputFormat.CSV_KEYVALUE_SEPARATOR_CONFIG = "mapreduce.input.keyvaluelinerecordreader.key.value.separator";
	}

	protected static class KeyValueCSVRecordWriter extends RecordWriter<Text, TextArrayWritable> {
		private static final String utf8 = "UTF-8";
		private static final byte[] newline;

		static {
			try {
				newline = "\n".getBytes(utf8);
			} catch (UnsupportedEncodingException uee) {
				throw new IllegalArgumentException("can't find " + utf8 + " encoding");
			}
		}

		protected DataOutputStream out;
		private final String tokenSeparator;
		private final String keyValueSeparator;

		public KeyValueCSVRecordWriter(final DataOutputStream out, final String csvSeparator,
				final String keyValueSeparator) {
			super();
			this.out = out;
			this.tokenSeparator = csvSeparator;
			this.keyValueSeparator = keyValueSeparator;
		}

		public void write(final Text key, final TextArrayWritable value) throws IOException, InterruptedException {
			if (key == null | value == null) {
				return;
			}
			this.out.write(key.getBytes(), 0, key.getLength());
			boolean first = true;
			for (final Writable field : value.get()) {
				this.writeObject(field, first);
				first = false;
			}
			this.out.write(KeyValueCSVRecordWriter.newline);
		}

		private void writeObject(final Writable o, final boolean first) throws IOException {
			if (first) {
				this.out.write(this.keyValueSeparator.getBytes(utf8));
			} else {
				this.out.write(this.tokenSeparator.getBytes(utf8));
			}
			boolean encloseQuotes = false;
			if (o.toString().contains(this.tokenSeparator)) {
				encloseQuotes = true;
			}
			if (encloseQuotes) {
				this.out.write("\"".getBytes(utf8));
			}
			if (o instanceof Text) {
				final Text to = (Text) o;
				this.out.write(to.getBytes(), 0, to.getLength());
			} else {
				this.out.write(o.toString().getBytes(utf8));
			}
			if (encloseQuotes) {
				this.out.write("\"".getBytes(utf8));
			}
		}

		public synchronized void close(final TaskAttemptContext context) throws IOException {
			this.out.close();
		}
	}
}
