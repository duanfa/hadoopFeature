package org.apache.hadoop.base.sequencefile;


import java.io.IOException;
import java.net.URI;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.join.TupleWritable;


public class SequenceTupleWriter {
	public static void main(String[] args) throws IOException {
		String uri = args[1];
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(uri), conf);
		Path path = new Path(uri);
		IntWritable key = new IntWritable();
		SequenceFile.Writer writer = null;
		Random r = new Random();
		int size = Integer.parseInt(args[0]);
		try {
			writer = SequenceFile.createWriter(fs, conf, path, key.getClass(), TupleWritable.class);
			for (int i = 0; i < size; i++) {
				key.set(i);
				Text t1=new Text("text"+i);
				Text t2=new Text("random:"+r.nextInt(size));
				Writable[] values = {key,t1,t2};
				TupleWritable value = new TupleWritable(values);
				value.get(0);
				writer.append(key, value);
			}
		} finally {
			IOUtils.closeStream(writer);
		}
	}
	
}
