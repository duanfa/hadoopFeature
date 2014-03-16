package org.apache.hadoop.base.samper;

import java.net.URI;

import org.apache.hadoop.base.combineFile.JobBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.lib.InputSampler;
import org.apache.hadoop.mapred.lib.TotalOrderPartitioner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class SortByTemperatureUsingTotalOrderPartitioner extends Configured
		implements Tool {
	@Override
	public int run(String[] args) throws Exception {
		JobConf jobconf = JobBuilder.parseInputAndOutput(this, getConf(), args);
		if (jobconf == null) {
			return -1;
		}
		jobconf.setInputFormat(SequenceFileInputFormat.class);
		jobconf.setOutputKeyClass(IntWritable.class);
		jobconf.setOutputFormat(SequenceFileOutputFormat.class);
		SequenceFileOutputFormat.setCompressOutput(jobconf, true);
		SequenceFileOutputFormat.setOutputCompressorClass(jobconf, GzipCodec.class);
		SequenceFileOutputFormat.setOutputCompressionType(jobconf,
				CompressionType.BLOCK);
		jobconf.setPartitionerClass(TotalOrderPartitioner.class);
		InputSampler.Sampler<IntWritable, Text> sampler = new InputSampler.RandomSampler<IntWritable, Text>(
				0.1, 10000, 10);
		InputSampler.writePartitionFile(jobconf, sampler);
		// Add to DistributedCache
		Configuration conf = getConf();
		String partitionFile = TotalOrderPartitioner.getPartitionFile(conf);
		URI partitionUri = new URI(partitionFile + "#"
				+ TotalOrderPartitioner.DEFAULT_PATH);
		DistributedCache.addCacheFile(partitionUri, conf);
		DistributedCache.createSymlink(conf);
		JobClient.runJob(jobconf);
		return 0;
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(
				new SortByTemperatureUsingTotalOrderPartitioner(), args);
		System.exit(exitCode);
	}
}