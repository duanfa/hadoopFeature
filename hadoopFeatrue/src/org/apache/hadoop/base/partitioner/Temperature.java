package org.apache.hadoop.base.partitioner;

import java.io.BufferedWriter;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Temperature {
	// 自己定义的key类应该实现WritableComparable接口
	public static class IntPair implements WritableComparable<IntPair> {
		int first;
		int second;

		/**
		 * Set the left and right values.
		 */
		public void set(int left, int right) {
			first = left;
			second = right;
		}

		public int getFirst() {
			return first;
		}

		public int getSecond() {
			return second;
		}

		@Override
		// 反序列化
		public void readFields(DataInput in) throws IOException {
			first = in.readInt();
			second = in.readInt();
		}

		@Override
		// 序列化
		public void write(DataOutput out) throws IOException {
			out.writeInt(first);
			out.writeInt(second);
		}

		@Override
		// key的比较
		public int compareTo(IntPair o) {
			if (first != o.first) {
				return first < o.first ? -1 : 1;
			} else if (second != o.second) {
				return second < o.second ? -1 : 1;
			} else {
				return 0;
			}
		}

		// 新定义类应该重写的两个方法
		@Override
		public int hashCode() {
			return first * 157 + second;
		}

		@Override
		public boolean equals(Object right) {
			if (right == null)
				return false;
			if (this == right)
				return true;
			if (right instanceof IntPair) {
				IntPair r = (IntPair) right;
				return r.first == first && r.second == second;
			} else {
				return false;
			}
		}
	}

	/**
	 * 分区函数类。根据first确定Partition。
	 */
	public static class FirstPartitioner extends Partitioner<IntPair, NullWritable> {
		@Override
		public int getPartition(IntPair key, NullWritable value, int numPartitions) {
			int partition = Math.abs(key.getFirst() * 127) % numPartitions;
			return partition;
		}
	}

	/**
	 * key比较函数类。first升序，second降序。
	 */
	public static class KeyComparator extends WritableComparator {
		protected KeyComparator() {
			super(IntPair.class, true);
		}

		@Override
		public int compare(WritableComparable w1, WritableComparable w2) {
			IntPair ip1 = (IntPair) w1;
			IntPair ip2 = (IntPair) w2;
			int l = ip1.getFirst();
			int r = ip2.getFirst();
			int cmp = (l == r ? 0 : (l < r ? -1 : 1));
			if (cmp != 0) {
				return cmp;
			}
			l = ip1.getSecond();
			r = ip2.getSecond();
			System.out.println("----KeyComparator---compare:"+l+":"+r+"----------");
			return l == r ? 0 : (l < r ? 1 : -1); // reverse
		}
	}

	/**
	 * 分组函数类。属于同一个组的value会放到同一个迭代器中，而比较是否是同一组需要使用GroupingComparator比较器。
	 */
	// 第二种方法，继承WritableComparator
	public static class GroupingComparator extends WritableComparator {
		protected GroupingComparator() {
			super(IntPair.class, true);
		}

		@Override
		// Compare two WritableComparables.
		public int compare(WritableComparable w1, WritableComparable w2) {
			IntPair ip1 = (IntPair) w1;
			IntPair ip2 = (IntPair) w2;
			int l = ip1.getFirst();
			int r = ip2.getFirst();
			System.out.println("----GroupingComparator---compare:"+l+":"+r+"----------");
			return l == r ? 0 : (l < r ? -1 : 1);
		}
	}

	// 自定义map
	public static class Map extends Mapper<LongWritable, Text, IntPair, NullWritable> {
		private final IntPair intkey = new IntPair();

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(line);
			int left = 0;
			int right = 0;
			if (tokenizer.hasMoreTokens()) {
				left = Integer.parseInt(tokenizer.nextToken());
				if (tokenizer.hasMoreTokens())
					right = Integer.parseInt(tokenizer.nextToken());
				intkey.set(left, right);
				context.write(intkey, NullWritable.get());
			}
		}
	}

	// 自定义reduce
	//
	public static class Reduce extends Reducer<IntPair, NullWritable, IntWritable, IntWritable> {
		private final IntWritable left = new IntWritable();
		private final IntWritable right = new IntWritable();

		public void reduce(IntPair key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
			System.out.println("******************************************");
			left.set(key.getFirst());
			right.set(key.getSecond());
			System.out.println("---Reduce-:"+left+","+right+"------------");
			context.write(left, right);
		}
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		// 读取hadoop配置
		Configuration conf = new Configuration();
		// 实例化一道作业
		Job job = new Job(conf, "temperature");
		job.setJarByClass(Temperature.class);
		// Mapper类型
		job.setMapperClass(Map.class);
		// 不再需要Combiner类型，因为Combiner的输出类型<Text,
		// IntWritable>对Reduce的输入类型<IntPair, IntWritable>不适用
		// job.setCombinerClass(Reduce.class);
		// Reducer类型
		job.setReducerClass(Reduce.class);
		// 分区函数
		job.setPartitionerClass(FirstPartitioner.class);
		// key比较函数
		job.setSortComparatorClass(KeyComparator.class);
		// 分组函数
		job.setGroupingComparatorClass(GroupingComparator.class);
		// map 输出Key的类型
		job.setMapOutputKeyClass(IntPair.class);
		// map输出Value的类型
		job.setMapOutputValueClass(NullWritable.class);
		// rduce输出Key的类型，是Text，因为使用的OutputFormatClass是TextOutputFormat
		job.setOutputKeyClass(IntWritable.class);
		// rduce输出Value的类型
		job.setOutputValueClass(IntWritable.class);
		// 将输入的数据集分割成小数据块splites，同时提供一个RecordReder的实现。
		job.setInputFormatClass(TextInputFormat.class);
		// 提供一个RecordWriter的实现，负责数据输出。
		job.setOutputFormatClass(TextOutputFormat.class);
		// 输入hdfs路径
		FileInputFormat.setInputPaths(job, new Path("hdfs://server128:9000/intpair"));
		// 输出hdfs路径
		FileOutputFormat.setOutputPath(job, new Path("hdfs://server128:9000/oooo5"));
		// 提交job
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
