import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class PrimeNumber {
	public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		private final static IntWritable zero = new IntWritable(0);
		private Text word = new Text();
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString());
			while (itr.hasMoreTokens()) {
				// String token = itr.nextToken();
				final int number = Integer.parseInt(itr.nextToken());
	            if(!isPrime(number)) {
	                word.set(Integer.toString(number));
	                context.write(word, one);
	            } else {
	                word.set(Integer.toString(number));
	                context.write(word, zero);
	            }
			}
		}
	}
	
	public static final boolean isPrime(int number) {
		boolean flag = false;
		int i = 2;
		while(i <= number / 2) {
           if(number % i == 0) {
                flag = true;
                break;
            }
            ++i;
        }
		return flag;
	}

	public static class DistinctReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		public void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {
			for (IntWritable val : values) {
				context.write(key, val);
				break;
			}
		}
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Prime Number");
		job.setJarByClass(PrimeNumber.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setReducerClass(DistinctReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
