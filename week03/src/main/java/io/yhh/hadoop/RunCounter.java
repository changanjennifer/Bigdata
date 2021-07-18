package io.yhh.hadoop;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class RunCounter {

	public static class CountMapper extends Mapper<Text, Text, Text,FlowBean>{
		
		@Override
		public void map(Text key, Text text, Context context) throws IOException, InterruptedException{
			String[] words = text.toString().split("\\s+");
			
			FlowBean bean = null;
			if ( null != words ) {
				long upFlow = Long.parseLong(words[words.length-3]);
				long downFlow = Long.parseLong(words[words.length-2]);
				bean = new FlowBean(upFlow, downFlow);
				System.out.println("total:" + bean.getSumFlow());
				context.write( new Text(words[1]),bean );
			}
		}
	}
	
	public class CountReducer extends Reducer<Text, FlowBean,  FlowBean,Text> {
		
		@Override
		protected void reduce(Text key, Iterable<FlowBean> valuesMap, Context context) throws IOException, InterruptedException{
			long upFlow = 0;
			long downFlow = 0;
			for (FlowBean value: valuesMap ) {
				upFlow += value.getUpFlow();
				downFlow += value.getDownFlow();
			}
			context.write( new FlowBean(upFlow, downFlow),key);
		}
	}
	
	public static void main(String[] args) throws Exception {
		
		Configuration config = new Configuration();
		FileSystem fs = FileSystem.get(config);
		
		Job job =  Job.getInstance(config);
		job.setJarByClass(RunCounter.class);
		
		job.setJobName("counter");

		job.setMapperClass(RunCounter.CountMapper.class);
		job.setReducerClass(RunCounter.CountReducer.class);
		
		job.setMapOutputKeyClass(FlowBean.class);
		job.setMapOutputValueClass(Text.class);

//		job.setOutputKeyClass(Text.class);
//		job.setOutputValueClass(FlowBean.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		//FileInputFormat.addInputPath(job, new Path("/mooccount/input"));
		FileInputFormat.setInputPaths(job, "/mooccount/input/*");
		Path outPath = new Path("/mooccount/output");
		if (fs.exists(outPath)) {
			fs.delete(outPath, true);
		}
		
		FileOutputFormat.setOutputPath(job, outPath);
//		FileInputFormat.setInputPaths(job, new Path("f:\\hadoop-test"));
//		FileOutputFormat.setOutputPath(job, new Path("f:\\hadoop-test-out"));
		
		boolean completed = job.waitForCompletion(true);
		if (completed) {
			System.out.println("completed!");
		}
	}
}
