import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class CityInputFormat 
{
	public static void main(String [] args) throws IOException, InterruptedException, ClassNotFoundException
	{
		Job job = new Job();
		job.setJarByClass(CityInputFormat.class);
		
		job.setJobName("City Input Format");
		
		System.out.println(args[0]);
		System.out.println(args[1]);
		
		FileInputFormat.addInputPath(job,new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		//job.setInputFormatClass(RecordFileInputFormat.class);
		
		job.setMapperClass(CityMapper.class);
		job.setReducerClass(CityReducer.class);
		
		//this should be only set when the mapper and reducer o/p key or value is different or else go to next lines
		job.setMapOutputKeyClass(Record.class);
		job.setMapOutputValueClass(Record.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		
		job.waitForCompletion(true);
	}
}
