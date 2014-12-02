import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;


public class CityReducer extends Reducer<Record, Record, Text, IntWritable>
{
	
	@Override
	  public void reduce(Record key, Iterable<Record> values, Context context)
	      throws IOException, InterruptedException 
	      {
			System.out.println("in the reducer");
			int total = 0;
				for(Record rec:values)
				{
					total = total + rec.getQuantity().get()*rec.getCost().get();					
				}
				context.write(key.getCity(), new IntWritable(total));
	      }
}
