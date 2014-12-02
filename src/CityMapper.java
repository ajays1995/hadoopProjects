import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class CityMapper extends Mapper<LongWritable, Text, Record, Record> 
{
	@Override
	  public void map(LongWritable key, Text value, Context context)
	      
	      {
			System.out.println("key is  "+key);
			System.out.println("each line  "+value);
			String line = value.toString();
			
			String [] array = line.split(",");
			Record rec = new Record();
			rec.setCity(new Text(array[0]));
			rec.setQuantity(new IntWritable(Integer.parseInt(array[1])));
			rec.setCost(new IntWritable(Integer.parseInt(array[2])));
			
			try {
				context.write(rec, rec);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
	      }
}
