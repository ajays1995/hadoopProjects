import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;


public class Record implements WritableComparable<Record>
{
	
	private Text city;
	private IntWritable quantity,cost;
	
	public Record()
	{
		city = new Text();
		quantity = new IntWritable();
		cost = new IntWritable();
	}

	public Text getCity() {
		return city;
	}

	public void setCity(Text city) {
		this.city = city;
	}

	public IntWritable getQuantity() {
		return quantity;
	}

	public void setQuantity(IntWritable quantity) {
		this.quantity = quantity;
	}

	public IntWritable getCost() {
		return cost;
	}

	public void setCost(IntWritable cost) {
		this.cost = cost;
	}

	@Override
	public void readFields(DataInput arg0) throws IOException
	{
		city.readFields(arg0);
		quantity.readFields(arg0);
		cost.readFields(arg0);
	}

	@Override
	public void write(DataOutput arg0) throws IOException 
	{
		city.write(arg0);
		quantity.write(arg0);
		cost.write(arg0);
		
	}

	@Override
	public int compareTo(Record o) 
	{		
		return city.compareTo(o.getCity());
	}
	public int hashCode()
	{
		return city.hashCode();
	}

}
