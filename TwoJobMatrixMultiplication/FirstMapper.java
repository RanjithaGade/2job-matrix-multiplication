import java.io.IOException;
import java.util.*;
 
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
 
public class FirstMapper  extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
           
            String line = value.toString();
            String[] indicesAndValue = line.split(",");
			//int length = indicesAndValue.length();
            //Text outputKey = new Text();
            //Text outputValue = new Text();
			if(indicesAndValue[0].equals("A"))
			{
			String toBuildValue= indicesAndValue[0]+","+indicesAndValue[1]+","+indicesAndValue[3];
			context.write(new Text(indicesAndValue[2]), new Text(toBuildValue));
			}
			else
			{
String toBuildValue= indicesAndValue[0]+","+indicesAndValue[2]+","+indicesAndValue[3];
			context.write(new Text(indicesAndValue[1]), new Text(toBuildValue));
			
			}

		}

	
}