
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SecondReducer
  extends Reducer<Text, Text, Text, Text> {
  String var="";
  @Override
  public void reduce(Text key, Iterable<Text> values,
      Context context)
      throws IOException, InterruptedException {
   int sum=0;
    for (Text val : values) {
		int value = Integer.parseInt(val.toString());
		
		sum =sum+value;
	}
	var = sum+"";
	context.write(key, new Text(var));


   
  }
}
