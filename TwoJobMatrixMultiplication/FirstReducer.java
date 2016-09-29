
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FirstReducer
  extends Reducer<Text, Text, Text, Text> {
  
  @Override
  public void reduce(Text key, Iterable<Text> values,
      Context context)
      throws IOException, InterruptedException {
   String firstMatrix[] = new String[100];
	String secondMatrix[] = new String[100];
	int firstIndex=0,
		secondIndex=0;

    for (Text val : values) {
		String str = val.toString();
		if (str.contains("A"))
		{
			firstMatrix[firstIndex] = val.toString();
			firstIndex++;
		}
		else
		{
			secondMatrix[secondIndex] = val.toString();
			secondIndex++;
		}                    }

for (int k=0; k<firstIndex; k++)
{
	String[] insideFirst=firstMatrix[k].split(",");
	for (int l=0; l<secondIndex; l++)
	{
		String[] insideSecond=secondMatrix[l].split(",");
int firstMatrixVal = Integer.parseInt(insideFirst[2]);
int secondMatrixVal = Integer.parseInt(insideSecond[2]);

		String tempValue = 1+","+insideFirst[1]+","+insideSecond[1]+","+firstMatrixVal*secondMatrixVal;
		
		context.write(key, new Text(tempValue));	

	}
}


   
  }
}
