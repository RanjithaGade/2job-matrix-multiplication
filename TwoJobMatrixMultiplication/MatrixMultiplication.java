import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MatrixMultiplication extends Configured implements Tool {
 private static final String OUTPUT_PATH = "Matrix_Intermediateoutput10";

@Override
 public int run(String[] args) throws Exception {
	Configuration conf = getConf();
	 FileSystem fs = FileSystem.get(conf);
    
    Job job = new Job(conf,"job 1");
    job.setJarByClass(MatrixMultiplication.class);
   //job.setJobName("Matrix Multiplication");
    job.setMapperClass(FirstMapper.class);
    job.setReducerClass(FirstReducer.class);
	 job.setInputFormatClass(TextInputFormat.class);
     job.setOutputFormatClass(TextOutputFormat.class);
    TextInputFormat.addInputPath(job, new Path(args[0]));
   TextOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH));
   //TextOutputFormat.setOutputPath(job, new Path(args[1]));
   

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
  

  job.waitForCompletion(true);

    Job job1 = new Job(conf,"job 2");
    job1.setJarByClass(MatrixMultiplication.class);
   // job.setJobName("Matrix Multiplication");
    job1.setMapperClass(SecondMapper.class);
    job1.setReducerClass(SecondReducer.class);
	 job1.setInputFormatClass(TextInputFormat.class);
  job1.setOutputFormatClass(TextOutputFormat.class);
    TextInputFormat.addInputPath(job1, new Path(OUTPUT_PATH));
    TextOutputFormat.setOutputPath(job1, new Path(args[1]));
   
    job1.setOutputKeyClass(Text.class);
    job1.setOutputValueClass(Text.class);
   
   return job1.waitForCompletion(true) ? 0 : 1;
   
}

  public static void main(String[] args) throws Exception {
     
ToolRunner.run(new Configuration(), new MatrixMultiplication(), args);

    
    
    
    
  }
}
