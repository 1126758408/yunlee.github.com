
import java.io.IOException;
import java.util.StringTokenizer;

import java.math.RoundingMode;
import java.text.DecimalFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Hw2Part1 {

	/*
	public static class TokenizerMapper 
       extends Mapper<Object, Text, Text, IntWritable>{
    
    	private final static IntWritable one = new IntWritable(1);
    	private Text word = new Text();
      
    	public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      		StringTokenizer itr = new StringTokenizer(value.toString());
      		while (itr.hasMoreTokens()) {
        		word.set(itr.nextToken());
        		context.write(word, one);
      		}
    	}
  	}
  	*/

  	public static class recordMapper
  		extends Mapper<Object, Text, Text, DoubleWritable> {

  		private final static DoubleWritable sessionTime = new DoubleWritable();
  		private Text word = new Text();
  		

  		public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
  			System.out.println(value);
  			String[] eachRecord = value.toString().trim().split("\\s+");
  			System.out.println(eachRecord.length);
  			if (eachRecord.length == 3) {
  				//eachRecord
  				String sourDest = new String();
  				sourDest = sourDest + eachRecord[0].trim();
  				sourDest = sourDest + " ";
  				sourDest = sourDest + eachRecord[1].trim();
  				//double sessionTime = Double.valueOf(eachRecord[2].trim().toString());

  				word.set(sourDest);
  				sessionTime.set(Double.valueOf(eachRecord[2].trim().toString()));
  				context.write(word, sessionTime);

  			}
  		}
  	}


  	/*
  	public static class IntSumCombiner
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    	private IntWritable result = new IntWritable();

    	public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      		int sum = 0;
      		for (IntWritable val : values) {
        		sum += val.get();
      		}
      		result.set(sum);
      		context.write(key, result);
    	}
  	}
  	*/

  	// This is the Reducer class
  	// reference http://hadoop.apache.org/docs/r2.6.0/api/org/apache/hadoop/mapreduce/Reducer.html
  	//
  	// We want to control the output format to look at the following:
  	//
  	// count of word = count
  	//

  	/*
  	public static class IntSumReducer
       extends Reducer<Text,IntWritable,Text,Text> {

    	private Text result_key= new Text();
    	private Text result_value= new Text();
    	private byte[] prefix;
    	private byte[] suffix;

    	protected void setup(Context context) {
      		try {
        		prefix= Text.encode("count of ").array();
        		suffix= Text.encode(" =").array();
      		} catch (Exception e) {
        		prefix = suffix = new byte[0];
      		}
    	}

    	public void reduce(Text key, Iterable<IntWritable> values, Context context) 
    			throws IOException, InterruptedException {
      		int sum = 0;
      		for (IntWritable val : values) {
        		sum += val.get();
      		}

      		// generate result key
      		result_key.set(prefix);
      		result_key.append(key.getBytes(), 0, key.getLength());
      		result_key.append(suffix, 0, suffix.length);

      		// generate result value
      		result_value.set(Integer.toString(sum));

      		context.write(result_key, result_value);
    	}
  	}
  	*/

  	public static class recordReducer 
  		extends Reducer<Text, DoubleWritable, Text, Text>{

  		//private Text resultKey= new Text();
    	//private Text resultValue= new Text();

    	public void reduce(Text key, Iterable<DoubleWritable> values, Context context) 
    		throws IOException, InterruptedException {

    		int sessionNum = 0;
    		Double sessionAve = 0.0;
    		/*
    		for (IntWritable val : values) {
        		sum += val.get();
      		}
    		*/
    		for (DoubleWritable val : values) {
    			sessionNum ++;
    			sessionAve += val.get();
    		}
            sessionAve = sessionAve / sessionNum;

            Text resultKey = key;
            String resValueString = new String();
            //resValueString = resValueString + " ";
            //System.out.println(Integer.toString(sessionNum));
            resValueString = resValueString + Integer.toString(sessionNum);
            resValueString = resValueString + " ";
            DecimalFormat df = new DecimalFormat("0.000");
            resValueString = resValueString + df.format(sessionAve);
            Text resultValue= new Text(resValueString);

            context.write(resultKey, resultValue);
    	}

  	}


	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		Configuration conf = new Configuration();
    	String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    	if (otherArgs.length < 2) {
      		System.err.println("hadoop jar ./Hw2Part1.jar <input file> <output directory>");
      		System.exit(2);
    	}

    	Job job = Job.getInstance(conf, "session");

    	job.setJarByClass(Hw2Part1.class);



    	job.setMapperClass(recordMapper.class);
    	//job.setCombinerClass(IntSumCombiner.class);
    	job.setReducerClass(recordReducer.class);

    	job.setMapOutputKeyClass(Text.class);
    	job.setMapOutputValueClass(DoubleWritable.class);

    	job.setOutputKeyClass(Text.class);
    	job.setOutputValueClass(Text.class);

    	// add the input paths as given by command line
    	//for (int i = 0; i < otherArgs.length - 1; ++i) {
      	FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
    	//}

    	// add the output path as given by the command line
    	FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

    	System.exit(job.waitForCompletion(true) ? 0 : 1);
		
	}
}