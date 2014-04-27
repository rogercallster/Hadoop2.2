package GraphProcessor;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
 * References:
 * ------------------
 * http://hadooptutorial.wikispaces.com/Iterative+MapReduce+and+Counters
 *  Slides of Graph Algorithms with MapReduce by Jimmy Lin All the logic of this code is from  http://www.slideshare.net/Culturadigital/mapreduce-algorithm-design side 89 -99  
 * http://eclipse.sys-con.com/
 * http://eclipse.sys-con.com/node/1287801 
 * */



public class GraphMain {

	private static final transient Logger LOG = LoggerFactory.getLogger(GraphMain.class);
	public static String OUTPATH ="output";
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
			System.out.println("Program started");
			
			Configuration conf = new Configuration();		
            int MAXCOUNT=24;
			LOG.info("HDFS Root Path: {}", conf.get("fs.defaultFS"));
			LOG.info("MR Framework: {}", conf.get("mapreduce.framework.name"));
			/* Set the Input/Output Paths on HDFS */
			String inputPath = "input",inputFrom=inputPath;
			String outputPath = OUTPATH ;
			int count=0;
			Boolean TRUE = true;
   	 while (TRUE)
			{
			
			deleteFolder(conf,outputPath);
			Job job;
			job = Job.getInstance(conf);
			System.out.println("Location 1");
	        job.setOutputKeyClass(IntWritable.class);
	        job.setOutputValueClass(Text.class);
	        job.setMapperClass(TokenizerMapper.class);
	        job.setReducerClass(IntSumReducer.class);
	        job.setInputFormatClass(TextInputFormat.class);//import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
	        job.setOutputFormatClass(TextOutputFormat.class);
			System.out.println("Location 2");
			FileInputFormat.addInputPath(job, new Path(inputFrom));
			FileOutputFormat.setOutputPath(job, new Path(outputPath));
			System.out.println("Location 3");
			
				job.waitForCompletion(true);
			if(inputPath != inputFrom){
                String inputFile = inputFrom.replace("part-r-00000", "");
                Path deleteFile = new Path(inputFile);
                FileSystem fs = FileSystem.get(conf);
                fs.delete(deleteFile, true);
            }
			
			inputFrom=outputPath+"/part-r-00000";//setting up  new input path for next ittr
			outputPath = OUTPATH + System.currentTimeMillis();
			
			count++;
			//count till limit
			if (count==MAXCOUNT)
			{
				TRUE=false;
			}
	            }
	            
	        



			 
			
			
	            
	            }
			//System.exit(newjob.waitForCompletion(true) ? 0 : 1);
			
	
	
	private static void deleteFolder(Configuration conf, String folderPath ) throws IOException {
		FileSystem fs = FileSystem.get(conf);
		Path path = new Path(folderPath);
		if(fs.exists(path)) {
			fs.delete(path,true);
		}
		
	}
   
}
	


