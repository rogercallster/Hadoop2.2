package GraphProcessor;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TokenizerMapper extends Mapper<Object, Text, IntWritable, Text> {
	
	 
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        //System.out.println("value is " + value);
        String lines = value.toString();
        if (value!=null && lines != "\\s+")
        {
           
           String[] stringArr = lines.split("\\s+");
          
           Text word1 = new Text();
           word1.set("nodenametype "+stringArr[2]);
           context.write( new IntWritable( Integer.parseInt( stringArr[0] ) ), word1 );
           
           Text word2 = new Text();      
           word2.set("valuetype "+stringArr[1]);
           context.write( new IntWritable( Integer.parseInt( stringArr[0] ) ), word2 );
           
           Text word3 = new Text();
           String[] adjecentNodeList = stringArr[2].split(":");
           int distanceForCurrentNode = Integer.parseInt(stringArr[1]) + 1;
           for (String nodesName :adjecentNodeList){
               word3.set("valuetype "+distanceForCurrentNode);
               context.write(new IntWritable(Integer.parseInt(nodesName)), word3);
             
           }
        }  
         
    }
}
