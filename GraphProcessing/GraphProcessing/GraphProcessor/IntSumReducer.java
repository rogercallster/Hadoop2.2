package GraphProcessor;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class IntSumReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
    public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
    	// System.out.println(key+" "+values +"  ....... " );
    	 
        long l = 9999999999l;
        String current = null;
        for (Text currentVal : values) {
            if (currentVal!=null)
            {
            	if( currentVal.toString().split("\\s+")[0].equals("nodenametype")){
            		current = currentVal.toString().split("\\s+")[1];
            	}
            	else if( currentVal.toString().split("\\s+")[0].equals("valuetype")){
            		int var=Integer.parseInt(currentVal.toString().split("\\s+")[1]);
            		if ( var < l) l=var;//choosing sortest path
            	}
            }
        }
        Text word = new Text();
        word.set(l+" "+current);
       
        context.write(key, word);
       
    }

}
