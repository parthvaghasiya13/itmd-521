import java.io.IOException;
import com.cloudera.sqoop.lib.RecordParser.ParseError;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.*;

public class Average_Temperature extends Configured implements Tool {

  public static class AverageRecordMapper
      extends Mapper<LongWritable, Text, IntWritable, IntWritable> {

    private Record averageRecord = null;

    public void map(LongWritable int_k, Text t, Context context) {
      Record record = new Record();
      try {
        record.parse(t); // Auto-generated: parse all fields from text.
      } catch (ParseError pe) {
        // Got a malformed record. Ignore it.
        return;
      }

			Integer temp;
			String temperature_str = record.get_temp();
			temperature = Integer.parseInt(temperature_str);
            Integer year=1985;
         try{
        context.write(new IntWritable(year), new IntWritable(temperature));
			}catch(Exception e){
					e.printStackTrace();
        }
	}
  }

  public static class AverageRecordReducer
      extends Reducer<IntWritable, IntWritable, IntWritable, FloatWritable> {

    // There will be a single reduce call with key '0' which gets
    // the max widget from each map task. Pick the max widget from
    // this list.
    public void reduce(IntWritable int_k, Iterable<IntWritable> vals, Context context)
        throws IOException, InterruptedException {
      Record averageRecord = null;
               
        Integer record_id = int_k.get();
                Integer year = 1985;
                        Integer sum = 0;
                        Integer count = 0;

                        for (IntWritable value:vals) {
                                sum = sum + value.get();
                                count = count + 1;
                        }
                        Float average = (float) sum/count;
                        context.write(new IntWritable(year), new FloatWritable(average));
  }
          }
  
  public int run(String [] args) throws Exception {
    Job job = new Job(getConf());

    job.setJarByClass(Average_Temperature.class);

    job.setMapperClass(AverageRecordMapper.class);
    job.setReducerClass(AverageRecordReducer.class);

    FileInputFormat.addInputPath(job, new Path("/user/vagrant/import_Parth_5"));
    FileOutputFormat.setOutputPath(job, new Path("averagerecord_5"));

	job.setMapOutputKeyClass(IntWritable.class);
	job.setMapOutputValueClass(IntWritable.class);


	job.setOutputKeyClass(IntWritable.class);
	job.setOutputValueClass(FloatWritable.class);
	job.setNumReduceTasks(1);

    if (!job.waitForCompletion(true)) {
      return 1;
    }
    return 0;
  }

  public static void main(String [] args) throws Exception {
    int ret = ToolRunner.run(new Average_Temperature(), args);
    System.exit(ret);
  }
}
