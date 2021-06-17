import java.io.IOException;
import java.util.List;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.util.StringTokenizer;


public class equijoin {
	
	public static class Map extends Mapper<LongWritable, Text, Text, Text> 
	{
		private Text Key = new Text();
		private Text Value = new Text();
		public void map(LongWritable key, Text value, Context context) 
		throws IOException, InterruptedException 
		{
			StringTokenizer rows = new StringTokenizer(value.toString(), "\n");
			int rows_count = rows.countTokens();
			for(int i=0; i<rows_count; i++) 
			{
				String parsed_row = rows.nextToken();
				StringTokenizer parsed_block = new StringTokenizer(parsed_row, ", ");
				String temp = parsed_block.nextToken();
				String join_key = parsed_block.nextToken();
				Key.set(join_key);
				Value.set(parsed_row);
				context.write(Key, Value);
			}
		}
	}
	
	public static class Red extends Reducer<Text, Text, Text, Text> 
	{

		public void reduce(Text key, Iterable<Text> values, Context context)
		throws IOException, InterruptedException 
		{
			List<String> table1 = new ArrayList<String>();
			List<String> table2 = new ArrayList<String>();
			List<String> rows = new ArrayList<String>();			
			String result = new String();
			String tablename = "";
			Text output = new Text();
			for (Text value : values) 
			{
				rows.add(value.toString());
			}
			if(rows.size() == 1) 
			{
				return;
			} 
			else 
			{
				String[] t_name = rows.get(0).split(", ");
				tablename = t_name[0];
				for (String r : rows) {
					if (tablename.equals(r.split(", ")[0])) {
						table1.add(r);
					} else {
						table2.add(r);
					}
				}
				if(table1.size() == 0 || table2.size() == 0) 
				{
					return;
				}
				else 
				{
					for (String t1 : table1) 
					{
						for (String t2 : table2) 
						{
							result = t1 + ", " + t2;
							output.set(result);
							context.write(null, output);
						}
					}

				}
			}
		}
	}
	

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		Job job = new Job(conf, "equijoin");
		job.setJarByClass(equijoin.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Red.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[1]));
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
