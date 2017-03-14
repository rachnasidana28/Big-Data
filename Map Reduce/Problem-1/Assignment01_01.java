import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Assignment01_01 {

	public static class Map extends Mapper<LongWritable, Text, Text,LongWritable> {

		HashSet<String> hashSet = new HashSet<String>();

		public void setup(Context context) throws IOException {

			Configuration conf = context.getConfiguration();
			Path inputFilePath = new Path(conf.get("inputFile"));

			FileSystem fs = FileSystem.get(conf);
			BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(inputFilePath)));
			String line = br.readLine();
			while (line != null) {
				hashSet.add(line.trim());
				line = br.readLine();
			}
		}

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			// Filtering the Palo Alto Businesses from business.csv
			String[] entries = value.toString().trim().split("::");
			if (entries[1].contains("Palo Alto")) {
				String[] categories = entries[2].split(",");
				for (String cat : categories) {
					
					cat = cat.replace("List(", "").toString();
					cat = cat.replace("(", "");
					cat = cat.replace(")", "");
					if (cat.trim().equals(""))
						continue;
					context.write(new Text(cat.trim()), new LongWritable(1));
				}
			}
		}
	}

	public static class Reduce extends Reducer<Text, LongWritable, Text,Text> {
		public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
			long sum = 0;
			for (LongWritable value : values) {
				sum +=value.get();
			}
			context.write(key, new Text(""));
		}

	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		if (args.length != 2) {
			System.err.println("Incompatible Number Of Arguments");
			System.err.println("Please supply input data file and output folder.");
			System.exit(2);
		}

		Path outputFilePath = new Path(args[1]);
		if (fs.exists(outputFilePath)) {
			/* If exist delete the output path */
			fs.delete(outputFilePath, true);
		}

		conf.set("inputFile", args[0]);
		Job job = Job.getInstance(conf, "Assignment01");

		job.setJarByClass(Assignment01_01.class);

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));

		FileOutputFormat.setOutputPath(job, outputFilePath);

		job.waitForCompletion(true);

	}

}
