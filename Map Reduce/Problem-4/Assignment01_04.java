import java.io.IOException;
import java.io.InputStreamReader;
import java.io.BufferedReader;
import java.io.FileReader;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Assignment01_04 {

	public static class Map01 extends Mapper<LongWritable, Text, Text, Text> {
		private HashSet<String> business_id = new HashSet<>();

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {

			@SuppressWarnings("deprecation")
			Path[] localFiles = DistributedCache.getLocalCacheFiles(context.getConfiguration());
			if (localFiles != null && localFiles.length > 0) {
				for (Path businessFile : localFiles) {
					System.out.println(businessFile.toString());
					BufferedReader br = new BufferedReader(new FileReader(businessFile.getName()));
					String line = br.readLine();
					// Assuming each line contains a keyword to filter.
					while (line != null) {
						String[] entries = line.toString().trim().split("::");
						if (entries.length == 3) {
							if (entries[1].contains("Stanford")) {
								business_id.add(entries[0].trim());
							}
						}
						line = br.readLine();
					}
				}
			}
		}

		protected void map(LongWritable baseAddress, Text value, Context context)
				throws IOException, InterruptedException {
			Text user = new Text();
			Text rating = new Text();
			String[] fields = value.toString().split("::");

			if (fields.length == 4) {
				if (business_id.contains(fields[2].trim())) {
					user.set(fields[1].trim());
					rating.set(fields[3].trim());
					context.write(user, rating);
				}
			}

		}
	}

	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		if (args.length != 3) {
			System.err.println("Incompatible Number Of Arguments");
			System.exit(2);
		}
		Path outputFilePath = new Path(args[2]);
		if (fs.exists(outputFilePath)) {
			/* If exist delete the output path */
			fs.delete(outputFilePath, true);
		}

		Job job = Job.getInstance(conf, "Assignment01_04");

		job.setJarByClass(Assignment01_04.class);

		job.setMapperClass(Map01.class);
		job.setNumReduceTasks(0);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FloatWritable.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.addInputPath(job, new Path(args[1]));
		FileOutputFormat.setOutputPath(job, outputFilePath);
		DistributedCache.addCacheFile(new Path(args[0]).toUri(), job.getConfiguration());

		job.waitForCompletion(true);

	}

}
