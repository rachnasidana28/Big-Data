import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;
import java.util.Map.Entry;

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

public class Assignment01_02 {

	public static class Map extends Mapper<LongWritable, Text, Text, FloatWritable> {

		HashSet<String> hashSet = new HashSet<String>();

		public void setup(Context context) throws IOException {

			Configuration conf = context.getConfiguration();
			Path filteredFilePath = new Path(conf.get("inputFilePath"));

			FileSystem fs = FileSystem.get(conf);
			BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(filteredFilePath)));
			String line = br.readLine();
			while (line != null) {
				hashSet.add(line.trim());
				line = br.readLine();
			}
		}

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] entries = value.toString().trim().split("::");
			if (entries.length == 4)
				context.write(new Text(entries[2]), new FloatWritable(Float.parseFloat(entries[3].trim())));

		}
	}

	public static class Reduce extends Reducer<Text, FloatWritable, Text, FloatWritable> {
		HashMap<String, Float> map = new HashMap<String, Float>();

		public void reduce(Text key, Iterable<FloatWritable> values, Context context)
				throws IOException, InterruptedException {

			float sum = 0;
			float count = 0;
			for (FloatWritable value : values) {
				sum += (float) value.get();
				count++;
			}
			float avg = sum / count;
			map.put(key.toString(), avg);

		}

		protected void cleanup(Reducer<Text, FloatWritable, Text, FloatWritable>.Context context)
				throws IOException, InterruptedException {
			TreeMap<String, Float> sortedMap = sortByValue(map);
			int i = 0;
			for (Entry<String, Float> entry : sortedMap.entrySet()) {
				context.write(new Text(entry.getKey()), new FloatWritable(entry.getValue()));
				i++;
				if (i == 10)
					break;
			}
		}

		public static TreeMap<String, Float> sortByValue(HashMap<String, Float> unsortedMap) {
			TreeMap<String, Float> sortedMap = new TreeMap(new OrderByValueDesc(unsortedMap));
			sortedMap.putAll(unsortedMap);
			return sortedMap;
		}

	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

		Configuration conf = new Configuration();
		if (args.length != 2) {
			System.err.println("Incompatible Number Of Arguments");
			System.err.println("Please supply input data file and output folder.");
			System.exit(2);
		}
		FileSystem fs = FileSystem.get(conf);
		Path outputFilePath = new Path(args[1]);
		if (fs.exists(outputFilePath)) {
			/* If exist delete the output path */
			fs.delete(outputFilePath, true);
		}

		conf.set("inputFilePath", args[0]);
		Job job = Job.getInstance(conf, "Assignment01_02");

		job.setJarByClass(Assignment01_02.class);

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FloatWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, outputFilePath);
		job.waitForCompletion(true);

	}

}

class OrderByValueDesc implements Comparator<Object> {
	HashMap<String, Float> map;

	public OrderByValueDesc(HashMap<String, Float> map) {
		this.map = map;
	}

	public int compare(Object keyA, Object keyB) {
		if (map.get(keyA) <= map.get(keyB)) {
			return 1;
		} else {
			return -1;
		}
	}
}
