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
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Assignment01_03 {

	/**
	 * @param args
	 */
	public static class Map01 extends Mapper<LongWritable, Text, Text, FloatWritable> {

		HashSet<String> hashSet = new HashSet<String>();

		public void setup(Context context) throws IOException {

			Configuration conf = context.getConfiguration();
			Path filteredFilePath = new Path(conf.get("Review.csv"));

			FileSystem fs = FileSystem.get(conf);
			BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(filteredFilePath)));
			String line;
			line = br.readLine();
			// Assuming each line contains a keyword to filter.
			while (line != null) {
				hashSet.add(line.trim());
				line = br.readLine();
			}
		}

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			// from ratings

			String[] entries = value.toString().trim().split("::");
			if (entries.length == 4)
				context.write(new Text(entries[2]), new FloatWritable(Float.parseFloat(entries[3].trim())));

		}
	}

	public static class Reduce01 extends Reducer<Text, FloatWritable, Text, FloatWritable> {
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
			int i=0;
			for(Entry<String, Float> entry : sortedMap.entrySet())
			{
				context.write(new Text(entry.getKey()), new FloatWritable(entry.getValue()));
				i++;
				if(i==10)
					break;
			}
		}
		public static TreeMap<String, Float > sortByValue(HashMap<String, Float> unsortedMap) {
			TreeMap<String, Float> sortedMap = new TreeMap(new OrderByValueDescOrder(unsortedMap));
			sortedMap.putAll(unsortedMap);
			return sortedMap;
		}

	}
	
	public static class Map02 extends Mapper<LongWritable, Text, Text, Text> {
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			StringBuffer tuple = new StringBuffer("");
			String[] row = value.toString().split("\t");
			if (row != null) {
				tuple.append("reviewRelation\t" + row[1]);
				context.write(new Text(row[0].trim()), new Text(tuple.toString()));
			}
		}
	}

	public static class Map03 extends Mapper<LongWritable, Text, Text, Text> {
		HashSet<String> hashSet = new HashSet<String>();

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			StringBuffer tuple = new StringBuffer("");
			String[] row = value.toString().trim().split("::");
			if (row != null) {
				tuple.append("businessRelation\t" + row[1] + "\t" + row[2]);
				context.write(new Text(row[0].trim()), new Text(tuple.toString()));
			}
		}
	}
	public static class Reducer02 extends Reducer<Text, Text, Text, Text> {

		@Override
		public void reduce(Text business_id, Iterable<Text> tuples, Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			boolean flag = false;

			String reviewTuple = "";
			String businessTuple = "";
			for (Text tuple : tuples) {
				if (tuple.toString().contains("reviewRelation")) {
					reviewTuple = tuple.toString();
					flag = true;
				} else
					businessTuple = tuple.toString();
			}
			if (!flag)
				return;

			String[] dataReview = reviewTuple.split("\t");
			String[] dataBusiness = businessTuple.split("\t");
			String value = dataBusiness[1]+"\t"+ dataBusiness[2]+"\t" + dataReview[1];
			context.write(new Text(business_id), new Text(value));
		}
	}
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

		Configuration conf = new Configuration();
		if (args.length != 4) {
			System.err.println("Incompatible Number Of Arguments");
			System.err.println("Please supply input data file and output folder.");
			System.exit(2);
		}
		FileSystem fs = FileSystem.get(conf);
		Path outputFilePath = new Path(args[3]);
		Path interOutputFilePath = new Path(args[2]);
		if (fs.exists(outputFilePath)) {
			/* If exist delete the output path */
			fs.delete(outputFilePath, true);
		}
		if(fs.exists(interOutputFilePath)){
			/* If exist delete the output path */
			fs.delete(interOutputFilePath, true);
		}
		conf.set("Review.csv", args[0]);

		Job job1 = Job.getInstance(conf, "Assignment01_03");

		job1.setJarByClass(Assignment01_03.class);

		job1.setMapperClass(Map01.class);
		job1.setReducerClass(Reduce01.class);

		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(FloatWritable.class);
		FileInputFormat.addInputPath(job1, new Path(args[0]));
		FileOutputFormat.setOutputPath(job1, interOutputFilePath);
		job1.waitForCompletion(true);
		
		
		Job job2 = Job.getInstance(conf,"Second Map");
		job2.setJarByClass(Assignment01_03.class);
		
		MultipleInputs.addInputPath(job2, interOutputFilePath,
				TextInputFormat.class, Map02.class);
		
		MultipleInputs.addInputPath(job2, new Path(args[1]), TextInputFormat.class,
				Map03.class);
		FileOutputFormat.setOutputPath(job2, new Path(args[3]));
		
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);
		
		job2.setReducerClass(Reducer02.class);
		
		job2.waitForCompletion(true);
		fs.delete(interOutputFilePath, true);
	
	}

}
class OrderByValueDescOrder implements Comparator<Object> {
	HashMap<String, Float> map;

	public OrderByValueDescOrder(HashMap<String, Float> map) {
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

