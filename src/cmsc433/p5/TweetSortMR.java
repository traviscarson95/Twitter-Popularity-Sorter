package cmsc433.p5;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import cmsc433.p5.TweetPopularityMR.PopularityReducer;
import cmsc433.p5.TweetPopularityMR.TweetMapper;

/**
 * Map reduce which sorts the output of {@link TweetPopularityMR}.
 * The input will either be in the form of: </br>
 * 
 * <code></br>
 * &nbsp;(screen_name,  score)</br>
 * &nbsp;(hashtag, score)</br>
 * &nbsp;(tweet_id, score)</br></br>
 * </code>
 * 
 * The output will be in the same form, but with results sorted on the score.
 * 
 */
public class TweetSortMR {

	/**
	 * Minimum <code>int</code> value for a pair to be included in the output.
	 * Pairs with an <code>int</code> less than this value are omitted.
	 */
	private static int CUTOFF = 10;

	public static class SwapMapper
	extends Mapper<Object, Text, IntWritable, Text> {

		String      id;
		int         score;

		@Override
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] columns = value.toString().split("\t");
			id = columns[0];
			score = Integer.valueOf(columns[1]);

			// TODO: Your code goes here
			if (score >= CUTOFF){
				// Set the score to be negative so it sorts backwards (greatest to least)
				// Flip the order (int, string) instead of (string, int) so that the 
				// data is sorted on the int
				context.write(new IntWritable(-score), new Text(id));
			} 
			
			// Entries sorted by value from greatest to least.
			// Any Integer less than CUTOFF should be omitted
		}
	}

	public static class SwapReducer
	extends Reducer<IntWritable, Text, Text, IntWritable> {

		@Override
		public void reduce(IntWritable key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			// TODO: Your code goes here
			for (Text val: values){
				// Set the negative score back to positive
				// Flip the data back to (string, int) for output
				context.write(val, new IntWritable(-key.get()));
			}
			
		}
	}

	/**
	 * This method performs value-based sorting on the given input by configuring
	 * the job as appropriate and using Hadoop.
	 * 
	 * @param job
	 *          Job created for this function
	 * @param input
	 *          String representing location of input directory
	 * @param output
	 *          String representing location of output directory
	 * @return True if successful, false otherwise
	 * @throws Exception
	 */
	public static boolean sort(Job job, String input, String output, int cutoff)
			throws Exception {

		CUTOFF = cutoff;

		job.setJarByClass(TweetSortMR.class);

		// TODO: Set up map-reduce...
		// Set Mapper and Reducer classes for the job.
		job.setMapperClass(SwapMapper.class);
		job.setReducerClass(SwapReducer.class);
				
		// Setup the input and output format for files
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		// Set the map's output classes because they are flipped
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);
				
		// Set key, output classes for the job.
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);



		// End

		FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));

		return job.waitForCompletion(true);
	}

}
