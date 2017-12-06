package cmsc433.p5;

import java.io.IOException;
import java.text.Collator;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;



/**
 * Map reduce which takes in a CSV file with tweets as input and output
 * key/value pairs.</br>
 * </br>
 * The key for the map reduce depends on the specified {@link TrendingParameter}
 * , <code>trendingOn</code> passed to
 * {@link #score(Job, String, String, TrendingParameter)}).
 */
public class TweetPopularityMR {

	// For your convenience...
	public static final IntWritable  TWEET_SCORE   = new IntWritable(1);
	public static final IntWritable  RETWEET_SCORE = new IntWritable(2);
	public static final IntWritable  MENTION_SCORE = new IntWritable(1);
	public static final IntWritable	 PAIR_SCORE = new IntWritable(1);

	// Is either USER, TWEET, HASHTAG, or HASHTAG_PAIR. Set for you before call to map()
	private static TrendingParameter trendingOn;

	public static class TweetMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
		
		// key type doesn't seem to matter. Keep it with Rance's.
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			// Converts the CSV line into a tweet object
			Tweet tweet = Tweet.createTweet(value.toString());
			// TODO: Your code goes here
			
			Text word = new Text();
			
			// Determine the trend, then give the proper score according
			// to the chart below.
			switch(trendingOn){
				case USER: 
					word.set(tweet.getUserScreenName());
					context.write(word,  TWEET_SCORE);
					
					if (tweet.wasRetweetOfUser()){
						word.set(tweet.getRetweetedUser());
						context.write(word, RETWEET_SCORE);
					}
					
					for (String s: tweet.getMentionedUsers()){
						word.set(s);
						context.write(word, MENTION_SCORE);
					}
					break;
				case TWEET: 
					// According to Ashwin, need to treat Tweet Id's as longs, not strings...
					// To deal with this, assume max tweet id length is 18,
					// add leading 0's until all tweets are at least 18 chars long
					String tweetId = tweet.getId().toString();
					String longConversion = "000000000000000000";
					String newTweetId = longConversion + tweetId;
					newTweetId = newTweetId.substring(tweetId.length());
									
					word.set(newTweetId);
					context.write(word, TWEET_SCORE);
					
					if (tweet.wasRetweetOfTweet()){
						tweetId = tweet.getRetweetedTweet().toString();
						newTweetId = longConversion + tweetId;
						newTweetId = newTweetId.substring(tweetId.length());
						word.set(newTweetId);
						context.write(word, RETWEET_SCORE);
					}
					break;
				case HASHTAG: 
					for (String hashtag: tweet.getHashtags()){
						word.set(hashtag);
						context.write(word, MENTION_SCORE);
					}
					break;
				case HASHTAG_PAIR: 
					ArrayList<String> hashtags = (ArrayList<String>) tweet.getHashtags();
					// Sort the hashtags to keep ordering
					hashtags.sort(new Comparator<String>(){
						@Override
						public int compare(String s1, String s2){
							return Collator.getInstance().compare(s1, s2);
						}
					});
					
					for (int i = 0; i < hashtags.size()-1; i++){
						for (int j = i+1; j < hashtags.size(); j++){
							word.set("(" + hashtags.get(i)  +"," + hashtags.get(j) + ")");
							context.write(word,  PAIR_SCORE);
						}
					}
					break;
			}
			
			
			// Maps the user screen name, tweet id, or hashtags used to relative weight
			/*
			 * Scores Calculations:
			 * 
			 * User = (# tweets by user) + 2*(# times retweeted) + (# times mentioned)
			 * Tweet = 1 + 2*(# times retweeted)
			 * Hashtag = (# times hashtag used)
			 * Hashtag_Pair = (# tweets the given pair of hashtags occurs in)
			 */
			
		}
	}

	public static class PopularityReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

			// TODO: Your code goes here
			// Takes the output of TweetMapper and calculates the score for each key
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			
			// If trending on Tweet Id's, need to remove leading 0's.
			if (trendingOn == TrendingParameter.TWEET){
				String keyString = key.toString();
				while(keyString.charAt(0) == '0')
					keyString = keyString.substring(1);
				key = new Text(keyString);
			}
			context.write(key, new IntWritable(sum));
		}
	}

	/**
	 * Method which performs a map reduce on a specified input CSV file and
	 * outputs the scored tweets, users, or hashtags.</br>
	 * </br>
	 * 
	 * @param job
	 * @param input
	 *          The CSV file containing tweets
	 * @param output
	 *          The output file with the scores
	 * @param trendingOn
	 *          The parameter on which to score
	 * @return true if the map reduce was successful, false otherwise.
	 * @throws Exception
	 */
	public static boolean score(Job job, String input, String output, TrendingParameter trendingOn) throws Exception {
		TweetPopularityMR.trendingOn = trendingOn;
		job.setJarByClass(TweetPopularityMR.class);

		// TODO: Set up map-reduce...
		// @author Rance Cleaveland

		// Set Mapper and Reducer classes for the job.
		job.setMapperClass(TweetMapper.class);
		job.setReducerClass(PopularityReducer.class);
		
		// Setup the input and output format for files
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		// Set key, output classes for the job.
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		// End
		FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));

		return job.waitForCompletion(true);
	}
	
}