package reddithate.jobs;

import com.jsoniter.JsonIterator;
import com.jsoniter.any.Any;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class TotalScore
{
    private static String USAGE = "USAGE: in out s|a";

    public static void main( String[] args ) throws Exception
    {
        if ( args.length != 3 )
        {
            throw new IllegalArgumentException( USAGE );
        }

        Path in = new Path( args[0] );
        Path out = new Path( args[1] );

        Configuration conf = new Configuration();
        Job job = Job.getInstance( conf, "score" );
        job.setJarByClass( TotalScore.class );

        FileInputFormat.setInputDirRecursive( job, true );
        FileInputFormat.addInputPath( job, in );
        FileOutputFormat.setOutputPath( job, out );

        if ( args[2].equals( "s" ) )
        {
            job.setMapperClass( SubredditByScoreMapper.class );
        }
        else if ( args[2].equals( "a" ) )
        {
            job.setMapperClass( AuthorByScoreMapper.class );
        }
        else
        {
            throw new IllegalArgumentException( USAGE );
        }

        job.setReducerClass( ScoreReducer.class );

        job.setMapOutputKeyClass( Text.class );
        job.setMapOutputValueClass( VIntWritable.class );
        job.setOutputKeyClass( Text.class );
        job.setOutputValueClass( LongWritable.class );

        if ( !job.waitForCompletion( true ) )
        {
            throw new Exception( "scoreJob failed" );
        }
    }

    public static class SubredditByScoreMapper extends Mapper<Object, Text, Text, VIntWritable>
    {
        private VIntWritable score = new VIntWritable();
        private Text subreddit = new Text();

        @Override
        protected void map( Object key, Text json, Context context ) throws IOException, InterruptedException
        {
            Any post = JsonIterator.deserialize( json.toString() );

            subreddit.set( post.get( "subreddit" ).toString() );
            score.set( post.get( "score" ).toInt() );

            context.write( subreddit, score );
        }
    }

    public static class AuthorByScoreMapper extends Mapper<Object, Text, Text, VIntWritable>
    {
        private VIntWritable score = new VIntWritable();
        private Text author = new Text();

        @Override
        protected void map( Object key, Text json, Context context ) throws IOException, InterruptedException
        {
            Any post = JsonIterator.deserialize( json.toString() );

            author.set( post.get( "author" ).toString() );
            score.set( post.get( "score" ).toInt() );

            context.write( author, score );
        }
    }

    public static class ScoreReducer extends Reducer<Text, VIntWritable, Text, LongWritable>
    {
        private LongWritable score = new LongWritable();

        @Override
        protected void reduce( Text key, Iterable<VIntWritable> forEachPost, Context context )
                throws IOException, InterruptedException
        {
            long total = 0;

            for ( VIntWritable score : forEachPost )
            {
                total += score.get();
            }

            score.set( total );
            context.write( key, score );
        }
    }
}
