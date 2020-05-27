package reddithate.jobs;

import com.jsoniter.JsonIterator;
import com.jsoniter.any.Any;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import reddithate.util.HateWords;
import reddithate.util.StringProcessing;

import java.io.IOException;
import java.util.List;

import static reddithate.util.RedditKeys.AUTHOR;
import static reddithate.util.RedditKeys.BODY;
import static reddithate.util.RedditKeys.SELFTEXT;
import static reddithate.util.RedditKeys.SUBREDDIT;
import static reddithate.util.RedditKeys.TITLE;

public class HateByWordCount
{
    private static VIntWritable ZERO = new VIntWritable( 1 );
    private static VIntWritable ONE = new VIntWritable( 1 );

    public static void main( String[] args ) throws Exception
    {
        if ( args.length != 3 )
        {
            throw new IllegalArgumentException( "usage: in out s|a" );
        }

        Path in = new Path( args[0] );
        Path out = new Path( args[1] );

        Configuration conf = new Configuration();
        Job job = Job.getInstance( conf, "hateByWordCount" );
        job.setJarByClass( HateByWordCount.class );

        FileInputFormat.setInputDirRecursive( job, true );
        FileInputFormat.addInputPath( job, in );
        FileOutputFormat.setOutputPath( job, out );

        switch ( args[2] )
        {
            case "s":
                job.setMapperClass( SubredditByHateByWordCountMapper.class );
                break;
            case "a":
                job.setMapperClass( AuthorByHateByWordCountMapper.class );
                break;
            default:
                throw new IllegalArgumentException( "usage: in out s|a" );
        }

        job.setReducerClass( HateByWordCountReducer.class );

        job.setMapOutputKeyClass( Text.class );
        job.setMapOutputValueClass( VIntWritable.class );
        job.setOutputKeyClass( Text.class );
        job.setOutputValueClass( DoubleWritable.class );

        if ( !job.waitForCompletion( true ) )
        {
            throw new Exception( "hateByWordCountJob failed" );
        }
    }

    public static class SubredditByHateByWordCountMapper extends Mapper<Object, Text, Text, VIntWritable>
    {
        private Text subreddit = new Text();

        @Override
        protected void map( Object key, Text json, Context context ) throws IOException, InterruptedException
        {
            Any post = JsonIterator.deserialize( json.toString() );
            subreddit.set( post.get( SUBREDDIT ).toString() );

            write( context, post.get( BODY ).toString() );
            write( context, post.get( TITLE ).toString() );
            write( context, post.get( SELFTEXT ).toString() );
        }

        void write( Context context, String text ) throws IOException, InterruptedException
        {
            if ( text.isEmpty() )
            {
                return;
            }

            List<String> tokens = StringProcessing.stem( text );

            for ( String token : tokens )
            {
                if ( HateWords.isHateWord( token ) )
                {
                    context.write( subreddit, ONE );
                }
                else
                {
                    context.write( subreddit, ZERO );
                }
            }
        }
    }

    public static class AuthorByHateByWordCountMapper extends Mapper<Object, Text, Text, VIntWritable>
    {
        private Text author = new Text();

        @Override
        protected void map( Object key, Text json, Context context ) throws IOException, InterruptedException
        {
            Any post = JsonIterator.deserialize( json.toString() );
            author.set( post.get( AUTHOR ).toString() );

            write( context, post.get( BODY ).toString() );
            write( context, post.get( TITLE ).toString() );
            write( context, post.get( SELFTEXT ).toString() );
        }

        void write( Context context, String text ) throws IOException, InterruptedException
        {
            if ( text.isEmpty() )
            {
                return;
            }

            List<String> tokens = StringProcessing.stem( text );

            for ( String token : tokens )
            {
                if ( HateWords.isHateWord( token ) )
                {
                    context.write( author, ONE );
                }
                else
                {
                    context.write( author, ZERO );
                }
            }
        }
    }

    public static class HateByWordCountReducer extends Reducer<Text, VIntWritable, Text, DoubleWritable>
    {
        private DoubleWritable hateByWordCount = new DoubleWritable();

        @Override
        protected void reduce( Text source, Iterable<VIntWritable> forEachPost, Context context )
                throws IOException, InterruptedException
        {
            long hate = 0;
            long wordCount = 0;

            for ( VIntWritable h : forEachPost )
            {
                hate += h.get();
                wordCount += 1;
            }

            hateByWordCount.set( hate / ( wordCount * 1.0 ) );
            context.write( source, hateByWordCount );
        }
    }
}
