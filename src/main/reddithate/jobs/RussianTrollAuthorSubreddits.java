package reddithate.jobs;

import com.jsoniter.JsonIterator;
import com.jsoniter.any.Any;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import reddithate.util.RussianTrollNames;

import java.io.IOException;

import static reddithate.util.RedditKeys.AUTHOR;
import static reddithate.util.RedditKeys.SUBREDDIT;

public class RussianTrollAuthorSubreddits extends Configured implements Tool
{
    private static final VIntWritable ONE = new VIntWritable( 1 );
    private static final String[] INPATHS = { "comments/RC_20", "submissions/RS_20" };
    private static final String[] OUTPATHS = { "comments/", "submissions/" };
    private static final String[] FILEDATE = { "16-03/",
                                               "16-04/",
                                               "16-05/",
                                               "16-06/",
                                               "16-07/",
                                               "16-08/",
                                               "16-09/",
                                               "16-10/",
                                               "17-12/",
                                               "18-01/",
                                               "18-02/" };
    private static final String[] SUBMISSION_TROLLNAMES = {

    };

    private static final String[] COMMENT_TROLLNAMES = {

    };


    private static String USAGE = "USAGE: in out rt|n [m]";

    public static void main( String[] args ) throws Exception
    {
        if ( args.length < 3 || args.length > 4 )
        {
            throw new IllegalArgumentException( USAGE );
        }

        if ( args.length == 4 && args[3].equals( "m" ) )
        {
            for ( int i = 0; i < INPATHS.length; i++ )
            {
                String inpath = INPATHS[i];
                String outpath = OUTPATHS[i];

                for ( String fd : FILEDATE )
                {
                    String[] next = new String[args.length];
                    System.arraycopy( args, 0, next, 0, args.length );
                    next[0] += inpath + fd;
                    next[1] += outpath + fd;
                    ToolRunner.run( new RussianTrollAuthorSubreddits(), next );
                }
            }
        }
        else
        {
            aggregate( args );
        }
    }

    public static void aggregate( String[] args ) throws Exception
    {
        Path in = new Path( args[0] );
        Path out = new Path( args[1] );

        Configuration conf = new Configuration();
        Job job = Job.getInstance( conf, "RussianTrollAuthorSubreddits" );
        job.setJarByClass( RussianTrollSubreddits.class );

        FileInputFormat.addInputPath( job, in );
        FileOutputFormat.setOutputPath( job, out );

        job.setMapperClass( RussianTrollMapper.class );
        job.setReducerClass( RussianTrollWords.TotalReducer.class );

        job.setMapOutputKeyClass( Text.class );
        job.setMapOutputValueClass( VIntWritable.class );
        job.setOutputKeyClass( Text.class );
        job.setOutputValueClass( VLongWritable.class );

        if ( !job.waitForCompletion( true ) )
        {
            throw new Exception( "russianTrollSubredditJob aggregate failed" );
        }
    }

    @Override
    public int run( String[] args ) throws Exception
    {
        Path in = new Path( args[0] );
        Path out = new Path( args[1] );

        Configuration conf = new Configuration();
        Job job = Job.getInstance( conf, "RussianTrollAuthorSubreddits" );
        job.setJarByClass( RussianTrollSubreddits.class );

        FileInputFormat.addInputPath( job, in );
        FileOutputFormat.setOutputPath( job, out );

        job.setMapperClass( RussianTrollMapper.class );
        job.setReducerClass( RussianTrollWords.TotalReducer.class );

        job.setMapOutputKeyClass( Text.class );
        job.setMapOutputValueClass( VIntWritable.class );
        job.setOutputKeyClass( Text.class );
        job.setOutputValueClass( VLongWritable.class );

        job.submit();
        return 0;
    }

    public static class RussianTrollMapper extends Mapper<Object, Text, Text, VIntWritable>
    {
        private Text subreddit = new Text();

        @Override
        protected void map( Object key, Text json, Context context ) throws IOException, InterruptedException
        {
            Any post = JsonIterator.deserialize( json.toString() );

            if ( RussianTrollNames.isTroll( post.get( AUTHOR ).toString() ) )
            {
                subreddit.set( post.get( SUBREDDIT ).toString() );
                context.write( subreddit, ONE );
            }
        }
    }
}