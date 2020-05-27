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
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import reddithate.util.RussianTrollNames;
import reddithate.util.StringProcessing;

import java.io.IOException;
import java.util.List;

import static reddithate.util.RedditKeys.AUTHOR;
import static reddithate.util.RedditKeys.BODY;
import static reddithate.util.RedditKeys.SELFTEXT;
import static reddithate.util.RedditKeys.TITLE;

public class RussianTrollWords extends Configured implements Tool
{
    private static final VIntWritable ONE = new VIntWritable( 1 );
    private static final String USAGE = "usage: in out rt|n [m]";

    private static final String[] inpaths = { "comments/RC_20", "submissions/RS_20" };
    private static final String[] outpaths = { "comments/", "submissions/" };
    private static final String[] filedate = { "16-03/",
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

    public static void main( String[] args ) throws Exception
    {
        if ( args.length < 3 || args.length > 4 )
        {
            throw new IllegalArgumentException( USAGE );
        }

        if ( args.length == 4 && args[3].equals( "m" ) )
        {
            for ( int i = 0; i < inpaths.length; i++ )
            {
                String inpath = inpaths[i];
                String outpath = outpaths[i];

                for ( String fd : filedate )
                {
                    String[] next = new String[args.length];
                    System.arraycopy( args, 0, next, 0, args.length );
                    next[0] += inpath + fd;
                    next[1] += outpath + fd;
                    ToolRunner.run( new RussianTrollWords(), next );
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
        Configuration conf = new Configuration();
        Job job = Job.getInstance( conf, "RussianTrollWords" );
        jobSetup( args, job );

        if ( !job.waitForCompletion( true ) )
        {
            throw new Exception( "russianTrollWordsJob aggregate failed" );
        }
    }

    private static void jobSetup( String[] args, Job job ) throws IOException
    {
        Path in = new Path( args[0] );
        Path out = new Path( args[1] );
        job.setJarByClass( RussianTrollWords.class );

        FileInputFormat.addInputPath( job, in );
        FileOutputFormat.setOutputPath( job, out );

        switch ( args[2] )
        {
            case "rt":
                job.setMapperClass( RussianTrollMapper.class );
                break;
            case "n":
                job.setMapperClass( NormalAuthorMapper.class );
                break;
            default:
                throw new IllegalArgumentException( USAGE );
        }

        job.setReducerClass( TotalReducer.class );

        job.setMapOutputKeyClass( Text.class );
        job.setMapOutputValueClass( VIntWritable.class );
        job.setOutputKeyClass( Text.class );
        job.setOutputValueClass( VLongWritable.class );
    }

    @Override
    public int run( String[] args ) throws Exception
    {
        Configuration conf = new Configuration();
        Job job = Job.getInstance( conf, "RussianTrollWords" );
        jobSetup( args, job );

        job.submit();
        return 0;
    }

    public static class RussianTrollMapper extends Mapper<Object, Text, Text, VIntWritable>
    {
        private Text word = new Text();

        @Override
        protected void map( Object key, Text json, Context context ) throws IOException, InterruptedException
        {
            Any post = JsonIterator.deserialize( json.toString() );

            if ( RussianTrollNames.isTroll( post.get( AUTHOR ).toString() ) )
            {
                write( context, post.get( BODY ).toString() );
                write( context, post.get( TITLE ).toString() );
                write( context, post.get( SELFTEXT ).toString() );
            }
        }

        void write( Context context, String text ) throws IOException, InterruptedException
        {
            if ( text.isEmpty() )
            {
                return;
            }

            List<String> tokens = StringProcessing.stopAndStem( text );

            for ( String token : tokens )
            {
                word.set( token );
                context.write( word, ONE );
            }
        }
    }

    public static class NormalAuthorMapper extends Mapper<Object, Text, Text, VIntWritable>
    {
        private Text word = new Text();

        @Override
        protected void map( Object key, Text json, Context context ) throws IOException, InterruptedException
        {
            Any post = JsonIterator.deserialize( json.toString() );

            if ( !RussianTrollNames.isTroll( post.get( AUTHOR ).toString() ) )
            {
                write( context, post.get( BODY ).toString() );
                write( context, post.get( TITLE ).toString() );
                write( context, post.get( SELFTEXT ).toString() );
            }
        }

        void write( Context context, String text ) throws IOException, InterruptedException
        {
            if ( text.isEmpty() )
            {
                return;
            }

            List<String> tokens = StringProcessing.stopAndStem( text );

            for ( String token : tokens )
            {
                word.set( token );
                context.write( word, ONE );
            }
        }
    }

    public static class TotalReducer extends Reducer<Text, VIntWritable, Text, VLongWritable>
    {
        private VLongWritable totalCount = new VLongWritable();

        @Override
        protected void reduce( Text key, Iterable<VIntWritable> count, Context context )
                throws IOException, InterruptedException
        {
            long total = 0;

            for ( VIntWritable p : count )
            {
                total += p.get();
            }

            totalCount.set( total );
            context.write( key, totalCount );
        }
    }
}
