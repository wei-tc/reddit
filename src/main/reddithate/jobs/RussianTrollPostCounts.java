package reddithate.jobs;

import com.jsoniter.JsonIterator;
import com.jsoniter.any.Any;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import reddithate.util.RussianTrollNames;

import java.io.IOException;

import static reddithate.util.RedditKeys.AUTHOR;

public class RussianTrollPostCounts
{
    private static VIntWritable ZERO = new VIntWritable( 1 );
    private static VIntWritable ONE = new VIntWritable( 1 );
    private static String USAGE = "usage: in out rt|n";

    public static void main( String[] args ) throws Exception
    {
        if ( args.length != 3 )
        {
            throw new IllegalArgumentException( USAGE );
        }

        Path in = new Path( args[0] );
        Path out = new Path( args[1] );

        Configuration conf = new Configuration();
        Job job = Job.getInstance( conf, "russianTrollPostCounts" );
        job.setJarByClass( HateByWordCount.class );

        FileInputFormat.setInputDirRecursive( job, true );
        FileInputFormat.addInputPath( job, in );
        FileOutputFormat.setOutputPath( job, out );

        if ( args[2].equals( "rt" ) )
        {
            job.setMapperClass( RussianTrollMapper.class );
        }
        else if ( args[2].equals( "n" ) )
        {
            job.setMapperClass( NormalAuthorMapper.class );
        }
        else
        {
            throw new IllegalArgumentException( USAGE );
        }

        job.setReducerClass( PostCountReducer.class );

        job.setMapOutputKeyClass( Text.class );
        job.setMapOutputValueClass( VIntWritable.class );
        job.setOutputKeyClass( Text.class );
        job.setOutputValueClass( VIntWritable.class );

        if ( !job.waitForCompletion( true ) )
        {
            throw new Exception( "russianTrollPostCountsJob failed" );
        }
    }

    public static class RussianTrollMapper extends Mapper<Object, Text, Text, VIntWritable>
    {
        private Text author = new Text();

        @Override
        protected void map( Object key, Text json, Context context ) throws IOException, InterruptedException
        {
            Any post = JsonIterator.deserialize( json.toString() );
            author.set( post.get( AUTHOR ).toString() );
            if ( RussianTrollNames.isTroll( author.toString() ) )
            {
                context.write( author, ONE );
            }
        }
    }

    public static class NormalAuthorMapper extends Mapper<Object, Text, Text, VIntWritable>
    {
        private Text author = new Text();

        @Override
        protected void map( Object key, Text json, Context context ) throws IOException, InterruptedException
        {
            Any post = JsonIterator.deserialize( json.toString() );
            author.set( post.get( AUTHOR ).toString() );
            if ( !RussianTrollNames.isTroll( author.toString() ) )
            {
                context.write( author, ONE );
            }
        }
    }

    public static class PostCountReducer extends Reducer<Text, VIntWritable, Text, VIntWritable>
    {
        private VIntWritable totalCount = new VIntWritable();

        @Override
        protected void reduce( Text author, Iterable<VIntWritable> forEachPost, Context context )
                throws IOException, InterruptedException
        {
            int count = 0;

            for ( VIntWritable h : forEachPost )
            {
                count += 1;
            }

            totalCount.set( count );
            context.write( author, totalCount );
        }
    }
}
