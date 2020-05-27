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
import reddithate.util.RussianTrollNames;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static reddithate.util.RedditKeys.AUTHOR;
import static reddithate.util.RedditKeys.UTC;
import static reddithate.util.UTCConverter.HOUR;
import static reddithate.util.UTCConverter.convertUTC;

public class RussianTrollHour
{
    private static VIntWritable ONE = new VIntWritable( 1 );

    public static void main( String[] args ) throws Exception
    {
        if ( args.length != 3 )
        {
            throw new IllegalArgumentException( "usage: in out rt|n" );
        }

        Path in = new Path( args[0] );
        Path out = new Path( args[1] );

        Configuration conf = new Configuration();
        Job job = Job.getInstance( conf, "RussianTrollHour" );
        job.setJarByClass( RussianTrollHour.class );

        FileInputFormat.setInputDirRecursive( job, true );
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
                throw new IllegalArgumentException( "usage: in out rt|n" );
        }

        job.setReducerClass( TimePostCountReducer.class );

        job.setMapOutputKeyClass( VIntWritable.class );
        job.setMapOutputValueClass( VIntWritable.class );
        job.setOutputKeyClass( VIntWritable.class );
        job.setOutputValueClass( LongWritable.class );

        if ( !job.waitForCompletion( true ) )
        {
            throw new Exception( "russianTrollHourJob failed" );
        }
    }

    public static VIntWritable convertHour( String hour )
    {
        Map<String, VIntWritable> toVIntWritable = new HashMap<>();
        toVIntWritable.put( "01", new VIntWritable( 1 ) );
        toVIntWritable.put( "02", new VIntWritable( 2 ) );
        toVIntWritable.put( "03", new VIntWritable( 3 ) );
        toVIntWritable.put( "04", new VIntWritable( 4 ) );
        toVIntWritable.put( "05", new VIntWritable( 5 ) );
        toVIntWritable.put( "06", new VIntWritable( 6 ) );
        toVIntWritable.put( "07", new VIntWritable( 7 ) );
        toVIntWritable.put( "08", new VIntWritable( 8 ) );
        toVIntWritable.put( "09", new VIntWritable( 9 ) );
        toVIntWritable.put( "10", new VIntWritable( 10 ) );
        toVIntWritable.put( "11", new VIntWritable( 11 ) );
        toVIntWritable.put( "12", new VIntWritable( 12 ) );
        toVIntWritable.put( "13", new VIntWritable( 13 ) );
        toVIntWritable.put( "14", new VIntWritable( 14 ) );
        toVIntWritable.put( "15", new VIntWritable( 15 ) );
        toVIntWritable.put( "16", new VIntWritable( 16 ) );
        toVIntWritable.put( "17", new VIntWritable( 17 ) );
        toVIntWritable.put( "18", new VIntWritable( 18 ) );
        toVIntWritable.put( "19", new VIntWritable( 19 ) );
        toVIntWritable.put( "20", new VIntWritable( 20 ) );
        toVIntWritable.put( "21", new VIntWritable( 21 ) );
        toVIntWritable.put( "22", new VIntWritable( 22 ) );
        toVIntWritable.put( "23", new VIntWritable( 23 ) );
        toVIntWritable.put( "24", new VIntWritable( 24 ) );
        return toVIntWritable.get( hour );
    }

    public static class RussianTrollMapper extends Mapper<Object, Text, VIntWritable, VIntWritable>
    {
        private VIntWritable hour = new VIntWritable();

        @Override
        protected void map( Object key, Text json, Context context ) throws IOException, InterruptedException
        {
            Any post = JsonIterator.deserialize( json.toString() );

            if ( RussianTrollNames.isTroll( post.get( AUTHOR ).toString() ) )
            {
                context.write( convertHour( convertUTC( Long.parseLong( post.get( UTC ).toString() ), HOUR ) ),
                               ONE );
            }
        }
    }

    public static class NormalAuthorMapper extends Mapper<Object, Text, VIntWritable, VIntWritable>
    {
        @Override
        protected void map( Object key, Text json, Context context ) throws IOException, InterruptedException
        {
            Any post = JsonIterator.deserialize( json.toString() );

            if ( !RussianTrollNames.isTroll( post.get( AUTHOR ).toString() ) )
            {
                context.write( convertHour( convertUTC( Long.parseLong( post.get( UTC ).toString() ), HOUR ) ),
                               ONE );
            }
        }
    }

    public static class TimePostCountReducer extends Reducer<VIntWritable, VIntWritable, VIntWritable, LongWritable>
    {
        private LongWritable totalPosts = new LongWritable();

        @Override
        protected void reduce( VIntWritable hour, Iterable<VIntWritable> posts, Context context )
                throws IOException, InterruptedException
        {
            long total = 0;

            for ( VIntWritable p : posts )
            {
                total += p.get();
            }

            totalPosts.set( total );
            context.write( hour, totalPosts );
        }
    }
}
