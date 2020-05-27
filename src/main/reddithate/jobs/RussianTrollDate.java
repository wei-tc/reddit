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
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

import static reddithate.util.RedditKeys.AUTHOR;
import static reddithate.util.RedditKeys.UTC;

public class RussianTrollDate
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
        Job job = Job.getInstance( conf, "RussianTrollDate" );
        job.setJarByClass( RussianTrollDate.class );

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

        job.setMapOutputKeyClass( Text.class );
        job.setMapOutputValueClass( VIntWritable.class );
        job.setOutputKeyClass( Text.class );
        job.setOutputValueClass( VIntWritable.class );

        if ( !job.waitForCompletion( true ) )
        {
            throw new Exception( "russianTrollDateJob failed" );
        }
    }

    public static String convertUTC( long utc )
    {
        LocalDateTime dateTime = LocalDateTime.ofEpochSecond( utc, 0, ZoneOffset.UTC );
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern( "dd MM yyyy" );
        return dateTime.format( formatter );
    }

    public static class RussianTrollMapper extends Mapper<Object, Text, Text, VIntWritable>
    {
        @Override
        protected void map( Object key, Text json, Context context ) throws IOException, InterruptedException
        {
            Any post = JsonIterator.deserialize( json.toString() );

            if ( RussianTrollNames.isTroll( post.get( AUTHOR ).toString() ) )
            {
                context.write( new Text( convertUTC( Long.parseLong( post.get( UTC ).toString() ) ) ),
                               ONE );
            }
        }
    }

    public static class NormalAuthorMapper extends Mapper<Object, Text, Text, VIntWritable>
    {
        @Override
        protected void map( Object key, Text json, Context context ) throws IOException, InterruptedException
        {
            Any post = JsonIterator.deserialize( json.toString() );

            if ( RussianTrollNames.isTroll( post.get( AUTHOR ).toString() ) )
            {
                context.write( new Text( convertUTC( Long.parseLong( post.get( UTC ).toString() ) ) ),
                               ONE );
            }
        }
    }

    public static class TimePostCountReducer extends Reducer<Text, VIntWritable, Text, VIntWritable>
    {
        private VIntWritable totalPosts = new VIntWritable();

        @Override
        protected void reduce( Text time, Iterable<VIntWritable> posts, Context context )
                throws IOException, InterruptedException
        {
            int total = 0;

            for ( VIntWritable p : posts )
            {
                total += p.get();
            }

            totalPosts.set( total );
            context.write( time, totalPosts );
        }
    }
}
