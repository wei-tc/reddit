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
import static reddithate.util.RedditKeys.UTC;
import static reddithate.util.UTCConverter.HOUR_DATE;
import static reddithate.util.UTCConverter.convertUTC;

public class RussianTrollHourDate
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
        Job job = Job.getInstance( conf, "RussianTrollHourDate" );
        job.setJarByClass( RussianTrollHourDate.class );

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

        job.setReducerClass( RussianTrollDate.TimePostCountReducer.class );

        job.setMapOutputKeyClass( Text.class );
        job.setMapOutputValueClass( VIntWritable.class );
        job.setOutputKeyClass( Text.class );
        job.setOutputValueClass( VIntWritable.class );

        if ( !job.waitForCompletion( true ) )
        {
            throw new Exception( "russianTrollHourDateJob failed" );
        }
    }

    public static class RussianTrollMapper extends Mapper<Object, Text, Text, VIntWritable>
    {
        Text hDate = new Text();

        @Override
        protected void map( Object key, Text json, Context context ) throws IOException, InterruptedException
        {
            Any post = JsonIterator.deserialize( json.toString() );

            if ( RussianTrollNames.isTroll( post.get( AUTHOR ).toString() ) )
            {
                hDate.set( convertUTC( Long.parseLong( post.get( UTC ).toString() ), HOUR_DATE ) );
                context.write( hDate, ONE );
            }
        }
    }

    public static class NormalAuthorMapper extends Mapper<Object, Text, Text, VIntWritable>
    {
        Text hDate = new Text();

        @Override
        protected void map( Object key, Text json, Context context ) throws IOException, InterruptedException
        {
            Any post = JsonIterator.deserialize( json.toString() );

            if ( !RussianTrollNames.isTroll( post.get( AUTHOR ).toString() ) )
            {
                hDate.set( convertUTC( Long.parseLong( post.get( UTC ).toString() ), HOUR_DATE ) );
                context.write( hDate, ONE );
            }
        }
    }
}
