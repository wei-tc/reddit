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
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import reddithate.util.HateWords;
import reddithate.util.RussianTrollNames;
import reddithate.util.StringProcessing;

import java.io.IOException;
import java.util.List;

import static reddithate.util.RedditKeys.AUTHOR;
import static reddithate.util.RedditKeys.BODY;
import static reddithate.util.RedditKeys.SELFTEXT;
import static reddithate.util.RedditKeys.TITLE;

public class HateByWordCountIfTroll
{
    public static void main( String[] args ) throws Exception
    {
        Path in = new Path( args[0] );
        Path out = new Path( args[1] );

        Configuration conf = new Configuration();
        Job job = Job.getInstance( conf, "hateByWordCountIfTroll" );
        job.setJarByClass( HateByWordCountIfTroll.class );

        FileInputFormat.setInputDirRecursive( job, true );
        FileInputFormat.addInputPath( job, in );
        FileOutputFormat.setOutputPath( job, out );

        job.setMapperClass( TotalHateMapper.class );
        job.setReducerClass( HateByWordCount.HateByWordCountReducer.class );

        job.setMapOutputKeyClass( Text.class );
        job.setMapOutputValueClass( VIntWritable.class );
        job.setOutputKeyClass( Text.class );
        job.setOutputValueClass( DoubleWritable.class );

        if ( !job.waitForCompletion( true ) )
        {
            throw new Exception( "hateByWordCountIfTrollJob failed" );
        }
    }

    public static class TotalHateMapper extends Mapper<Object, Text, Text, VIntWritable>
    {
        static VIntWritable ZERO = new VIntWritable( 0 );
        static VIntWritable ONE = new VIntWritable( 1 );

        private Text isTroll = new Text();

        @Override
        protected void map( Object key, Text json, Context context ) throws IOException, InterruptedException
        {
            Any post = JsonIterator.deserialize( json.toString() );

            if ( RussianTrollNames.isTroll( post.get( AUTHOR ).toString() ) )
            {
                isTroll.set( "troll" );
            }
            else
            {
                isTroll.set( "normal" );
            }

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
                    context.write( isTroll, ONE );
                }
                else
                {
                    context.write( isTroll, ZERO );
                }
            }
        }
    }
}
