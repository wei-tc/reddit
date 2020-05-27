package reddithate.util;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.LowerCaseFilter;
import org.apache.lucene.analysis.core.StopFilter;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.analysis.en.PorterStemFilter;
import org.apache.lucene.analysis.standard.ClassicTokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class StringProcessing
{
    public static List<String> stem( String input ) throws IOException
    {
        Analyzer analyzer = new Stemmer();
        TokenStream stream = analyzer.tokenStream( null, input );
        return processStream( stream );
    }

    public static List<String> stopAndStem( String input ) throws IOException
    {
        Analyzer analyzer = new RemoveStopWordsStem();
        TokenStream stream = analyzer.tokenStream( null, input );
        return processStream( stream );
    }

    private static List<String> processStream( TokenStream stream ) throws IOException
    {
        final CharTermAttribute charTermAttribute = stream.addAttribute( CharTermAttribute.class );
        try
        {
            stream.reset();
            List<String> output = new ArrayList<>();
            while ( stream.incrementToken() )
            {
                output.add( charTermAttribute.toString() );
            }

            stream.end();
            return output;
        }
        finally
        {
            stream.close();
        }
    }

    public static class RemoveStopWordsStem extends Analyzer
    {
        @Override
        protected TokenStreamComponents createComponents( String s )
        {
            final Tokenizer source = new ClassicTokenizer();
            TokenStream result = new LowerCaseFilter( source );
            result = new StopFilter( result, EnglishAnalyzer.ENGLISH_STOP_WORDS_SET );
            result = new PorterStemFilter( result );
            return new TokenStreamComponents( source, result );
        }
    }

    public static class Stemmer extends Analyzer
    {
        @Override
        protected TokenStreamComponents createComponents( String s )
        {
            final Tokenizer source = new ClassicTokenizer();
            TokenStream result = new LowerCaseFilter( source );
            result = new PorterStemFilter( result );
            return new TokenStreamComponents( source, result );
        }
    }
}
