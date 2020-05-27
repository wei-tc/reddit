package reddithate.util;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

public class UTCConverter
{
    public static final String HOUR_DATE = "kk dd MM yyyy";
    public static final String HOUR_MY = "kk MM yyyy";
    public static final String HOUR = "kk";


    public static String convertUTC( long utc, String pattern )
    {
        LocalDateTime dateTime = LocalDateTime.ofEpochSecond( utc, 0, ZoneOffset.UTC );
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern( pattern ); // kk == hh 24format
        return dateTime.format( formatter );
    }
}
