package org.neo4j.rdf.sail.utils;

import java.io.IOException;

import org.apache.log4j.FileAppender;
import org.apache.log4j.Layout;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;

public class MutatingLogger
{
    private static Logger logger;
    static
    {
        Layout layout = new PatternLayout( "[%d{ISO8601}]: %m%n" );
        logger = Logger.getLogger( MutatingLogger.class );
        try
        {
            logger.addAppender( new FileAppender( layout, 
                "mutating-operations.log" ) );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
        logger.info( "new session ==========================================" );
    }
    
    public static Logger getLogger()
    {
        return logger;
    }
    
    public static class Timer
    {
        private long time;
        
        public Timer()
        {
            reset();
        }
        
        public long lap()
        {
            long newTime = System.currentTimeMillis();
            long diff = newTime - time;
            this.time = newTime;
            return diff;
        }
        
        public void reset()
        {
            this.time = System.currentTimeMillis();
        }
    }
}
