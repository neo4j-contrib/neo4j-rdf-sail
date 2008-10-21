package org.neo4j.rdf.sail;

public abstract class Playbacker
{
}

/*import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;

import org.neo4j.api.core.EmbeddedNeo;
import org.neo4j.api.core.NeoService;
import org.neo4j.rdf.store.CachingLuceneIndexService;
import org.neo4j.rdf.store.VerboseQuadStore;
import org.neo4j.util.index.IndexService;
import org.openrdf.sail.Sail;
import org.openrdf.sail.SailConnection;

import com.knowledgereefsystems.agsail.sesameutils.replay.PlaybackSail;

// Why is this a test case, well I don't know... don't really care.
public class Playbacker
{
	public static void main( String[] args ) throws Exception
	{
		File fileToPlayback = new File(
//			"playback/load-atoll-data.log"
//			"playback/atoll-activity.log"
//			"playback/atoll-activity-full.log"
			"playback/relatedthings.log"
			);
		
		Sail baseSail = null;
		
		// The Neo Rdf Store
		final NeoService neo = new EmbeddedNeo( "var/neo",
		    EmbeddedNeo.loadConfigurations( "neo_no_adaptive_cache.props" ) );
		final IndexService indexService = new CachingLuceneIndexService( neo );
		VerboseQuadStore store =
			new VerboseQuadStore( neo, indexService );
		NeoSail neoSail = new NeoSail( neo, store );
		baseSail = neoSail;
		
		// The Native Store
//		baseSail = new NativeStore( new File( "var/ns" ) );
		
		try
		{
			baseSail.initialize();
			SimpleTimer timer = new SimpleTimer();
			for ( int i = 0; i < 3; i++ )
			{
				Sail sail = null;
				InputStream in = null;
				try
				{
					if ( i > 0 )
					{
						timer.newLap();
					}
					in = new FileInputStream( fileToPlayback );
					sail = new PlaybackSail(baseSail, in);
					System.out.println( "Playing back " + baseSail + " with " +
					    fileToPlayback.getName() + "..." );
					sail.initialize();
					System.out.println( "Done" );
					SailConnection connection = baseSail.getConnection();
					System.out.println( "Size=" + connection.size() );
					connection.close();
				}
				finally
				{
					in.close();
					sail.shutDown();
				}
			}
			timer.end();
		}
		finally
		{
			indexService.shutdown();
			neo.shutdown();
		}
	}
	
	public void testNothing()
	{
	}
}
*/
