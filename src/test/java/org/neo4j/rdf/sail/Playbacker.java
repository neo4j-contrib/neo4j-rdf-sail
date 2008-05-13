package org.neo4j.rdf.sail;

// Why is this a test case, well I don't know... don't really care.
public class Playbacker extends NeoTestCase
{
//	public static void main( String[] args ) throws Exception
//	{
//		File fileToPlayback = new File(
//			"playback/load-atoll-data.log"
//		//	"playback/atoll-activity.log"
//		//	"playback/atoll-activity-full.log"
//		//	"playback/relatedthings.log"
//			);
//		
//		Sail baseSail = null;
//		
//		// The Neo Rdf Store
//		final NeoService neo = new EmbeddedNeo( "var/neo" );
//		final IndexService indexService = new CachingLuceneIndexService( neo );
//		VerboseQuadStore store = new VerboseQuadStore( neo, indexService );
//		NeoSail neoSail = new NeoSail( neo, store );
//		baseSail = neoSail;
//		
//		// The Native Store
//		// baseSail = new NativeStore( new File( "var/ns" ) );
//		
//		try
//		{
//			baseSail.initialize();
//			for ( int i = 0; i < 1; i++ )
//			{
//				SimpleTimer timer = null;
//				Sail sail = null;
//				InputStream in = null;
//				try
//				{
//					in = new FileInputStream( fileToPlayback );
//					timer = new SimpleTimer();
//					sail = new PlaybackSail(baseSail, in);
//					System.out.println( "Playing back..." );
//					sail.initialize();
//					System.out.println( "Done" );
//				}
//				finally
//				{
//					timer.end();
//					in.close();
//					sail.shutDown();
//				}
//			}
//		}
//		finally
//		{
//			indexService.shutdown();
//			neo.shutdown();
//		}
//	}
//	
//	public void testNothing()
//	{
//	}
}
