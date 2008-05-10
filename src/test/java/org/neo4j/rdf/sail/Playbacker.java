package org.neo4j.rdf.sail;

public class Playbacker extends NeoTestCase
{
	public static void main( String[] args ) throws Exception
	{
//		final NeoService neo = new EmbeddedNeo( "var/neo" );
//		final IndexService indexService = new LuceneIndexService( neo );
//		VerboseQuadStore store = new VerboseQuadStore( neo, indexService );
//		File fileToPlayback = new File(
////			"playback/load-atoll-data.log"
////			"playback/atoll-activity.log"
////			"playback/atoll-activity-full.log"
//            "playback/relatedthings.log"
//			);
//		Sail baseSail = new NeoSail( neo, store );
//		try
//		{
//			for ( int i = 0; i < 20; i++ )
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
	}
	
	public void testNothing()
	{
	}
}
