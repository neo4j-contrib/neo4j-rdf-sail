package org.neo4j.rdf.sail;

import java.io.File;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.EmbeddedGraphDatabase;
import org.neo4j.rdf.fulltext.FulltextIndex;
import org.neo4j.rdf.fulltext.SimpleFulltextIndex;
import org.neo4j.util.EntireGraphDeletor;

public class NeoTestUtils
{
	private static final File BASE_DIR = new File( "target/var" );
	
	private NeoTestUtils()
	{
	}

	public static void deleteEntireNodeSpace( GraphDatabaseService neo )
	{
		Transaction tx = neo.beginTx();
		try
		{
			for ( Relationship rel : neo.getReferenceNode().getRelationships() )
			{
				Node node = rel.getOtherNode( neo.getReferenceNode() );
				rel.delete();
				new EntireGraphDeletor().delete( node );
			}
			tx.success();
		}
		finally
		{
			tx.finish();
		}
	}

	public static GraphDatabaseService createNeo()
	{
		String dir = new File( BASE_DIR, "neo" ).getAbsolutePath();
		removeDir( new File( dir ) );
		final GraphDatabaseService neo = new EmbeddedGraphDatabase( dir );
		return neo;
	}
	
	public static FulltextIndex createFulltextIndex( GraphDatabaseService neo )
	{
		return new SimpleFulltextIndex( neo, new File( BASE_DIR, "fulltext" ) );
	}

	private static void removeDir( File dir )
	{
		if ( dir.exists() )
		{
			for ( File file : dir.listFiles() )
			{
				if ( file.isDirectory() )
				{
					removeDir( file );
				}
				else
				{
					file.delete();
				}
			}
		}
	}
}
