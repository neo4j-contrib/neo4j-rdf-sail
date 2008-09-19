package org.neo4j.rdf.sail;

import java.io.File;

import org.neo4j.api.core.EmbeddedNeo;
import org.neo4j.api.core.NeoService;
import org.neo4j.api.core.Node;
import org.neo4j.api.core.Relationship;
import org.neo4j.api.core.Transaction;
import org.neo4j.rdf.fulltext.FulltextIndex;
import org.neo4j.rdf.fulltext.SimpleFulltextIndex;
import org.neo4j.util.EntireGraphDeletor;

public class NeoTestUtils
{
	private static final File BASE_DIR = new File( "var/test" );
	
	private NeoTestUtils()
	{
	}

	public static void deleteEntireNodeSpace( NeoService neo )
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

	public static NeoService createNeo()
	{
		String dir = new File( BASE_DIR, "neo" ).getAbsolutePath();
		removeDir( new File( dir ) );
		final NeoService neo = new EmbeddedNeo( dir );
		return neo;
	}
	
	public static FulltextIndex createFulltextIndex( NeoService neo )
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
