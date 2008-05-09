package org.neo4j.rdf.sail;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;

import org.neo4j.api.core.EmbeddedNeo;
import org.neo4j.api.core.NeoService;
import org.neo4j.rdf.store.RdfStore;
import org.neo4j.rdf.store.VerboseQuadStore;
import org.neo4j.util.index.IndexService;
import org.neo4j.util.index.LuceneIndexService;
import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.sail.SailRepository;
import org.openrdf.rio.RDFFormat;
import org.openrdf.sail.Sail;

public class BatchInserter
{
	private NeoService neo;
	private RdfStore store;
	
	public BatchInserter( NeoService neo, RdfStore store )
	{
		this.neo = neo;
		this.store = store;
	}
	
	public void insert( File... files ) throws Exception
	{
		SimpleTimer timer = new SimpleTimer();
		Sail sail = new NeoSail( neo, store );
		try
		{
			sail.initialize();
			Repository repo = new SailRepository( sail );
			RepositoryConnection rc = repo.getConnection();
			for ( File file : files )
			{
				rc.add( file, "", RDFFormat.TRIG );
			}
			rc.commit();
			rc.close();
		}
		finally
		{
			sail.shutDown();
			timer.end();
		}
	}
	
	public static void main( final String[] args ) throws Exception
	{
		final NeoService neo = new EmbeddedNeo( "var/neo" );
		final IndexService indexService = new LuceneIndexService( neo );
		VerboseQuadStore store = new VerboseQuadStore( neo, indexService );
		try
		{
			new BatchInserter( neo, store ).insert( listFiles( args ) );
		}
		finally
		{
			indexService.shutdown();
			neo.shutdown();
		}
	}
	
	private static File[] listFiles( final String[] args )
	{
		ArrayList<File> files = new ArrayList<File>();
		if ( args.length == 0 )
		{
			// Just a stupid default
			addIfExists( files, new File( "cens.trig" ) );
		}
		
		for ( String arg : args )
		{
			File file = new File( arg );
			if ( !addIfExists( files, file ) )
			{
				System.out.println( "File '" + file.getAbsolutePath() +
					"' doesn't exist, skipping" );
			}
		}
		return files.toArray( new File[ files.size() ] );
	}
	
	private static boolean addIfExists( Collection<File> files, File file )
	{
		if ( file.exists() )
		{
			files.add( file );
			return true;
		}
		return false;
	}
}
