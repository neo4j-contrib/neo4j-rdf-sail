package org.neo4j.rdf.sail.rmi;

import java.net.URI;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import org.junit.Test;
import org.neo4j.api.core.NeoService;
import org.neo4j.rdf.sail.BaseSailTest;
import org.neo4j.rdf.sail.BatchInserter;
import org.neo4j.rdf.sail.NeoSail;
import org.neo4j.rdf.sail.NeoTestUtils;
import org.neo4j.rdf.store.CachingLuceneIndexService;
import org.neo4j.rdf.store.RdfStore;
import org.neo4j.util.index.IndexService;
import org.openrdf.sail.Sail;

public class RmiSailTest extends BaseSailTest
{
	private static final int PORT = 5001;
	private static final String BASE_URI = "rmi://localhost:" + PORT + "/";
	private static final String RESOURCE_URI = BASE_URI + "NeoSail";

    private NeoService neo = null;
    private IndexService idx = null;
    
    static
    {
        try
        {
            LocateRegistry.createRegistry( PORT );
        }
        catch ( RemoteException e )
        {
            e.printStackTrace();
        }
    }
    
    private void setupRmi()
    {
        try
        {
            neo = NeoTestUtils.createNeo();
            idx = new CachingLuceneIndexService( neo );
            RdfStore store = createStore( neo, idx );
            RmiSailServer.register( new NeoSail( neo, store ), new java.net.URI(
                RESOURCE_URI ) );
        }
        catch ( Exception e )
        {
            throw new RuntimeException( e );
        }
    }
    
    private void tearDownRmi()
    {
        try
        {
            idx.shutdown();
            neo.shutdown();
        }
        catch ( Exception e )
        {
            throw new RuntimeException( e );
        }
    }
    
	public static void main( String[] args ) throws Exception
	{
		LocateRegistry.createRegistry( PORT );
		final NeoService neo = NeoTestUtils.createNeo();
        final IndexService idx = new CachingLuceneIndexService( neo );
        Runtime.getRuntime().addShutdownHook( new Thread()
        {
            @Override
            public void run()
            {
                NeoTestUtils.deleteEntireNodeSpace( neo );
                idx.shutdown();
                neo.shutdown();
            }
        } );
		RdfStore store = createStore( neo, idx );
		new BatchInserter( neo, store )
		    .insert( BatchInserter.listFiles( args ) );
		RmiSailServer.register( new NeoSail( neo, store ), new java.net.URI(
		    RESOURCE_URI ) );
		System.out.println( "Server started" );
	}

	@Test
	public void testGetConnection()
	{
	}

	@Override
	protected void before() throws Exception
	{
        setupRmi();
	}

	@Override
	protected void after()
	{
        tearDownRmi();
	}

	@Override
	protected Sail createSail() throws Exception
	{
		return new RmiSailClient( new URI( RESOURCE_URI ) );
	}

	@Override
	protected void deleteEntireNodeSpace() throws Exception
	{
        NeoTestUtils.deleteEntireNodeSpace( neo );
	}
}
