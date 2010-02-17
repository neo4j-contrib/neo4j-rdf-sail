package org.neo4j.rdf.sail.rmi;

import java.net.URI;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;

import org.junit.Test;
import org.neo4j.rdf.sail.BaseSailTest;
import org.neo4j.rdf.sail.GraphDatabaseSail;
import org.neo4j.rdf.sail.TestUtils;
import org.neo4j.rdf.store.CachingLuceneIndexService;
import org.neo4j.rdf.store.RdfStore;
import org.neo4j.rdf.store.RdfStoreImpl;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.index.IndexService;
import org.openrdf.sail.Sail;

public class RmiSailTest extends BaseSailTest
{
	private static final int PORT = 5001;
	private static final String BASE_URI = "rmi://localhost:" + PORT + "/";
	private static final String RESOURCE_URI = BASE_URI + "GraphDbSail";

    private GraphDatabaseService graphDb = null;
    private IndexService idx = null;
    
    private RdfStore store = null;
    private GraphDatabaseSail graphDbSail = null;
    
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
            graphDb = TestUtils.createGraphDb();
            idx = new CachingLuceneIndexService( graphDb );
            store = createStore( graphDb, idx );
            graphDbSail = new GraphDatabaseSail( graphDb, store );
            RmiSailServer.register( graphDbSail, new java.net.URI(
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
            graphDb.shutdown();
        }
        catch ( Exception e )
        {
            throw new RuntimeException( e );
        }
    }
    
	public static void main( String[] args ) throws Exception
	{
		/* RESTORE ME
		LocateRegistry.createRegistry( PORT );
		final GraphDatabaseService graphDb = TestUtils.createGraphDb();
        final IndexService idx = new CachingLuceneIndexService( graphDb );
        Runtime.getRuntime().addShutdownHook( new Thread()
        {
            @Override
            public void run()
            {
                TestUtils.deleteEntireNodeSpace( graphDb );
                idx.shutdown();
                graphDb.shutdown();
            }
        } );
		RdfStore store = createStore( graphDb, idx );
		new BatchInserter( graphDb, store )
		    .insert( BatchInserter.listFiles( args ) );
		RmiSailServer.register( new GraphDbSail( graphDb, store ), new java.net.URI(
		    RESOURCE_URI ) );
		System.out.println( "Server started" );
		*/
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
    protected void tearDownSail() throws Exception
    {
	    super.tearDownSail();
		( ( RdfStoreImpl ) store ).getFulltextIndex().clear();
	    this.graphDbSail.shutDown();
    }

	@Override
	protected Sail createSail() throws Exception
	{
		return new RmiSailClient( new URI( RESOURCE_URI ) );
	}

	@Override
	protected void deleteEntireNodeSpace() throws Exception
	{
//        TestUtils.deleteEntireNodeSpace( graphDb );
	}
}
