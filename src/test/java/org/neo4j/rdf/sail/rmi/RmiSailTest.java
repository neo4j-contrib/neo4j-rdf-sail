package org.neo4j.rdf.sail.rmi;

import java.net.URI;
import java.rmi.Naming;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.server.UnicastRemoteObject;

import org.junit.Test;
import org.neo4j.api.core.EmbeddedNeo;
import org.neo4j.api.core.NeoService;
import org.neo4j.rdf.sail.BaseSailTest;
import org.neo4j.rdf.sail.BatchInserter;
import org.neo4j.rdf.sail.NeoSail;
import org.neo4j.rdf.sail.NeoTestUtils;
import org.neo4j.rdf.store.CachingLuceneIndexService;
import org.neo4j.rdf.store.RdfStore;
import org.neo4j.rdf.store.VerboseQuadStore;
import org.neo4j.util.index.IndexService;
import org.openrdf.sail.Sail;

public class RmiSailTest extends BaseSailTest
{
	public interface NeoCallback extends Remote
	{
		void deleteEntireNodeSpace() throws RemoteException;
	}
	public static class NeoCallbackImpl extends UnicastRemoteObject implements
	    NeoCallback
	{
		private final NeoService neo;

		public NeoCallbackImpl( NeoService neo ) throws RemoteException
		{
			super();
			this.neo = neo;
		}

		public void deleteEntireNodeSpace()
		{
			NeoTestUtils.deleteEntireNodeSpace( neo );
		}
	}

	private static final int PORT = 5001;
	private static final String BASE_URI = "rmi://localhost:" + PORT + "/";
	private static final String RESOURCE_URI = BASE_URI + "NeoSail";
	private static final String CALLBACK_URI = BASE_URI + "NeoCallback";

	public static void main( String[] args ) throws Exception
	{
		LocateRegistry.createRegistry( PORT );
		NeoService neo = NeoTestUtils.createNeo();
		RdfStore store = createStore( neo );
		new BatchInserter( neo, store )
		    .insert( BatchInserter.listFiles( args ) );
		RmiSailServer.register( new NeoSail( neo, store ), new java.net.URI(
		    RESOURCE_URI ) );
		Naming.rebind( CALLBACK_URI, new NeoCallbackImpl( neo ) );
		System.out.println( "Server started" );
	}

	private NeoCallback callback;

	@Test
	public void testGetConnection()
	{
	}

	@Override
	protected void before() throws Exception
	{
		callback = ( NeoCallback ) Naming.lookup( CALLBACK_URI );
	}

	@Override
	protected void after()
	{
		//callback = null;
	}

	@Override
	protected Sail createSail() throws Exception
	{
		return new RmiSailClient( new URI( RESOURCE_URI ) );
	}

	@Override
	protected void deleteEntireNodeSpace() throws Exception
	{
		//callback.deleteEntireNodeSpace();
	}
}
