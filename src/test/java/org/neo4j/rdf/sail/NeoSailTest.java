package org.neo4j.rdf.sail;

import org.junit.Test;
import org.neo4j.api.core.NeoService;
import org.neo4j.rdf.store.RdfStore;
import org.openrdf.sail.Sail;

public class NeoSailTest extends BaseSailTest
{
	private RdfStore store = null;
	private NeoService neo;

	public static void main( String[] args ) throws Exception
	{
		BatchInserter.main( args );
	}

	@Test
	public void testLoadNeoSail()
	{
	}

	@Override
	protected void before()
	{
		neo = NeoTestUtils.createNeo();
		this.store = createStore( neo );
	}

	@Override
	protected void after()
	{
		this.store = null;
	}

	@Override
	protected Sail createSail() throws Exception
	{
		return new NeoSail( neo, store );
	}

	@Override
	protected void deleteEntireNodeSpace() throws Exception
	{
		NeoTestUtils.deleteEntireNodeSpace( neo );
	}
}
