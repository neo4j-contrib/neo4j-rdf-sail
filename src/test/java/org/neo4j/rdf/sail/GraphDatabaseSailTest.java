package org.neo4j.rdf.sail;

import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.rdf.store.CachingLuceneIndexService;
import org.neo4j.rdf.store.RdfStore;
import org.neo4j.index.IndexService;
import org.openrdf.sail.Sail;

public class GraphDatabaseSailTest extends BaseSailTest
{
	private RdfStore store = null;
	private GraphDatabaseService graphDb = null;
    private IndexService indexService = null;

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
		graphDb = TestUtils.createGraphDb();
        indexService = new CachingLuceneIndexService( graphDb );
		this.store = createStore( graphDb, indexService );
	}
	
	@Override
    protected void tearDownSail() throws Exception
    {
	    clearFulltextIndex();
	    super.tearDownSail();
    }

	@Override
	protected void after()
	{
        indexService.shutdown();
        graphDb.shutdown();
		this.store = null;
	}

	@Override
	protected Sail createSail() throws Exception
	{
		return new GraphDatabaseSail( graphDb, store );
	}

	@Override
	protected void deleteEntireNodeSpace() throws Exception
	{
//		NeoTestUtils.deleteEntireNodeSpace( neo );
	}
}
