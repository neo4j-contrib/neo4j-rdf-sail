package org.neo4j.rdf.sail;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import info.aduna.iteration.CloseableIteration;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;

import javax.xml.datatype.XMLGregorianCalendar;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.api.core.NeoService;
import org.neo4j.rdf.store.RdfStore;
import org.neo4j.rdf.store.VerboseQuadStore;
import org.neo4j.util.index.IndexService;
import org.openrdf.model.Literal;
import org.openrdf.model.Namespace;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.datatypes.XMLDatatypeUtil;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.model.vocabulary.RDFS;
import org.openrdf.model.vocabulary.XMLSchema;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.impl.EmptyBindingSet;
import org.openrdf.query.parser.ParsedQuery;
import org.openrdf.query.parser.sparql.SPARQLParser;
import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.sail.SailRepository;
import org.openrdf.rio.RDFFormat;
import org.openrdf.sail.Sail;
import org.openrdf.sail.SailChangedEvent;
import org.openrdf.sail.SailChangedListener;
import org.openrdf.sail.SailConnection;
import org.openrdf.sail.SailConnectionListener;
import org.openrdf.sail.SailException;
import org.neo4j.rdf.fulltext.FulltextIndex;

public abstract class BaseSailTest
{
	private Sail sail = null;
	private FulltextIndex fulltextIndex = null;

	@Before
	public final void setUp() throws Exception
	{
		before();
		this.sail = createSail();
		sail.initialize();
		Repository repo = new SailRepository( sail );
		RepositoryConnection rc = repo.getConnection();
		rc.add( BaseSailTest.class.getResource( "neoSailTest.trig" ), "",
		    RDFFormat.TRIG );
		rc.add( BaseSailTest.class.getResource( "fulltextTest.trig" ), "",
		    RDFFormat.TRIG );
		rc.add( BaseSailTest.class.getResource( "docs.trig" ), "",
		    RDFFormat.TRIG );
		rc.commit();
		rc.close();

		/*
		while (!fulltextIndex.queueIsEmpty()) {
		    Object mutex = "";
		    synchronized (mutex) {
			    System.out.println("waiting for FulltextIndex queue to empty");
			    mutex.wait(100);
		    }
		}*/
	}

	protected final RdfStore createStore( NeoService neo, 
        IndexService indexService )
	{
		fulltextIndex = NeoTestUtils.createFulltextIndex( neo );
		fulltextIndex.clear();
		return new VerboseQuadStore( neo, indexService, null,
			fulltextIndex );
	}
    
	protected abstract void before() throws Exception;

	protected abstract void after() throws Exception;

	@After
	public final void tearDown() throws Exception
	{
		tearDownSail();
		deleteEntireNodeSpace();
        after();
	}

	protected abstract void deleteEntireNodeSpace() throws Exception;

	protected Sail sail()
	{
		return this.sail;
	}

	protected void tearDownSail() throws Exception
	{
		sail().shutDown();
	}

	protected abstract Sail createSail() throws Exception;

    // full text search ////////////////////////////////////////////////////////

	@Test
    public void testNeo() throws Exception {
        NeoRdfSailConnection sc;
        sc = (NeoRdfSailConnection) sail.getConnection();
        try {
            evaluateFreetextQuery("Yoichiro", sc);
//            testFreetextQuery("Yoichiro", sc);
//            testFreetextQuery("Endo", sc);
//            testFreetextQuery("Lilia", sc);
//            testFreetextQuery("Moshkina", sc);
        } finally {
            sc.close();
        }

//        printRepository(repo, RDFFormat.TRIG, System.out);
    }

    private void evaluateFreetextQuery(final String query,
                                       final NeoRdfSailConnection sc) throws SailException {
        CloseableIteration<? extends FulltextQueryResult, SailException> iter;
        iter = sc.evaluate(query);
        try {
            while (iter.hasNext()) {
                FulltextQueryResult r = iter.next();

                double score = r.getScore();
                String snippet = r.getSnippet();
                Statement st = r.getStatement();
                Resource subject = st.getSubject();
                URI predicate = st.getPredicate();
                Value object = st.getObject();
                Resource context = st.getContext();

                System.out.println("result for query \"" + query + "\":");
                System.out.println("    score: " + score);
                System.out.println("    snippet: " + snippet);
                System.out.println("    statement:");
                System.out.println("        subject: " + subject);
                System.out.println("        predicate: " + predicate);
                System.out.println("        object: " + object);
                System.out.println("        context: " + context);
            }
        } finally {
            iter.close();
        }
    }

    // statement manipulation //////////////////////////////////////////////////
	@Test
	public void testGetStatementsS_POG() throws Exception
	{
		boolean includeInferred = false;
		SailConnection sc = sail.getConnection();
		URI uriA = sail.getValueFactory().createURI(
		    "http://example.org/test/S_POG#a" );
		URI uriB = sail.getValueFactory().createURI(
		    "http://example.org/test/S_POG#b" );
		URI uriC = sail.getValueFactory().createURI(
		    "http://example.org/test/S_POG#c" );
		URI uriD = sail.getValueFactory().createURI(
		    "http://example.org/test/S_POG#d" );
		int before, after;
		// default context, different S,P,O
		sc.removeStatements( uriA, null, null );
		sc.commit();
		before = countStatements( sc.getStatements( uriA, null, null,
		    includeInferred ) );
		sc.addStatement( uriA, uriB, uriC );
		sc.commit();
		after = countStatements( sc.getStatements( uriA, null, null,
		    includeInferred ) );
		assertEquals( 0, before );
		assertEquals( 1, after );
		// one specific context, different S,P,O
		sc.removeStatements( uriA, null, null, uriD );
		sc.commit();
		before = countStatements( sc.getStatements( uriA, null, null,
		    includeInferred, uriD ) );
		sc.addStatement( uriA, uriB, uriC, uriD );
		sc.commit();
		after = countStatements( sc.getStatements( uriA, null, null,
		    includeInferred, uriD ) );
		assertEquals( 0, before );
		assertEquals( 1, after );
		// one specific context, same S,P,O,G
		sc.removeStatements( uriA, null, null, uriA );
		sc.commit();
		before = countStatements( sc.getStatements( uriA, null, null,
		    includeInferred, uriA ) );
		sc.addStatement( uriA, uriA, uriA, uriA );
		sc.commit();
		after = countStatements( sc.getStatements( uriA, null, null,
		    includeInferred, uriA ) );
		assertEquals( 0, before );
		assertEquals( 1, after );
		// default context, same S,P,O
		sc.removeStatements( uriA, null, null );
		sc.commit();
		before = countStatements( sc.getStatements( uriA, null, null,
		    includeInferred ) );
		sc.addStatement( uriA, uriA, uriA );
		sc.commit();
		after = countStatements( sc.getStatements( uriA, null, null,
		    includeInferred ) );
		assertEquals( 0, before );
		assertEquals( 1, after );
		sc.close();
	}

	@Test
	public void testGetStatementsSP_OG() throws Exception
	{
		SailConnection sc = sail.getConnection();
		URI uriA = sail.getValueFactory().createURI(
		    "http://example.org/test/SP_OG" );
		int before, after;
		// Add statement to the implicit null context.
		// sc.removeStatements(null, null, null, uriA);
		before = countStatements( sc.getStatements( uriA, uriA, null, false ) );
		sc.addStatement( uriA, uriA, uriA );
		sc.commit();
		after = countStatements( sc.getStatements( uriA, uriA, null, false ) );
		assertEquals( 0, before );
		assertEquals( 1, after );
		sc.close();
	}

	@Test
	public void testGetStatementsO_SPG() throws Exception
	{
		SailConnection sc = sail.getConnection();
		URI uriA = sail.getValueFactory().createURI(
		    "http://example.org/test/O_SPG" );
		Literal plainLitA = sail.getValueFactory().createLiteral(
		    "arbitrary plain literal 9548734867" );
		Literal stringLitA = sail.getValueFactory().createLiteral(
		    "arbitrary string literal 8765", XMLSchema.STRING );
		int before, after;
		// Add statement to a specific context.
		sc.removeStatements( null, null, uriA, uriA );
		sc.commit();
		before = countStatements( sc.getStatements( null, null, uriA, false ) );
		sc.addStatement( uriA, uriA, uriA );
		sc.commit();
		after = countStatements( sc.getStatements( null, null, uriA, false ) );
		assertEquals( 0, before );
		assertEquals( 1, after );
		// Add plain literal statement to the default context.
		sc.removeStatements( null, null, plainLitA );
		sc.commit();
		before = countStatements( sc.getStatements( null, null, plainLitA,
		    false ) );
		sc.addStatement( uriA, uriA, plainLitA );
		sc.commit();
		after = countStatements( sc
		    .getStatements( null, null, plainLitA, false ) );
		assertEquals( 0, before );
		assertEquals( 1, after );
		// Add string-typed literal statement to the default context.
		sc.removeStatements( null, null, plainLitA );
		sc.commit();
		before = countStatements( sc.getStatements( null, null, stringLitA,
		    false ) );
		sc.addStatement( uriA, uriA, stringLitA );
		sc.commit();
		after = countStatements( sc.getStatements( null, null, stringLitA,
		    false ) );
		assertEquals( 0, before );
		assertEquals( 1, after );
		sc.close();
	}

	@Test
	public void testGetStatementsPO_SG() throws Exception
	{
		SailConnection sc = sail.getConnection();
		URI uriA = sail.getValueFactory().createURI(
		    "http://example.org/test/PO_SG#a" );
		URI uriB = sail.getValueFactory().createURI(
		    "http://example.org/test/PO_SG#b" );
		URI marko = sail.getValueFactory().createURI(
		    "http://knowledgereefsystems.com/thing/q" );
		URI firstName = sail.getValueFactory().createURI(
		    "http://knowledgereefsystems.com/2007/11/core#firstName" );
		Literal plainLitA = sail.getValueFactory().createLiteral(
		    "arbitrary plain literal 8765675" );
		Literal markoName = sail.getValueFactory().createLiteral( "Marko",
		    XMLSchema.STRING );
		int before, after;
		// Add statement to the implicit null context.
		sc.removeStatements( null, null, null, uriA );
		sc.commit();
		before = countStatements( sc.getStatements( null, uriA, uriA, false ) );
		sc.addStatement( uriA, uriA, uriA );
		sc.commit();
		after = countStatements( sc.getStatements( null, uriA, uriA, false ) );
		assertEquals( 0, before );
		assertEquals( 1, after );
		// Add plain literal statement to the default context.
		sc.removeStatements( null, null, plainLitA );
		sc.commit();
		before = countStatements( sc.getStatements( null, uriA, plainLitA,
		    false ) );
		sc.addStatement( uriA, uriA, plainLitA );
		sc.addStatement( uriA, uriB, plainLitA );
		sc.addStatement( uriB, uriB, plainLitA );
		sc.commit();
		after = countStatements( sc
		    .getStatements( null, uriA, plainLitA, false ) );
		assertEquals( 0, before );
		assertEquals( 1, after );
		// Add string-typed literal statement to the default context.
		sc.removeStatements( null, null, markoName );
		sc.commit();
		before = countStatements( sc.getStatements( null, firstName, markoName,
		    false ) );
		sc.addStatement( marko, firstName, markoName );
		sc.commit();
		after = countStatements( sc.getStatements( null, firstName, markoName,
		    false ) );
		assertEquals( 0, before );
		assertEquals( 1, after );
		assertEquals( marko, toSet(
		    sc.getStatements( null, firstName, markoName, false ) ).iterator()
		    .next().getSubject() );
		sc.close();
	}

	@Test
	public void testGetStatementsSPO_G() throws Exception
	{
		boolean includeInferred = false;
		SailConnection sc = sail.getConnection();
		URI uriA = sail.getValueFactory().createURI(
		    "http://example.org/test/S_POG#a" );
		URI uriB = sail.getValueFactory().createURI(
		    "http://example.org/test/S_POG#b" );
		URI uriC = sail.getValueFactory().createURI(
		    "http://example.org/test/S_POG#c" );
		URI uriD = sail.getValueFactory().createURI(
		    "http://example.org/test/S_POG#d" );
		int before, after;
		// default context, different S,P,O
		sc.removeStatements( uriA, null, null );
		sc.commit();
		before = countStatements( sc.getStatements( uriA, uriB, uriC,
		    includeInferred ) );
		sc.addStatement( uriA, uriB, uriC );
		sc.commit();
		after = countStatements( sc.getStatements( uriA, uriB, uriC,
		    includeInferred ) );
		assertEquals( 0, before );
		assertEquals( 1, after );
		// TODO: this should pass
		/*
		// default context, same S,P,O
		sc.removeStatements(uriA, null, null);
		sc.commit();
		before = countStatements(sc.getStatements(uriA, uriA, uriA, includeInferred));
		sc.addStatement(uriA, uriA, uriA);
		sc.commit();
		after = countStatements(sc.getStatements(uriA, uriA, uriA, includeInferred));
		assertEquals(0, before);
		assertEquals(1, after);
		//*/
		// one specific context, different S,P,O
		sc.removeStatements( uriA, null, null, uriD );
		sc.commit();
		before = countStatements( sc.getStatements( uriA, uriB, uriC,
		    includeInferred, uriD ) );
		sc.addStatement( uriA, uriB, uriC, uriD );
		sc.commit();
		after = countStatements( sc.getStatements( uriA, uriB, uriC,
		    includeInferred, uriD ) );
		assertEquals( 0, before );
		assertEquals( 1, after );
		// one specific context, same S,P,O,G
		sc.removeStatements( uriA, null, null, uriA );
		sc.commit();
		before = countStatements( sc.getStatements( uriA, uriA, uriA,
		    includeInferred, uriA ) );
		sc.addStatement( uriA, uriA, uriA, uriA );
		sc.commit();
		after = countStatements( sc.getStatements( uriA, uriA, uriA,
		    includeInferred, uriA ) );
		assertEquals( 0, before );
		assertEquals( 1, after );
		sc.close();
	}

	@Test
	public void testGetStatementsP_SOG() throws Exception
	{
		SailConnection sc = sail.getConnection();
		URI uriA = sail.getValueFactory().createURI(
		    "http://example.org/test/P_SOG#a" );
		URI uriB = sail.getValueFactory().createURI(
		    "http://example.org/test/P_SOG#b" );
		URI uriC = sail.getValueFactory().createURI(
		    "http://example.org/test/P_SOG#c" );
		URI marko = sail.getValueFactory().createURI(
		    "http://knowledgereefsystems.com/thing/q" );
		URI firstName = sail.getValueFactory().createURI(
		    "http://knowledgereefsystems.com/2007/11/core#firstName" );
		Literal plainLitA = sail.getValueFactory().createLiteral(
		    "arbitrary plain literal 238445" );
		Literal markoName = sail.getValueFactory().createLiteral( "Marko",
		    XMLSchema.STRING );
		int before, after;
		// Add statement to the implicit null context.
		sc.removeStatements( null, uriA, null );
		sc.commit();
		before = countStatements( sc.getStatements( null, uriA, null, false ) );
		sc.addStatement( uriA, uriA, uriA );
		sc.commit();
		after = countStatements( sc.getStatements( null, uriA, null, false ) );
		assertEquals( 0, before );
		assertEquals( 1, after );
		// Add plain literal statement to the default context.
		sc.removeStatements( null, uriA, null );
		sc.commit();
		before = countStatements( sc.getStatements( null, uriA, null, false ) );
		sc.addStatement( uriA, uriA, plainLitA );
		sc.addStatement( uriA, uriB, plainLitA );
		sc.addStatement( uriB, uriB, plainLitA );
		sc.commit();
		after = countStatements( sc.getStatements( null, uriA, null, false ) );
		assertEquals( 0, before );
		assertEquals( 1, after );
		// Add string-typed literal statement to the default context.
		sc.removeStatements( null, firstName, null );
		sc.commit();
		before = countStatements( sc.getStatements( null, firstName, null,
		    false ) );
		sc.addStatement( marko, firstName, markoName );
		sc.commit();
		after = countStatements( sc
		    .getStatements( null, firstName, null, false ) );
		assertEquals( 0, before );
		assertEquals( 1, after );
		assertEquals( marko, toSet(
		    sc.getStatements( null, firstName, null, false ) ).iterator()
		    .next().getSubject() );
		// Add statement to a non-null context.
		sc.removeStatements( null, uriA, null );
		sc.commit();
		before = countStatements( sc.getStatements( null, uriA, null, false ) );
		sc.addStatement( uriA, uriA, uriA, uriA );
		sc.commit();
		after = countStatements( sc.getStatements( null, uriA, null, false ) );
		assertEquals( 0, before );
		assertEquals( 1, after );
		/*
		sc.removeStatements(null, uriA, null);
		sc.commit();
		before = countStatements(sc.getStatements(null, uriA, null, false));
		sc.addStatement(uriB, uriA, uriC, uriC);
		sc.addStatement(uriC, uriA, uriA, uriA);
		sc.commit();
		sc.addStatement(uriA, uriA, uriB, uriB);
		sc.commit();
		after = countStatements(sc.getStatements(null, uriA, null, false));
		assertEquals(0, before);
		assertEquals(3, after);
		*/
		sc.close();
	}

	@Test
	public void testGetStatementsWithVariableContexts() throws Exception
	{
		SailConnection sc = sail.getConnection();
		URI uriA = sail.getValueFactory().createURI( "http://example.org/uriA" );
		boolean includeInferred = false;
		int count;
		sc.removeStatements( uriA, uriA, uriA );
		sc.commit();
		Resource[] contexts = { uriA, null };
		sc.addStatement( uriA, uriA, uriA, contexts );
		sc.commit();
		// Get statements from all contexts.
		count = countStatements( sc.getStatements( uriA, null, null,
		    includeInferred ) );
		assertEquals( 2, count );
		// Get statements from a specific named context.
		count = countStatements( sc.getStatements( null, null, null,
		    includeInferred, uriA ) );
		assertEquals( 1, count );
		// Get statements from the null context.
		Resource[] c = { null };
		count = countStatements( sc.getStatements( null, null, null,
		    includeInferred, c ) );
		assertTrue( count > 0 );
		int countLast = count;
		// Get statements from more than one context.
		count = countStatements( sc.getStatements( null, null, null,
		    includeInferred, contexts ) );
		assertEquals( 1 + countLast, count );
		// Test inference
		// TODO: inference not supported right now
		// URI instance1 = sail.getValueFactory().createURI(
		// "urn:org.neo4j.rdf.sail.test/instance1");
		// count = countStatements(
		// sc.getStatements(instance1, RDF.TYPE, null, false));
		// assertEquals(1, count);
		// count = countStatements(
		// sc.getStatements(instance1, RDF.TYPE, null, true));
		// assertEquals(2, count);
		sc.close();
	}

	@Test
	public void testRemoveStatements() throws Exception
	{
		SailConnection sc = sail.getConnection();
		URI uriA = sail.getValueFactory().createURI( "http://example.org/uriA" );
		Resource[] contexts = { uriA, null };
		boolean includeInferred = false;
		int count;
		// Remove from all contexts.
		sc.removeStatements( uriA, null, null );
		sc.commit();
		count = countStatements( sc.getStatements( uriA, null, null,
		    includeInferred ) );
		assertEquals( 0, count );
		sc.addStatement( uriA, uriA, uriA, contexts );
		sc.commit();
		count = countStatements( sc.getStatements( uriA, null, null,
		    includeInferred ) );
		assertEquals( 2, count );
		sc.removeStatements( uriA, null, null );
		sc.commit();
		count = countStatements( sc.getStatements( uriA, null, null,
		    includeInferred ) );
		assertEquals( 0, count );
		// Remove from one named context.
		sc.removeStatements( uriA, null, null );
		sc.commit();
		count = countStatements( sc.getStatements( uriA, null, null,
		    includeInferred ) );
		assertEquals( 0, count );
		sc.addStatement( uriA, uriA, uriA, contexts );
		sc.commit();
		count = countStatements( sc.getStatements( uriA, null, null,
		    includeInferred ) );
		assertEquals( 2, count );
		Resource[] oneContext = { uriA };
		sc.removeStatements( uriA, null, null, oneContext );
		sc.commit();
		count = countStatements( sc.getStatements( uriA, null, null,
		    includeInferred ) );
		assertEquals( 1, count );
		// Remove from the null context.
		sc.removeStatements( uriA, null, null );
		sc.commit();
		count = countStatements( sc.getStatements( uriA, null, null,
		    includeInferred ) );
		assertEquals( 0, count );
		sc.addStatement( uriA, uriA, uriA, contexts );
		sc.commit();
		count = countStatements( sc.getStatements( uriA, null, null,
		    includeInferred ) );
		assertEquals( 2, count );
		Resource[] nullContext = { null };
		sc.removeStatements( uriA, null, null, nullContext );
		sc.commit();
		count = countStatements( sc.getStatements( uriA, null, null,
		    includeInferred ) );
		assertEquals( 1, count );
		// Remove from more than one context.
		sc.removeStatements( uriA, null, null );
		sc.commit();
		count = countStatements( sc.getStatements( uriA, null, null,
		    includeInferred ) );
		assertEquals( 0, count );
		sc.addStatement( uriA, uriA, uriA, contexts );
		sc.commit();
		count = countStatements( sc.getStatements( uriA, null, null,
		    includeInferred ) );
		assertEquals( 2, count );
		sc.removeStatements( uriA, null, null );
		sc.commit();
		count = countStatements( sc.getStatements( uriA, null, null,
		    includeInferred, contexts ) );
		assertEquals( 0, count );
		sc.close();
	}

	@Test
	public void testClear() throws Exception
	{
		SailConnection sc = null;
		try
		{
			URI uriA = sail.getValueFactory().createURI(
			    "http://example.org/uriA" );
			URI uriB = sail.getValueFactory().createURI(
			    "http://example.org/uriB" );
			URI uriC = sail.getValueFactory().createURI(
			    "http://example.org/uriC" );
			sc = sail.getConnection();
			sc.clear();
			assertEquals( 0L, sc.size() );
			sc.addStatement( uriA, uriB, uriA, uriA );
			sc.addStatement( uriA, uriB, uriB, uriA );
			sc.addStatement( uriA, uriB, uriC, uriA );
			assertEquals( 3L, sc.size( uriA ) );
			sc.addStatement( uriA, uriB, uriA, uriB );
			sc.addStatement( uriA, uriB, uriC, uriB );
			assertEquals( 2L, sc.size( uriB ) );
			sc.addStatement( uriA, uriB, uriC );
			assertEquals( 1L, sc.size( null ) );
			sc.addStatement( uriA, uriB, uriA, uriC );
			sc.addStatement( uriA, uriB, uriB, uriC );
			sc.addStatement( uriA, uriB, uriC, uriC );
			sc.addStatement( uriC, uriB, uriC, uriC );
			assertEquals( 4L, sc.size( uriC ) );
			assertEquals( 10L, sc.size() );
			sc.clear( uriA, uriC );
			assertEquals( 1L, sc.size( null ) );
			assertEquals( 0L, sc.size( uriA ) );
			assertEquals( 2L, sc.size( uriB ) );
			assertEquals( 0L, sc.size( uriC ) );
			assertEquals( 3L, sc.size() );
			sc.clear();
			assertEquals( 0L, sc.size() );
		}
		finally
		{
			sc.close();
		}
	}

	@Test
	public void testGetContextIDs() throws Exception
	{
		// TODO
	}

	@Test
	public void testSize() throws Exception
	{
		URI uriA = sail.getValueFactory().createURI( "http://example.org/uriA" );
		URI uriB = sail.getValueFactory().createURI( "http://example.org/uriB" );
		URI uriC = sail.getValueFactory().createURI( "http://example.org/uriC" );
		SailConnection sc = sail.getConnection();
		sc.removeStatements( null, null, null );
		try
		{
			assertEquals( 0L, sc.size() );
			sc.addStatement( uriA, uriA, uriA, uriA );
			// sc.commit();
			assertEquals( 1L, sc.size() );
			sc.addStatement( uriA, uriA, uriA, uriB );
			// sc.commit();
			assertEquals( 2L, sc.size() );
			sc.addStatement( uriB, uriB, uriB, uriB );
			// sc.commit();
			assertEquals( 3L, sc.size() );
			sc.addStatement( uriC, uriC, uriC );
			// sc.commit();
			assertEquals( 4L, sc.size() );
			assertEquals( 1L, sc.size( uriA ) );
			assertEquals( 2L, sc.size( uriB ) );
			assertEquals( 0L, sc.size( uriC ) );
			assertEquals( 1L, sc.size( null ) );
			assertEquals( 3L, sc.size( uriB, null ) );
			assertEquals( 3L, sc.size( uriB, uriC, null ) );
			assertEquals( 4L, sc.size( uriA, uriB, null ) );
			assertEquals( 4L, sc.size( uriA, uriB, uriC, null ) );
			assertEquals( 3L, sc.size( uriA, uriB ) );
		}
		finally
		{
			sc.close();
		}
	}

	// URIs ////////////////////////////////////////////////////////////////////
	// literals ////////////////////////////////////////////////////////////////
	// Note: this test will always pass as long as we're using ValueFactoryImpl
	@Test
	public void testCreateLiteralsThroughValueFactory() throws Exception
	{
		Literal l;
		ValueFactory vf = sail.getValueFactory();
		l = vf.createLiteral( "a plain literal" );
		assertNotNull( l );
		assertNull( l.getLanguage() );
		assertEquals( "a plain literal", l.getLabel() );
		assertNull( l.getDatatype() );
		l = vf.createLiteral( "auf Deutsch, bitte", "de" );
		assertNotNull( l );
		assertEquals( "de", l.getLanguage() );
		assertEquals( "auf Deutsch, bitte", l.getLabel() );
		assertNull( l.getDatatype() );
		// Test data-typed createLiteral methods
		l = vf.createLiteral( "foo", XMLSchema.STRING );
		assertNotNull( l );
		assertNull( l.getLanguage() );
		assertEquals( "foo", l.getLabel() );
		assertEquals( XMLSchema.STRING, l.getDatatype() );
		l = vf.createLiteral( 42 );
		assertNotNull( l );
		assertNull( l.getLanguage() );
		assertEquals( "42", l.getLabel() );
		assertEquals( 42, l.intValue() );
		assertEquals( XMLSchema.INT, l.getDatatype() );
		l = vf.createLiteral( 42l );
		assertNotNull( l );
		assertNull( l.getLanguage() );
		assertEquals( "42", l.getLabel() );
		assertEquals( 42l, l.longValue() );
		assertEquals( XMLSchema.LONG, l.getDatatype() );
		l = vf.createLiteral( ( short ) 42 );
		assertNotNull( l );
		assertNull( l.getLanguage() );
		assertEquals( "42", l.getLabel() );
		assertEquals( ( short ) 42, l.shortValue() );
		assertEquals( XMLSchema.SHORT, l.getDatatype() );
		l = vf.createLiteral( true );
		assertNotNull( l );
		assertNull( l.getLanguage() );
		assertEquals( "true", l.getLabel() );
		assertEquals( true, l.booleanValue() );
		assertEquals( XMLSchema.BOOLEAN, l.getDatatype() );
		l = vf.createLiteral( ( byte ) 'c' );
		assertNotNull( l );
		assertNull( l.getLanguage() );
		assertEquals( "99", l.getLabel() );
		assertEquals( (byte)'c', l.byteValue() );
		assertEquals( XMLSchema.BYTE, l.getDatatype() );
		XMLGregorianCalendar calendar = XMLDatatypeUtil
		    .parseCalendar( "2002-10-10T12:00:00-05:00" );
		l = vf.createLiteral( calendar );
		assertNotNull( l );
		assertNull( l.getLanguage() );
		assertEquals( "2002-10-10T12:00:00-05:00", l.getLabel() );
		assertEquals( calendar, l.calendarValue() );
		assertEquals( XMLSchema.DATETIME, l.getDatatype() );
	}

	@Test
	public void testGetLiteralsFromTripleStore() throws Exception
	{
		Literal l;
		XMLGregorianCalendar calendar;
		ValueFactory vf = sail.getValueFactory();
		SailConnection sc = sail.getConnection();
		// Get an actual plain literal from the triple store.
		URI ford = vf.createURI( "urn:org.neo4j.rdf.sail.test/ford" );
		l = ( Literal ) toSet(
		    sc.getStatements( ford, RDFS.COMMENT, null, false ) ).iterator()
		    .next().getObject();
		assertNotNull( l );
		assertNull( l.getLanguage() );
		assertEquals( "he really knows where his towel is", l.getLabel() );
		assertNull( l.getDatatype() );
		URI thor = vf.createURI( "urn:org.neo4j.rdf.sail.test/thor" );
		// FIXME: restore this test when support for language tags has been
		// added
		// Get an actual language-tagged literal from the triple store.
		URI foafName = vf.createURI( "http://xmlns.com/foaf/0.1/name" );
		Iterator<Statement> iter = toSet(
		    sc.getStatements( thor, foafName, null, false ) ).iterator();
		boolean found = false;
		while ( iter.hasNext() )
		{
			l = ( Literal ) iter.next().getObject();
			if ( l.getLanguage().equals( "en" ) )
			{
				found = true;
				assertEquals( "Thor", l.getLabel() );
				assertNull( l.getDatatype() );
			}
			// if (l.getLanguage().equals("is")) {
			// found = true;
			// assertEquals("?�r", l.getLabel());
			// }
		}
		assertTrue( found );
		// Get an actual data-typed literal from the triple-store.
		URI msnChatID = vf.createURI( "http://xmlns.com/foaf/0.1/msnChatID" );
		l = ( Literal ) toSet( sc.getStatements( thor, msnChatID, null, false ) )
		    .iterator().next().getObject();
		assertNotNull( l );
		assertNull( l.getLanguage() );
		assertEquals( "Thorster123", l.getLabel() );
		assertEquals( XMLSchema.STRING, l.getDatatype() );
		// Test Literal.xxxValue() methods for Literals read from the triple
		// store
		URI valueUri, hasValueUri;
		hasValueUri = vf.createURI( "urn:org.neo4j.rdf.sail.test/hasValue" );
		valueUri = vf.createURI( "urn:org.neo4j.rdf.sail.test/stringValue" );
		l = ( Literal ) toSet(
		    sc.getStatements( valueUri, hasValueUri, null, false ) ).iterator()
		    .next().getObject();
		assertNotNull( l );
		assertNull( l.getLanguage() );
		assertEquals( "foo", l.getLabel() );
		assertEquals( XMLSchema.STRING, l.getDatatype() );
		valueUri = vf.createURI( "urn:org.neo4j.rdf.sail.test/byteValue" );
		l = ( Literal ) toSet(
		    sc.getStatements( valueUri, hasValueUri, null, false ) ).iterator()
		    .next().getObject();
		assertNotNull( l );
		assertNull( l.getLanguage() );
		assertEquals( "99", l.getLabel() );
		assertEquals( XMLSchema.BYTE, l.getDatatype() );
		assertEquals( (byte)'c', l.byteValue() );
		valueUri = vf.createURI( "urn:org.neo4j.rdf.sail.test/booleanValue" );
		l = ( Literal ) toSet(
		    sc.getStatements( valueUri, hasValueUri, null, false ) ).iterator()
		    .next().getObject();
		assertNotNull( l );
		assertNull( l.getLanguage() );
		assertEquals( "false", l.getLabel() );
		assertEquals( XMLSchema.BOOLEAN, l.getDatatype() );
		assertEquals( false, l.booleanValue() );
		valueUri = vf.createURI( "urn:org.neo4j.rdf.sail.test/intValue" );
		l = ( Literal ) toSet(
		    sc.getStatements( valueUri, hasValueUri, null, false ) ).iterator()
		    .next().getObject();
		assertNotNull( l );
		assertNull( l.getLanguage() );
		assertEquals( "42", l.getLabel() );
		assertEquals( XMLSchema.INT, l.getDatatype() );
		assertEquals( 42, l.intValue() );
		valueUri = vf.createURI( "urn:org.neo4j.rdf.sail.test/shortValue" );
		l = ( Literal ) toSet(
		    sc.getStatements( valueUri, hasValueUri, null, false ) ).iterator()
		    .next().getObject();
		assertNotNull( l );
		assertNull( l.getLanguage() );
		assertEquals( "42", l.getLabel() );
		assertEquals( XMLSchema.SHORT, l.getDatatype() );
		assertEquals( ( short ) 42, l.shortValue() );
		valueUri = vf.createURI( "urn:org.neo4j.rdf.sail.test/longValue" );
		l = ( Literal ) toSet(
		    sc.getStatements( valueUri, hasValueUri, null, false ) ).iterator()
		    .next().getObject();
		assertNotNull( l );
		assertNull( l.getLanguage() );
		assertEquals( "42", l.getLabel() );
		assertEquals( XMLSchema.LONG, l.getDatatype() );
		assertEquals( 42l, l.longValue() );
		valueUri = vf.createURI( "urn:org.neo4j.rdf.sail.test/floatValue" );
		l = ( Literal ) toSet(
		    sc.getStatements( valueUri, hasValueUri, null, false ) ).iterator()
		    .next().getObject();
		assertNotNull( l );
		assertNull( l.getLanguage() );
		assertEquals( "3.1415926", l.getLabel() );
		assertEquals( XMLSchema.FLOAT, l.getDatatype() );
		assertEquals( ( float ) 3.1415926, l.floatValue() );
		valueUri = vf.createURI( "urn:org.neo4j.rdf.sail.test/doubleValue" );
		l = ( Literal ) toSet(
		    sc.getStatements( valueUri, hasValueUri, null, false ) ).iterator()
		    .next().getObject();
		assertNotNull( l );
		assertNull( l.getLanguage() );
		assertEquals( "3.1415926", l.getLabel() );
		assertEquals( XMLSchema.DOUBLE, l.getDatatype() );
		assertEquals( 3.1415926, l.doubleValue() );
		valueUri = vf.createURI( "urn:org.neo4j.rdf.sail.test/dateTimeValue" );
		calendar = XMLDatatypeUtil.parseCalendar( "2002-10-10T12:00:00-05:00" );
		l = ( Literal ) toSet(
		    sc.getStatements( valueUri, hasValueUri, null, false ) ).iterator()
		    .next().getObject();
		assertNotNull( l );
		assertNull( l.getLanguage() );
		assertEquals( "2002-10-10T12:00:00-05:00", l.getLabel() );
		assertEquals( XMLSchema.DATETIME, l.getDatatype() );
		assertEquals( calendar, l.calendarValue() );
		sc.close();
	}

	// blank nodes /////////////////////////////////////////////////////////////
	@Test
	public void testBlankNodes() throws Exception
	{
		System.out.println( "who needs blank nodes?" );
	}

	// tuple queries ///////////////////////////////////////////////////////////
	@Test
	public void testEvaluate() throws Exception
	{
		Set<String> languages;
		SailConnection sc = sail.getConnection();
		URI uriA = sail.getValueFactory().createURI( "http://example.org/uriA" );
		sc.addStatement( uriA, uriA, uriA );
		sc.commit();
		SPARQLParser parser = new SPARQLParser();
		BindingSet bindings = new EmptyBindingSet();
		String baseURI = "http://example.org/bogus/";
		String queryStr;
		ParsedQuery query;
		CloseableIteration<? extends BindingSet, QueryEvaluationException> results;
		int count;
		// s ?p ?o SELECT
		queryStr = "SELECT ?y ?z WHERE { <http://example.org/uriA> ?y ?z }";
		query = parser.parseQuery( queryStr, baseURI );
		results = sc.evaluate( query.getTupleExpr(), query.getDataset(),
		    bindings, false );
		count = 0;
		while ( results.hasNext() )
		{
			count++;
			BindingSet set = results.next();
			URI y = ( URI ) set.getValue( "y" );
			Value z = ( Value ) set.getValue( "z" );
			assertNotNull( y );
			assertNotNull( z );
			// System.out.println("y = " + y + ", z = " + z);
		}
		results.close();
		assertTrue( count > 0 );
		// s p ?o SELECT using a namespace prefix
		// TODO: commented out languages for now
		/*
		queryStr = "PREFIX foaf: <http://xmlns.com/foaf/0.1/>\n"
		        + "SELECT ?z WHERE { <urn:org.neo4j.rdf.sail.test/thor> foaf:name ?z }";
		query = parser.parseQuery(queryStr, baseURI);
		results = sc.evaluate(query.getTupleExpr(), query.getDataset(), bindings, false);
		count = 0;
		languages = new HashSet<String>();
		while (results.hasNext()) {
		    count++;
		    BindingSet set = results.next();
		    Literal z = (Literal) set.getValue("z");
		    assertNotNull(z);
		    languages.add(z.getLanguage());
		}
		results.close();
		assertTrue(count > 0);
		assertEquals(2, languages.size());
		assertTrue(languages.contains("en"));
		assertTrue(languages.contains("is"));   */
		// ?s p o SELECT using a plain literal value with no language tag
		queryStr = "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n"
		    + "SELECT ?s WHERE { ?s rdfs:comment \"he really knows where his towel is\" }";
		URI fordUri = sail.getValueFactory().createURI(
		    "urn:org.neo4j.rdf.sail.test/ford" );
		query = parser.parseQuery( queryStr, baseURI );
		results = sc.evaluate( query.getTupleExpr(), query.getDataset(),
		    bindings, false );
		count = 0;
		while ( results.hasNext() )
		{
			count++;
			BindingSet set = results.next();
			URI s = ( URI ) set.getValue( "s" );
			assertNotNull( s );
			assertEquals( s, fordUri );
		}
		results.close();
		assertTrue( count > 0 );
		// ?s p o SELECT using a language-specific literal value
		// TODO: commented out data type <-> non-data type literal equivalence
		// problems
		// queryStr = "PREFIX foaf: <http://xmlns.com/foaf/0.1/>\n"
		// + "SELECT ?s WHERE { ?s foaf:name \"Thor\"@en }";
		// URI thorUri =
		// sail.getValueFactory().createURI("urn:org.neo4j.rdf.sail.test/thor");
		// query = parser.parseQuery(queryStr, baseURI);
		// results = sc.evaluate(query.getTupleExpr(), query.getDataset(),
		// bindings, false);
		// count = 0;
		// while (results.hasNext()) {
		// count++;
		// BindingSet set = results.next();
		// URI s = (URI) set.getValue("s");
		// assertNotNull(s);
		// assertEquals(s, thorUri);
		// }
		// results.close();
		// assertTrue(count > 0);
		// // The language tag is necessary
		// queryStr = "PREFIX foaf: <http://xmlns.com/foaf/0.1/>\n"
		// + "SELECT ?s WHERE { ?s foaf:name \"Thor\" }";
		// query = parser.parseQuery(queryStr, baseURI);
		// results = sc.evaluate(query.getTupleExpr(), query.getDataset(),
		// bindings, false);
		// count = 0;
		// while (results.hasNext()) {
		// count++;
		// results.next();
		// }
		// results.close();
		// assertEquals(0, count);
		// ?s p o SELECT using a typed literal value
		// TODO: commented out data type <-> non-data type literal equivalence
		// problems
		// queryStr = "PREFIX foaf: <http://xmlns.com/foaf/0.1/>\n"
		// + "PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>\n"
		// +
		// "SELECT ?s WHERE { ?s foaf:msnChatID \"Thorster123\"^^xsd:string }";
		// query = parser.parseQuery(queryStr, baseURI);
		// results = sc.evaluate(query.getTupleExpr(), query.getDataset(),
		// bindings, false);
		// count = 0;
		// while (results.hasNext()) {
		// count++;
		// BindingSet set = results.next();
		// URI s = (URI) set.getValue("s");
		// assertNotNull(s);
		// assertEquals(s, thorUri);
		// }
		// results.close();
		// assertTrue(count > 0);
		// // The data type is necessary
		// queryStr = "PREFIX foaf: <http://xmlns.com/foaf/0.1/>\n"
		// + "PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>\n"
		// + "SELECT ?s WHERE { ?s foaf:msnChatID \"Thorster123\" }";
		// query = parser.parseQuery(queryStr, baseURI);
		// results = sc.evaluate(query.getTupleExpr(), query.getDataset(),
		// bindings, false);
		// count = 0;
		// while (results.hasNext()) {
		// count++;
		// results.next();
		// }
		// results.close();
		// assertEquals(0, count);
		// s ?p o SELECT
		// TODO: commented out languages for now
		queryStr = "PREFIX foaf: <http://xmlns.com/foaf/0.1/>\n"
		    + "PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>\n"
		    + "SELECT ?p WHERE { <urn:org.neo4j.rdf.sail.test/thor> ?p \"Thor\"@en }";
		query = parser.parseQuery( queryStr, baseURI );
		URI foafNameUri = sail.getValueFactory().createURI(
		    "http://xmlns.com/foaf/0.1/name" );
		results = sc.evaluate( query.getTupleExpr(), query.getDataset(),
		    bindings, false );
		count = 0;
		while ( results.hasNext() )
		{
			count++;
			BindingSet set = results.next();
			URI p = ( URI ) set.getValue( "p" );
			assertNotNull( p );
			assertEquals( p, foafNameUri );
		}
		results.close();
		assertTrue( count > 0 );
		// context-specific SELECT
		queryStr = "PREFIX foaf: <http://xmlns.com/foaf/0.1/>\n"
		    + "SELECT ?z\n" + "FROM <urn:org.neo4j.rdf.sail.test/ctx1>\n"
		    + "WHERE { <urn:org.neo4j.rdf.sail.test/thor> foaf:name ?z }";
		query = parser.parseQuery( queryStr, baseURI );
		results = sc.evaluate( query.getTupleExpr(), query.getDataset(),
		    bindings, false );
		count = 0;
		languages = new HashSet<String>();
		while ( results.hasNext() )
		{
			count++;
			BindingSet set = results.next();
			Literal z = ( Literal ) set.getValue( "z" );
			assertNotNull( z );
			languages.add( z.getLanguage() );
		}
		results.close();
		assertTrue( count > 0 );
		assertEquals( 2, languages.size() );
		assertTrue( languages.contains( "en" ) );
		assertTrue( languages.contains( "is" ) );
		queryStr = "PREFIX foaf: <http://xmlns.com/foaf/0.1/>\n"
		    + "SELECT ?z\n" + "FROM <http://example.org/emptycontext>\n"
		    + "WHERE { <urn:org.neo4j.rdf.sail.test/thor> foaf:name ?z }";
		query = parser.parseQuery( queryStr, baseURI );
		results = sc.evaluate( query.getTupleExpr(), query.getDataset(),
		    bindings, false );
		count = 0;
		while ( results.hasNext() )
		{
			count++;
			results.next();
		}
		results.close();
		assertEquals( 0, count );
		// s p o? select without and with inferencing
		// TODO commented out waiting for inferencing
		// queryStr =
		// "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n"
		// + "SELECT ?o\n"
		// + "WHERE { <urn:org.neo4j.rdf.sail.test/instance1> rdf:type ?o }";
		// query = parser.parseQuery(queryStr, baseURI);
		// results = sc.evaluate(query.getTupleExpr(), query.getDataset(),
		// bindings, false);
		// count = 0;
		// while (results.hasNext()) {
		// count++;
		// BindingSet set = results.next();
		// URI o = (URI) set.getValue("o");
		// assertEquals("urn:org.neo4j.rdf.sail.test/classB", o.toString());
		// }
		// results.close();
		// assertEquals(1, count);
		// results = sc.evaluate(query.getTupleExpr(), query.getDataset(),
		// bindings, true);
		// count = 0;
		// boolean foundA = false, foundB = false;
		// while (results.hasNext()) {
		// count++;
		// BindingSet set = results.next();
		// URI o = (URI) set.getValue("o");
		// String s = o.toString();
		// if (s.equals("urn:org.neo4j.rdf.sail.test/classA")) {
		// foundA = true;
		// } else if (s.equals("urn:org.neo4j.rdf.sail.test/classB")) {
		// foundB = true;
		// }
		// }
		// results.close();
		// assertEquals(2, count);
		// assertTrue(foundA);
		// assertTrue(foundB);
		sc.close();
	}

	// listeners ///////////////////////////////////////////////////////////////
	@Test
	public void testSailConnectionListeners() throws Exception
	{
		TestListener listener1 = new TestListener(), listener2 = new TestListener();
		SailConnection sc = sail.getConnection();
		sc.addConnectionListener( listener1 );
		URI uriA = sail.getValueFactory().createURI( "http://example.org/uriA" );
		sc.removeStatements( null, null, null, uriA );
		sc.commit();
		sc.addConnectionListener( listener2 );
		sc.addStatement( uriA, uriA, uriA, uriA );
		sc.commit();
		// TODO: listening on removal is not yet supported
		// assertEquals(1, listener1.getRemoved());
		// assertEquals(0, listener2.getRemoved());
		assertEquals( 1, listener1.getAdded() );
		assertEquals( 1, listener2.getAdded() );
		sc.close();
	}

	@Test
	public void testSailChangedListeners() throws Exception
	{
		final Collection<SailChangedEvent> events = new LinkedList<SailChangedEvent>();
		SailChangedListener listener = new SailChangedListener()
		{
			public void sailChanged( final SailChangedEvent event )
			{
				events.add( event );
			}
		};
		sail().addSailChangedListener( listener );
		URI uriA = sail.getValueFactory().createURI( "http://example.org/uriA" );
		SailConnection sc = sail.getConnection();
		assertEquals( 0, events.size() );
		sc.addStatement( uriA, uriA, uriA, uriA );
		sc.commit();
		assertEquals( 1, events.size() );
		SailChangedEvent event = events.iterator().next();
		assertTrue( event.statementsAdded() );
		assertFalse( event.statementsRemoved() );
		events.clear();
		assertEquals( 0, events.size() );
		sc.removeStatements( uriA, uriA, uriA, uriA );
		sc.commit();
		assertEquals( 1, events.size() );
		event = events.iterator().next();
		assertFalse( event.statementsAdded() );
		assertTrue( event.statementsRemoved() );
		sc.close();
	}

	// namespaces //////////////////////////////////////////////////////////////
	@Test
	public void testClearNamespaces() throws Exception
	{
		SailConnection sc = sail.getConnection();
		CloseableIteration<? extends Namespace, SailException> namespaces;
		int count;
		count = 0;
		namespaces = sc.getNamespaces();
		while ( namespaces.hasNext() )
		{
			namespaces.next();
			count++;
		}
		namespaces.close();
		assertTrue( count > 0 );
		// TODO: actually clear namespaces (but this wipes them out for
		// subsequent tests)
		sc.close();
	}

	@Test
	public void testGetNamespace() throws Exception
	{
		SailConnection sc = sail.getConnection();
		String name;
		name = sc.getNamespace( "rdf" );
		assertNull( name );
		// assertEquals(name, "http://www.w3.org/1999/02/22-rdf-syntax-ns#");
		name = sc.getNamespace( "rdfs" );
		sc.commit();
		assertEquals( name, "http://www.w3.org/2000/01/rdf-schema#" );
		sc.close();
	}

	@Test
	public void testGetNamespaces() throws Exception
	{
		SailConnection sc = sail.getConnection();
		CloseableIteration<? extends Namespace, SailException> namespaces;
		int before = 0, during = 0, after = 0;
		// just iterate through all namespaces
		namespaces = sc.getNamespaces();
		while ( namespaces.hasNext() )
		{
			Namespace ns = namespaces.next();
			before++;
			// System.out.println("namespace: " + ns);
		}
		namespaces.close();
		// Note: assumes that these namespace prefixes are unused.
		int nTests = 10;
		String prefixPrefix = "testns";
		String namePrefix = "http://example.org/test";
		for ( int i = 0; i < nTests; i++ )
		{
			sc.setNamespace( prefixPrefix + i, namePrefix + i );
		}
		sc.commit();
		namespaces = sc.getNamespaces();
		while ( namespaces.hasNext() )
		{
			Namespace ns = namespaces.next();
			during++;
			String prefix = ns.getPrefix();
			String name = ns.getName();
			if ( prefix.startsWith( prefixPrefix ) )
			{
				assertEquals( name, namePrefix
				    + prefix.substring( prefixPrefix.length() ) );
			}
		}
		namespaces.close();
		for ( int i = 0; i < nTests; i++ )
		{
			sc.removeNamespace( prefixPrefix + i );
		}
		sc.commit();
		namespaces = sc.getNamespaces();
		while ( namespaces.hasNext() )
		{
			namespaces.next();
			after++;
		}
		namespaces.close();
		assertEquals( during, before + nTests );
		assertEquals( after, before );
		sc.close();
	}

	@Test
	public void testSetNamespace() throws Exception
	{
		SailConnection sc = sail.getConnection();
		String prefix = "foo";
		String emptyPrefix = "";
		String name = "http://example.org/foo";
		String otherName = "http://example.org/bar";
		sc.removeNamespace( prefix );
		sc.removeNamespace( emptyPrefix );
		sc.commit();
		// Namespace initially absent?
		assertNull( sc.getNamespace( prefix ) );
		assertNull( sc.getNamespace( emptyPrefix ) );
		// Can we set the namespace?
		sc.setNamespace( prefix, name );
		sc.commit();
		assertEquals( sc.getNamespace( prefix ), name );
		// Can we reset the namespace?
		sc.setNamespace( prefix, otherName );
		sc.commit();
		assertEquals( sc.getNamespace( prefix ), otherName );
		// Can we use an empty namespace prefix?
		sc.setNamespace( emptyPrefix, name );
		sc.commit();
		assertEquals( sc.getNamespace( emptyPrefix ), name );
		sc.close();
	}

	@Test
	public void testRemoveNamespace() throws Exception
	{
		SailConnection sc = sail.getConnection();
		String prefix = "foo";
		String emptyPrefix = "";
		String name = "http://example.org/foo";
		// Set namespace initially.
		sc.setNamespace( prefix, name );
		sc.commit();
		assertEquals( sc.getNamespace( prefix ), name );
		// Remove the namespace and make sure it's gone.
		sc.removeNamespace( prefix );
		sc.commit();
		assertNull( sc.getNamespace( prefix ) );
		// Same thing for the default namespace.
		sc.setNamespace( emptyPrefix, name );
		sc.commit();
		assertEquals( sc.getNamespace( emptyPrefix ), name );
		sc.removeNamespace( emptyPrefix );
		sc.commit();
		assertNull( sc.getNamespace( emptyPrefix ) );
		sc.close();
	}

	// connections and transactions ////////////////////////////////////////////
	@Test
	public void testPersistentCommits() throws Exception
	{
		SailConnection sc;
		int count;
		boolean includeInferred = false;
		URI uriA = sail.getValueFactory().createURI(
		    "http://example.org/test/persistentCommits#a" );
		sc = sail.getConnection();
		try
		{
			count = countStatements( sc.getStatements( uriA, null, null,
			    includeInferred ) );
			assertEquals( 0, count );
			sc.close();
			sc = sail.getConnection();
			sc.addStatement( uriA, uriA, uriA );
			count = countStatements( sc.getStatements( uriA, null, null,
			    includeInferred ) );
			assertEquals( 1, count );
			sc.commit();
			sc.close();
			sc = sail.getConnection();
			count = countStatements( sc.getStatements( uriA, null, null,
			    includeInferred ) );
			assertEquals( 1, count );
			sc.close();
			sc = sail.getConnection();
			sc.removeStatements( uriA, null, null );
			count = countStatements( sc.getStatements( uriA, null, null,
			    includeInferred ) );
			assertEquals( 0, count );
			sc.commit();
			sc.close();
			sc = sail.getConnection();
			count = countStatements( sc.getStatements( uriA, null, null,
			    includeInferred ) );
			assertEquals( 0, count );
		}
		finally
		{
			sc.close();
		}
	}
	
	@Test
	public void testFulltextSearch() throws Exception
	{
		SailConnection sc;
		int count;
		boolean includeInferred = false;
		URI uriA = sail.getValueFactory().createURI(
		    "http://example.org/test/fulltextSearch#a" );
		URI uriB = sail.getValueFactory().createURI(
			"http://example.org/test/fulltextSearch#b" );
		URI uriC = sail.getValueFactory().createURI(
			"http://example.org/test/fulltextSearch#c" );
		sc = sail.getConnection();
		try
		{
			count = countStatements( sc.getStatements( uriA, null, null,
			    includeInferred ) );
			assertEquals( 0, count );
			sc.close();
			sc = sail.getConnection();
			String text1 = "Lorem ipsum dolor sit amet, " +
				"consectetuer adipiscing elit. Nunc neque.";
			String text2 = "Lorem ipsum dolor sit amet. " +
				"Aliquam adipiscing sapien.";
			String text3 = "Integer et neque ut erat feugiat varius. " +
				"Aliquam adipiscing sapien.";
			sc.addStatement( uriA, uriB, sail.getValueFactory().createLiteral(
				text1 ) );
			sc.addStatement( uriA, uriB, sail.getValueFactory().createLiteral(
				text2 ) );
			sc.addStatement( uriA, uriC, sail.getValueFactory().createLiteral(
				text3 ) );
			sc.commit();
			sc.close();
			sc = sail.getConnection();
			
			// TODO Work-around for the fulltext index, which is asynchronous
			long time = System.currentTimeMillis();
			while ( System.currentTimeMillis() - time < 3000 &&
				countStatements( ( ( NeoRdfSailConnection ) sc ).evaluate(
					"integer" ) ) == 0 )
			{
				Thread.sleep( 100 );
			}
			
			count = countStatements( ( ( NeoRdfSailConnection ) sc ).evaluate(
				"Lorem ipsum" ) );
			assertEquals( 2, count );
			count = countStatements( ( ( NeoRdfSailConnection ) sc ).evaluate(
				"dolo*" ) );
			assertEquals( 2, count );
			count = countStatements( ( ( NeoRdfSailConnection ) sc ).evaluate(
				"aliq* integer" ) );
			assertEquals( 1, count );
			
			sc.removeStatements( uriA, null, null );
			sc.commit();
		}
		finally
		{
			sc.close();
		}
	}

	@Test
	public void testVisibilityOfChanges() throws Exception
	{
		SailConnection sc1, sc2;
		int count;
		boolean includeInferred = false;
		URI uriA = sail.getValueFactory().createURI(
		    "http://example.org/test/visibilityOfChanges#a" );
		sc1 = sail.getConnection();
		sc2 = sail.getConnection();
		try
		{
			// Statement doesn't exist for either connection.
			count = countStatements( sc1.getStatements( uriA, null, null,
			    includeInferred ) );
			assertEquals( 0, count );
			count = countStatements( sc2.getStatements( uriA, null, null,
			    includeInferred ) );
			assertEquals( 0, count );
			// First connection adds a statement. It is visible to the first
			// connection, but not to the second.
			sc1.addStatement( uriA, uriA, uriA );
			count = countStatements( sc1.getStatements( uriA, null, null,
			    includeInferred ) );
			assertEquals( 1, count );
			count = countStatements( sc2.getStatements( uriA, null, null,
			    includeInferred ) );
			assertEquals( 0, count );
			// ...
		}
		finally
		{
			sc2.close();
			sc1.close();
		}
	}
	
	@Test
	public void testMetadata() throws Exception
	{
	    SailConnection sc = sail.getConnection();
        URI uriA = sail.getValueFactory().createURI(
            "http://example.org/test/metadata#a" );
        URI uriB = sail.getValueFactory().createURI(
            "http://example.org/test/metadata#b" );
        URI uriC = sail.getValueFactory().createURI(
            "http://example.org/test/metadata#c" );
        URI uriD = sail.getValueFactory().createURI(
            "http://example.org/test/metadata#d" );
        URI uriE = sail.getValueFactory().createURI(
            "http://example.org/test/metadata#e" );
        int count;
        boolean includeInferred = false;
	    try
	    {
            count = countStatements( sc.getStatements( uriA, null, null,
                includeInferred ) );
            assertEquals( 0, count );
            sc.addStatement( uriA, uriB, uriC );
            count = countStatements( sc.getStatements( uriA, uriB, uriC,
                includeInferred ) );
            assertEquals( 1, count );
            CloseableIteration<? extends Statement, SailException> itr =
                sc.getStatements( uriA, uriB, uriC, includeInferred );
            Statement statement = itr.next();
            itr.close();
            
            ValueFactory vf = new ValueFactoryImpl();
            NeoRdfStatement neoRdfStatement = ( NeoRdfStatement ) statement;
            Literal value = vf.createLiteral( "One of those test values" );
            Literal value2 = vf.createLiteral( 12345d );
            Map<String, Literal> metadata = neoRdfStatement.getMetadata();
            assertEquals( 0, metadata.size() );
            metadata.put( uriD.stringValue(), value );
            metadata.put( uriE.stringValue(), value2 );
            
            ( ( NeoRdfSailConnection ) sc ).setStatementMetadata(
                neoRdfStatement, metadata );
            sc.commit();
            sc.close();
            sc = sail.getConnection();
            
            itr = sc.getStatements( uriA, uriB, uriC, includeInferred );
            statement = itr.next();
            itr.close();
            neoRdfStatement = ( NeoRdfStatement ) statement;
            metadata = neoRdfStatement.getMetadata();
            assertEquals( 2, metadata.size() );
            assertEquals( value, metadata.get( uriD.stringValue() ) );
            assertEquals( value2, metadata.get( uriE.stringValue() ) );
            metadata.remove( uriE.stringValue() );
            ( ( NeoRdfSailConnection ) sc ).setStatementMetadata(
                statement, metadata );
            sc.commit();
            sc.close();
            sc = sail.getConnection();
            
            itr = sc.getStatements( uriA, uriB, uriC, includeInferred );
            statement = itr.next();
            itr.close();
            neoRdfStatement = ( NeoRdfStatement ) statement;
            metadata = neoRdfStatement.getMetadata();
            
            assertEquals( 1, metadata.size() );
            assertEquals( value, metadata.get( uriD.stringValue() ) );
            
            sc.removeStatements( uriA, null, null );
            sc.commit();
        }
        finally
        {
            sc.close();
        }
    }
	
	@Test
	public void testAddWithMetadata() throws Exception
	{
	    NeoRdfSailConnection sc = ( NeoRdfSailConnection ) sail.getConnection();
        URI uriA = sail.getValueFactory().createURI(
            "http://example.org/test/metadata#a" );
        URI uriB = sail.getValueFactory().createURI(
            "http://example.org/test/metadata#b" );
        URI uriC = sail.getValueFactory().createURI(
            "http://example.org/test/metadata#c" );
        URI contextA = sail.getValueFactory().createURI(
            "http://example.org/test/metadataContext#a" );
        URI contextB = sail.getValueFactory().createURI(
            "http://example.org/test/metadataContext#b" );
        int count;
        boolean includeInferred = false;
	    try
	    {
            count = countStatements( sc.getStatements( uriA, null, null,
                includeInferred ) );
            assertEquals( 0, count );
            String metadataKey = "http://example.org/test/someExampleUri";
            ValueFactory vf = new ValueFactoryImpl();
            Literal valueA = vf.createLiteral( "Just a literal indeed" );
            Literal valueB = vf.createLiteral( false );
            Map<String, Literal> metadata = new HashMap<String, Literal>();
            metadata.put( metadataKey, valueA );
            Statement statement = sc.addStatement( metadata, uriA, uriB, uriC,
                contextA );
            NeoRdfStatement neoRdfStatement = ( NeoRdfStatement ) statement;
            assertEquals( valueA,
                neoRdfStatement.getMetadata().get( metadataKey ) );
            
            metadata.clear();
            metadata.put( metadataKey, valueB );
            statement = sc.addStatement( metadata, uriA, uriB, uriC, contextB );
            neoRdfStatement = ( NeoRdfStatement ) statement;
            assertEquals( valueB,
                neoRdfStatement.getMetadata().get( metadataKey ) );
            neoRdfStatement = ( NeoRdfStatement ) sc.getStatements( uriA, uriB,
                uriC, includeInferred, contextA ).next();
            assertEquals( valueA,
                neoRdfStatement.getMetadata().get( metadataKey ) );
            neoRdfStatement = ( NeoRdfStatement ) sc.getStatements( uriA, uriB,
                uriC, includeInferred, contextB ).next();
            assertEquals( valueB,
                neoRdfStatement.getMetadata().get( metadataKey ) );
            
            sc.removeStatements( uriA, null, null );
            sc.commit();
	    }
	    finally
	    {
	        sc.close();
	    }
	}
	
    @Test
    public void testNullContext() throws Exception
    {
        SailConnection sc = sail.getConnection();
        URI uriA = sail.getValueFactory().createURI(
            "http://example.org/test/nullContext#a" );
        URI uriB = sail.getValueFactory().createURI(
            "http://example.org/test/nullContext#b" );
        URI uriC = sail.getValueFactory().createURI(
            "http://example.org/test/nullContext#c" );
        int count = 0;
        boolean includeInferred = false;
        try
        {
            count = countStatements( sc.getStatements( uriA, null, null,
                includeInferred ) );
            assertEquals( 0, count );
            sc.addStatement( uriA, uriB, uriC );
            Statement statement = sc.getStatements( uriA, uriB, uriC,
                includeInferred, new Resource[] { null } ).next();
            Resource context = statement.getContext();
            assertNull( context );
            sc.removeStatements( uriA, null, null );
            sc.commit();
        }
        finally
        {
            sc.close();
        }
    }

	// TODO: concurrency testing ///////////////////////////////////////////////
	////////////////////////////////////////////////////////////////////////////
	private class TestListener implements SailConnectionListener
	{
		private int added = 0, removed = 0;

		public void statementAdded( final Statement statement )
		{
			added++;
		}

		public void statementRemoved( final Statement statement )
		{
			removed++;
		}

		public int getAdded()
		{
			return added;
		}

		public int getRemoved()
		{
			return removed;
		}
	}

	private int countStatements(
	    CloseableIteration<?, SailException> statements )
	    throws SailException
	{
		int count = 0;
		while ( statements.hasNext() )
		{
			statements.next();
			count++;
		}
		statements.close();
		return count;
	}

	private Set<Statement> toSet(
	    final CloseableIteration<? extends Statement, SailException> iter )
	    throws SailException
	{
		Set<Statement> set = new HashSet<Statement>();
		while ( iter.hasNext() )
		{
			set.add( iter.next() );
		}
		iter.close();
		return set;
	}
}