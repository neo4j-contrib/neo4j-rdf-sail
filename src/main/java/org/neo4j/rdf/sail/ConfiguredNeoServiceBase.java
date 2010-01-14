package org.neo4j.rdf.sail;

import java.io.Serializable;
import java.util.Collections;
import java.util.Map;

import org.neo4j.kernel.EmbeddedGraphDatabase;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;

public class ConfiguredNeoServiceBase implements GraphDatabaseService
{
    private final GraphDatabaseService neo;

    protected ConfiguredNeoServiceBase( String storeDir )
    {
        if ( getConfiguration().isEmpty() )
        {
            this.neo = new EmbeddedGraphDatabase( storeDir );
        }
        else
        {
            this.neo = new EmbeddedGraphDatabase( storeDir, getConfiguration() );
        }
    }
        
    protected Map<String, String> getConfiguration()
    {
        return Collections.emptyMap();
    }
    
    protected GraphDatabaseService neo()
    {
        return this.neo;
    }
    
    public void shutdown()
    {
        neo().shutdown();
    }

    public Transaction beginTx()
    {
        return neo().beginTx();
    }

    public Node createNode()
    {
        return neo().createNode();
    }

    public boolean enableRemoteShell()
    {
        return neo().enableRemoteShell();
    }

    public boolean enableRemoteShell( Map<String, Serializable> params )
    {
        return neo().enableRemoteShell( params );
    }

    public Node getNodeById( long id )
    {
        return neo().getNodeById( id );
    }
    
    public Relationship getRelationshipById( long id )
    {
        return neo().getRelationshipById( id );
    }

    public Node getReferenceNode()
    {
        return neo().getReferenceNode();
    }

    public Iterable<RelationshipType> getRelationshipTypes()
    {
        return neo().getRelationshipTypes();
    }
    
    public Iterable<Node> getAllNodes()
    {
        return neo().getAllNodes();
    }

}
