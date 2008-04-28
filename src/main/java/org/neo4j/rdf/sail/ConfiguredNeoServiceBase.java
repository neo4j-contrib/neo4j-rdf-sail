package org.neo4j.rdf.sail;

import java.io.Serializable;
import java.util.Collections;
import java.util.Map;

import org.neo4j.api.core.EmbeddedNeo;
import org.neo4j.api.core.NeoService;
import org.neo4j.api.core.Node;
import org.neo4j.api.core.Transaction;

public class ConfiguredNeoServiceBase implements NeoService
{
    private final NeoService neo;

    protected ConfiguredNeoServiceBase( String storeDir )
    {
        if ( getConfiguration().isEmpty() )
        {
            this.neo = new EmbeddedNeo( storeDir );
        }
        else
        {
            this.neo = new EmbeddedNeo( storeDir, getConfiguration() );
        }
    }
        
    protected Map<String, String> getConfiguration()
    {
        return Collections.emptyMap();
    }
    
    protected NeoService neo()
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

    public Node getReferenceNode()
    {
        return neo().getReferenceNode();
    }    
}
