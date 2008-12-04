package org.neo4j.rdf.sail;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.rdf.model.CompleteStatement;
import org.openrdf.model.Literal;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;

public class NeoRdfStatementImpl implements NeoRdfStatement, Serializable
{
    private Statement statement;
    private Map<String, Literal> metadata;
    
    public NeoRdfStatementImpl( Statement statement,
        CompleteStatement neoStatement )
    {
        this.statement = statement;
        this.metadata = new HashMap<String, Literal>();
        for ( String key : neoStatement.getMetadata().getKeys() )
        {
            this.metadata.put( key, NeoSesameMapper.createLiteral(
                neoStatement.getMetadata().get( key ) ) );
        }
    }
    
    public Resource getContext()
    {
        return this.statement.getContext();
    }
    
    public Value getObject()
    {
        return this.statement.getObject();
    }
    
    public URI getPredicate()
    {
        return this.statement.getPredicate();
    }
    
    public Resource getSubject()
    {
        return this.statement.getSubject();
    }
    
    public Map<String, Literal> getMetadata()
    {
        return new HashMap<String, Literal>( this.metadata );
    }
    
    @Override
    public String toString()
    {
        return this.statement.toString();
    }
}
