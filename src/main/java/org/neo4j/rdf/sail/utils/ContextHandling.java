package org.neo4j.rdf.sail.utils;

import org.neo4j.rdf.model.Context;
import org.neo4j.rdf.model.Wildcard;
import org.openrdf.model.Resource;

public class ContextHandling {
    public static org.neo4j.rdf.model.Context createContext(final Resource resource) {
        // TODO: handle BNode contexts (their String representation is probably not what you want)
        return (null == resource)
                ? org.neo4j.rdf.model.Context.NULL : new org.neo4j.rdf.model.Context(resource.toString());
    }

    public static org.neo4j.rdf.model.Value createSingleContext(boolean mustBeOne, final Resource... contexts) {
        org.neo4j.rdf.model.Value context = null;
        if (contexts.length == 0) {
            if (mustBeOne) {
                throw new IllegalArgumentException("Must be exactely one context");
            }
            context = new Wildcard("?g");
        } else if (contexts.length == 1) {
            context = createContext(contexts[0]);
        } else {
            throw new IllegalArgumentException("Can only have zero or one context");
        }
        return context;
    }
    
    public static org.neo4j.rdf.model.Context[] createContexts( final Resource... resources )
    {
        if ( resources == null )
        {
            return new Context[] { org.neo4j.rdf.model.Context.NULL };
        }
        
        Context[] contexts = new Context[ resources.length ];
        for ( int i = 0; i < resources.length; i++ )
        {
            contexts[ i ] = createContext( resources[ i ] );
        }
        return contexts;
    }
}
