package org.neo4j.rdf.sail;

import org.neo4j.rdf.model.Context;
import org.openrdf.model.BNode;
import org.openrdf.model.Literal;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;

/**
 * Author: josh
 * Date: Apr 25, 2008
 * Time: 7:17:16 PM
 */
public class GraphDatabaseSesameMapper {
    // TODO: check whether ValueFactoryImpl is thread-safe
    private static final ValueFactory VALUE_FACTORY = new ValueFactoryImpl();

    public static Literal createLiteral(final org.neo4j.rdf.model.Literal from) {
        return (null == from.getDatatype())
                ? (null == from.getLanguage())
                // FIXME: casting to String may not be safe
                        ? VALUE_FACTORY.createLiteral((String) from.getValue())
                        : VALUE_FACTORY.createLiteral((String) from.getValue(), from.getLanguage())
                : VALUE_FACTORY.createLiteral((String) from.getValue(), createUri(from.getDatatype()));
    }

    public static BNode createBlankNode(final org.neo4j.rdf.model.BlankNode from) {
        // FIXME: possible null pointer exception
        return VALUE_FACTORY.createBNode(from.getInternalIdOrNull());
    }

    public static URI createUri(final org.neo4j.rdf.model.Uri from) {
        return VALUE_FACTORY.createURI(from.getUriAsString());
    }

    public static Resource createResource(final org.neo4j.rdf.model.Resource from) {
        return (from instanceof org.neo4j.rdf.model.Uri)
                ? createUri((org.neo4j.rdf.model.Uri) from)
                : createBlankNode((org.neo4j.rdf.model.BlankNode) from);
    }

    public static Value createValue(final org.neo4j.rdf.model.Value from) {
        return (from instanceof org.neo4j.rdf.model.Literal)
                ? createLiteral((org.neo4j.rdf.model.Literal) from)
                : (from instanceof org.neo4j.rdf.model.Uri)
                        ? createUri((org.neo4j.rdf.model.Uri) from)
                        : createBlankNode((org.neo4j.rdf.model.BlankNode) from);
    }

    public static Resource createContext(final org.neo4j.rdf.model.Context from) {
        // TODO: blank node contexts
        return from == null || from.equals( org.neo4j.rdf.model.Context.NULL ) ? null :
            VALUE_FACTORY.createURI(from.getUriAsString());
    }

    public static Statement createStatement(final org.neo4j.rdf.model.CompleteStatement from,
        final boolean readMetadata) {
        Context context = from.getContext();
        if (context != null && context.isWildcard()) {
            throw new IllegalArgumentException("Cannot have wildcard context");
        }
//System.out.println("context = " + context);
//System.out.println("    actually null?: " + (null == context.getUriAsString()));

        Statement result = (null == context)
                ? VALUE_FACTORY.createStatement(createResource(from.getSubject()),
                        createUri(from.getPredicate()),
                        createValue(from.getObject()))
                : VALUE_FACTORY.createStatement(
                        createResource(from.getSubject()),
                        createUri(from.getPredicate()),
                        createValue(from.getObject()),
                        createContext(context));
        if ( readMetadata )
        {
            result = new GraphDatabaseStatementImpl( result, from );
        }
        return result;
    }

    // TODO (maybe): createNamespace
}
