package org.neo4j.rdf.sail;

import org.openrdf.model.URI;
import org.openrdf.model.Resource;
import org.openrdf.model.BNode;
import org.openrdf.model.Literal;
import org.openrdf.model.Statement;
import org.openrdf.model.Value;
import org.neo4j.rdf.model.BlankNode;
import org.neo4j.rdf.model.Wildcard;
import org.neo4j.rdf.sail.utils.ContextHandling;

/**
 * Author: josh
 * Date: Apr 25, 2008
 * Time: 6:09:18 PM
 */
public class SesameGraphDatabaseMapper {
    public static org.neo4j.rdf.model.Literal createLiteral(final Literal lit) {
        org.neo4j.rdf.model.Uri datatype = (null == lit.getDatatype())
                ? null : createUri(lit.getDatatype());

        return new org.neo4j.rdf.model.Literal(lit.getLabel(), datatype, lit.getLanguage());
    }

    public static org.neo4j.rdf.model.Uri createUri(final URI uri) {
        return new org.neo4j.rdf.model.Uri(uri.toString());
    }

    public static BlankNode createBlankNode(final BNode bnode) {
        return new org.neo4j.rdf.model.BlankNode(bnode.getID());
    }

    public static org.neo4j.rdf.model.Resource createResource(final Resource res) {
        if (res instanceof URI) {
            return createUri((URI) res);
        } else if (res instanceof BNode) {
            return createBlankNode((BNode) res);
        } else {
            throw new IllegalArgumentException("bad resource type");
        }
    }

    public static org.neo4j.rdf.model.Value createValue(final Value val) {
        return (val instanceof Literal)
                ? createLiteral((Literal) val)
                : (val instanceof URI)
                        ? createUri((URI) val)
                        : createBlankNode((BNode) val);
    }

    public static org.neo4j.rdf.model.WildcardStatement createWildcardStatement(final Resource subject,
                                                                                final URI predicate,
                                                                                final Value object,
                                                                                final Resource... inContexts) {
        org.neo4j.rdf.model.Value context =
                ContextHandling.createSingleContext(false, inContexts);
        return new org.neo4j.rdf.model.WildcardStatement(
                (null == subject) ? new Wildcard("?s") : createResource(subject),
                (null == predicate) ? new Wildcard("?p") : createUri(predicate),
                (null == object) ? new Wildcard("?o") : createValue(object),
                context);
    }

    public static org.neo4j.rdf.model.CompleteStatement createCompleteStatement(final Resource subject,
                                                                                final URI predicate,
                                                                                final Value object,
                                                                                final Resource... inContexts) {
        org.neo4j.rdf.model.Context context = (org.neo4j.rdf.model.Context)
                ContextHandling.createSingleContext(true, inContexts);

        if (object instanceof Literal) {
            return new org.neo4j.rdf.model.CompleteStatement(
                    createResource(subject),
                    createUri(predicate),
                    (org.neo4j.rdf.model.Literal) createValue(object),
                    context);
        } else {
            return new org.neo4j.rdf.model.CompleteStatement(
                    createResource(subject),
                    createUri(predicate),
                    (org.neo4j.rdf.model.Resource) createValue(object),
                    context);
        }
    }

    public static org.neo4j.rdf.model.Statement createCompleteStatement(final Statement st) {
        org.neo4j.rdf.model.Context context = ContextHandling.createContext(st.getContext());
        if (st.getObject() instanceof Literal) {
            return new org.neo4j.rdf.model.CompleteStatement(
                    createResource(st.getSubject()),
                    createUri(st.getPredicate()),
                    (org.neo4j.rdf.model.Literal) createValue(st.getObject()),
                    context);
        } else {
            return new org.neo4j.rdf.model.CompleteStatement(
                    createResource(st.getSubject()),
                    createUri(st.getPredicate()),
                    (org.neo4j.rdf.model.Resource) createValue(st.getObject()),
                    context);
        }
    }

    // TODO (maybe): createNamespace
}
