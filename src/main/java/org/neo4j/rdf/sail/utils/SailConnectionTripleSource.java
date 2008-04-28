package org.neo4j.rdf.sail.utils;

import info.aduna.iteration.CloseableIteration;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.algebra.evaluation.TripleSource;
import org.openrdf.sail.SailConnection;
import org.openrdf.sail.SailException;
import org.openrdf.model.Resource;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.Statement;
import org.openrdf.model.Value;
import org.openrdf.model.URI;

public class SailConnectionTripleSource implements TripleSource {
    private SailConnection baseConnection;
    private ValueFactory valueFactory;
    private boolean includeInferred;

    public SailConnectionTripleSource(final SailConnection conn, final ValueFactory valueFactory, final boolean includeInferred) {
        baseConnection = conn;
        this.valueFactory = valueFactory;
        this.includeInferred = includeInferred;
    }

    public CloseableIteration<? extends Statement, QueryEvaluationException> getStatements(final Resource subj,
                                                                                           final URI pred,
                                                                                           final Value obj,
                                                                                           final Resource... contexts) {
        try {
            return new QueryEvaluationIteration(
                    baseConnection.getStatements(subj, pred, obj, includeInferred, contexts));
        } catch (SailException e) {
            return new EmptyCloseableIteration<Statement, QueryEvaluationException>();
        }
    }

    public ValueFactory getValueFactory() {
        return valueFactory;
    }
}