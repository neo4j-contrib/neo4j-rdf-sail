package org.neo4j.rdf.sail.utils;

import info.aduna.iteration.CloseableIteration;

import org.neo4j.rdf.util.TemporaryLogger;
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
        TemporaryLogger.Timer timer = new TemporaryLogger.Timer();
        try {
            return new QueryEvaluationIteration(
                    baseConnection.getStatements(subj, pred, obj, includeInferred, contexts));
        } catch (SailException e) {
            return new EmptyCloseableIteration<Statement, QueryEvaluationException>();
        }/* finally {
            TemporaryLogger.getLogger().info( "SailConnectionTripleSource.getStatements: " +
                "S:" + subj + "  P:" + pred + "  O:" + obj + "  G:" + contextsString( contexts ) +
                "  time:" + timer.lap() );
        }*/
    }
    
    private String contextsString( Resource... contexts )
    {
        if ( contexts == null )
        {
            return "null";
        }
        
        StringBuffer buffer = new StringBuffer( "[" );
        int counter = 0;
        for ( Resource context : contexts )
        {
            if ( counter++ > 0 )
            {
                buffer.append( "," );
            }
            buffer.append( (null == context) ? "null" : context.toString() );
        }
        buffer.append( "]" );
        return buffer.toString();
    }

    public ValueFactory getValueFactory() {
        return valueFactory;
    }
}
