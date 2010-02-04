package org.neo4j.rdf.sail;

import java.util.Map;

import org.openrdf.model.Literal;
import org.openrdf.model.Statement;

public interface GraphDatabaseStatement extends Statement
{
    Map<String, Literal> getMetadata();
}
