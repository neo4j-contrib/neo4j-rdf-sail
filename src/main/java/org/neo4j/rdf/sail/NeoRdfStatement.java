package org.neo4j.rdf.sail;

import java.util.Map;

import org.openrdf.model.Statement;

public interface NeoRdfStatement extends Statement
{
    Map<String, Object> getMetadata();
}
