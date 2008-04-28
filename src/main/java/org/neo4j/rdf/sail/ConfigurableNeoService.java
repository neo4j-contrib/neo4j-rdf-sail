package org.neo4j.rdf.sail;

import java.util.Map;

import org.neo4j.api.core.NeoService;

public interface ConfigurableNeoService extends NeoService
{
    void startup( String storeDir, Map<String, String> config );
    void shutdown();
}
