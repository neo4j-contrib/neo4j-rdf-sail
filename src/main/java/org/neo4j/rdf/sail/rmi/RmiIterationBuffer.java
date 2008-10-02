package org.neo4j.rdf.sail.rmi;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.Collection;

interface RmiIterationBuffer<E, X extends Exception> extends Remote
{
    void close() throws X, RemoteException;
    
    Collection<E> getChunk() throws X, RemoteException;
}
