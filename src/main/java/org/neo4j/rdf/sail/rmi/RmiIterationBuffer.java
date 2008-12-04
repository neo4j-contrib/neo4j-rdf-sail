package org.neo4j.rdf.sail.rmi;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.Collection;

/**
 * An iteration buffer which is optimized for use with RMI. Instead of getting
 * one item at the time it gets a couple of items, minimizing the network
 * overhead.
 *
 * @param <E> the type of items returned.
 * @param <X> the type of exception thrown in something goes wrong.
 */
interface RmiIterationBuffer<E, X extends Exception> extends Remote
{
    void close() throws X, RemoteException;
    
    Collection<E> getChunk() throws X, RemoteException;
}
