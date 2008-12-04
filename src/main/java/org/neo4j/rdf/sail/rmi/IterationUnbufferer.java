package org.neo4j.rdf.sail.rmi;

import static org.neo4j.rdf.sail.rmi.RmiSailClient.RMI_CONNECTION_FAILED;
import info.aduna.iteration.CloseableIteration;

import java.rmi.RemoteException;
import java.util.Collection;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * An iterator which resides on the client side, returning items gotten from
 * a buffering (server side) iterator.
 *
 * @param <E> the type of items in the iteration.
 * @param <X> the type of exception thrown if something goes wrong.
 */
class IterationUnbufferer<E, X extends Exception> implements
    CloseableIteration<E, X>
{
    static <E, X extends Exception> CloseableIteration<E, X> unbuffer(
        RmiIterationBuffer<E, X> buffer )
    {
        return new IterationUnbufferer<E, X>( buffer );
    }
    
    private final RmiIterationBuffer<E, X> buffer;
    private Iterator<E> state;
    
    private IterationUnbufferer( RmiIterationBuffer<E, X> buffer )
    {
        this.buffer = buffer;
    }
    
    public void close() throws X
    {
        try
        {
            buffer.close();
        }
        catch ( RemoteException ex )
        {
            throw new RuntimeException( RMI_CONNECTION_FAILED, ex );
        }
    }
    
    public boolean hasNext() throws X
    {
        if ( state != null && state.hasNext() )
        {
            return true;
        }
        Collection<E> data;
        try
        {
            data = buffer.getChunk();
        }
        catch ( RemoteException ex )
        {
            return false;
        }
        if ( data == null || data.size() == 0 )
        {
            return false;
        }
        state = data.iterator();
        return true;
    }
    
    public E next() throws X
    {
        if ( hasNext() )
        {
            return state.next();
        }
        throw new NoSuchElementException();
    }
    
    public void remove() throws X
    {
        // NOTE: Buffering makes it impossible to remove stuff,
        // if all we have access to is the Iteration interface.
        throw new UnsupportedOperationException();
    }
}
