package org.neo4j.rdf.sail;

public class SimpleTimer
{
	private long startTime;
	
	public SimpleTimer()
	{
		this.startTime = System.currentTimeMillis();
	}
	
	public void end()
	{
		long time = System.currentTimeMillis() - startTime;
		int seconds = ( int ) ( time / 1000 );
		int minutes = seconds / 60;
		seconds = seconds % 60;
		int millis = ( int ) ( time % 1000 );
		System.out.println( "Time: " + minutes + " min " +
			seconds + "," + millis + " sec" );
	}
}
