package org.neo4j.rdf.sail;

public class SimpleTimer
{
	private long totalTime;
	private int counter;
	private long currentStartTime;
	
	public SimpleTimer()
	{
		this.currentStartTime = System.currentTimeMillis();
	}
	
	public void newLap()
	{
		long time = System.currentTimeMillis() - currentStartTime;
		printTime( time, "Lap time" );
		totalTime += time;
		counter++;
		currentStartTime = System.currentTimeMillis();
	}
	
	public void end()
	{
		newLap();
		printTime( totalTime / counter, "Average time" );
	}
	
	private void printTime( long time, String title )
	{
		int seconds = ( int ) ( time / 1000 );
		int minutes = seconds / 60;
		seconds = seconds % 60;
		int millis = ( int ) ( time % 1000 );
		System.out.println( title + ": " + minutes + " min " +
			seconds + "," + millis + " sec" );
	}
}
