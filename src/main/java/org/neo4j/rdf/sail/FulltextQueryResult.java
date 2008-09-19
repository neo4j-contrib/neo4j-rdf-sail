package org.neo4j.rdf.sail;

import java.io.Serializable;

import org.neo4j.rdf.fulltext.QueryResult;
import org.openrdf.model.Statement;

public class FulltextQueryResult implements Serializable
{
	private Statement statement;
	private double score;
	private String snippet;
	
	public FulltextQueryResult( QueryResult wrappedResult )
	{
		this.statement = NeoSesameMapper.createStatement(
			wrappedResult.getStatement() );
		this.score = wrappedResult.getScore();
		this.snippet = wrappedResult.getSnippet();
	}
	
	public Statement getStatement()
	{
		return this.statement;
	}
	
	public double getScore()
	{
		return this.score;
	}
	
	public String getSnippet()
	{
		return this.snippet;
	}
}
