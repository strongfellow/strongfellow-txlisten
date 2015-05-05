package com.strongfellow.txlisten;

import java.util.List;

import com.beust.jcommander.Parameter;


public class Args {

	@Parameter
	public List<String> commands;
	
	@Parameter(names = {"--bucket"})
	public String bucket;
	
	@Parameter(names = {"--block"})
	public String block;
}
