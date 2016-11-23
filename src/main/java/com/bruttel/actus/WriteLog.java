/**
Schreibt die Log Resultate in ein CSV File
@author Daniel Bruttel
@version 1.0
*/
package com.bruttel.actus;

import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

public class WriteLog {
	String logFile;
	String className;
	String output;
	String ram;
	String contracts;
	String riskfactors;
	String knoten;
	String run;
	long start;
	long stop;

	public WriteLog(String logFile, String className, String output, String ram, String contracts, String riskfactors,
			String knoten, String run, long start, long stop) {
		this.logFile = logFile;
		this.className = className;
		this.ram = ram;
		this.contracts = contracts;
		this.riskfactors = riskfactors;
		this.knoten = knoten;
		this.run = run;
		this.stop = stop;
		this.start = start;

		try {
			// Pfad erstellen
			URI pfad = new URI("file:///home/user/".concat(logFile));
			// File einlesen
			List<String> lines = Files.readAllLines(Paths.get(pfad));
			lines.add(className + "," + output + "," + ram + "," + contracts + "," + riskfactors + "," + knoten + ","
					+ run + "," + (stop - start));
			Files.write(Paths.get(pfad), lines);

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
