package org.jax.mgi.gxdindexer.indexer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Main {
	public static Logger logger = LoggerFactory.getLogger("gxdindexer Main");
	public static List<String> SPECIFIED_INDEXERS = new ArrayList<String>();
	public static HashMap<String,Indexer> indexerMap = new HashMap<String,Indexer>();
	public static boolean RUN_ALL_INDEXERS=false;

	static {
		/*
		 * All indexers must be added to this list in order to be run.
		 * The key is the name you would use to specify your indexer as a command argument
		 * */
		indexerMap.put("gxdMarker", new GxdMarkerIndexer());
	}

	public static int maxThreads = 1;

	private static List<String> getIndexers() {
		List<String> indexes = new ArrayList<String>();

		for(String indexName : indexerMap.keySet()) {
			indexes.add(indexName);
		}
		Collections.sort(indexes);
		return indexes;
	}

	private static void parseCommandInput(String[] args) {
		Set<String> arguments = new HashSet<String>();
		for (int i = 0; i < args.length; i++){
			arguments.add(args[i]);
		}
		if(!arguments.isEmpty()) {
			RUN_ALL_INDEXERS = arguments.contains("all");
			//start processing commands
			for(String arg : arguments) {
				if(arg.contains("maxThreads=")) {
					String argValue = arg.replace("maxThreads=", "");
					maxThreads = Integer.parseInt(argValue);
				} else if(indexerMap.containsKey(arg)) {
					SPECIFIED_INDEXERS.add(arg);
					logger.info("adding user specified index: " + arg + " to list of indexers to run.");
				} else if("hmdc".equalsIgnoreCase(arg) || "hdp".equalsIgnoreCase(arg)) {
					SPECIFIED_INDEXERS.add("hdpGene");
					SPECIFIED_INDEXERS.add("hdpDisease");
					SPECIFIED_INDEXERS.add("hdpGrid");
					SPECIFIED_INDEXERS.add("hdpGridAnnotation");
				} else if("gxd".equalsIgnoreCase(arg)) {
					SPECIFIED_INDEXERS.add("gxdLitIndex");
					SPECIFIED_INDEXERS.add("gxdResult");
					SPECIFIED_INDEXERS.add("gxdImagePane");
					SPECIFIED_INDEXERS.add("gxdDifferentialMarker");
					SPECIFIED_INDEXERS.add("gxdEmapaAC");
				} else if ("gxdht".equalsIgnoreCase(arg)) {
					SPECIFIED_INDEXERS.add("gxdHtSample");
					SPECIFIED_INDEXERS.add("gxdHtExperiment");
				} else if ("list".equalsIgnoreCase(arg)) {
					for (String s : getIndexers()) {
						System.out.println(s);
					}
					System.exit(0);
				} else {
					logger.info("unknown indexer \""+arg+"\"");
				}
			}
		}
	}

	public static void main(String[] args) {
		parseCommandInput(args);

		if(RUN_ALL_INDEXERS) {
			SPECIFIED_INDEXERS = new ArrayList<String>();
			logger.info("\"all\" option was selected. Beginning run of all indexers");
			for(String idxKey : indexerMap.keySet()) {
				SPECIFIED_INDEXERS.add(idxKey);
			}
		}

		if(SPECIFIED_INDEXERS == null || SPECIFIED_INDEXERS.size() == 0) {
			exitWithMessage("There are no specified indexers to run. Exiting.");
		}

		// track failed indexers for later reporting
		List<String> failedIndexers = new ArrayList<String>();

		ExecutorService executorPool = Executors.newFixedThreadPool(maxThreads);
		
		for(String idxKey: SPECIFIED_INDEXERS) {
			executorPool.submit(indexerMap.get(idxKey));
		}
		
		try {
			executorPool.shutdown();
			while(!executorPool.awaitTermination(30, TimeUnit.SECONDS)) {
				logger.info("Waiting for Threads to finish");
			}
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		
		for(String idxKey: SPECIFIED_INDEXERS) {
			if(!indexerMap.get(idxKey).indexPassed) {
				failedIndexers.add(idxKey);
			}
		}

		// return error if any indexers failed
		if(failedIndexers.size() > 0) {
			String errorMsg = "Failed or Incomplete Indexes: " + StringUtils.join(failedIndexers,",") + "\n Please view the above logs for more details.";
			exitWithMessage(errorMsg);
		} else{
			logger.info("Completed run of the following indexes:"+StringUtils.join(SPECIFIED_INDEXERS,","));
		}
	}

	private static void exitWithMessage(String errorMsg) {
		exitWithMessage(errorMsg,null);
	}

	private static void exitWithMessage(String errorMsg,Exception ex) {
		if(ex== null) logger.error(errorMsg);
		else logger.error(errorMsg,ex);
		// logger needs some time to work before we exit (I know it looks stupid to call sleep() here, but it works)
		try {
			Thread.sleep(500);
		} catch (InterruptedException e) {
			// I really could care less if this exception is ever thrown
			e.printStackTrace();
		}
		System.exit(-1);
	}
}
