package org.jax.mgi.gxdindexer;

import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.sql.ResultSet;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.jax.mgi.gxdindexer.shr.SQLExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.Refresh;
import co.elastic.clients.elasticsearch.core.BulkRequest;
import co.elastic.clients.elasticsearch.core.BulkResponse;
import co.elastic.clients.elasticsearch.core.bulk.BulkOperation;
import co.elastic.clients.elasticsearch.indices.CreateIndexRequest;
import co.elastic.clients.elasticsearch.indices.CreateIndexResponse;
import co.elastic.clients.elasticsearch.indices.DeleteIndexRequest;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.rest_client.RestClientTransport;

/**
 * Indexer
 * @author kstone
 * This is the parent class for all of the indexers, and it supplies some useful 
 * functions that all of the indexers might need.
 * 
 * It also sets up the sql connection, as well as the connection to a solr index, 
 * which is passed to it during construction time.
 */

public abstract class Indexer implements Runnable {

    protected ElasticsearchClient client;
	public SQLExecutor ex = new SQLExecutor();

	public Logger logger = LoggerFactory.getLogger(this.getClass());
	private String indexName = "";
	protected DecimalFormat df = new DecimalFormat("#.00");
	protected Runtime runtime = Runtime.getRuntime();
	public boolean indexPassed = true;
	public boolean skipOptimizer = false;
	private boolean doNotWriteDocToES = false;   // for collect data only, not write data to ES
	public Map<String, List<Map<String, Object>>> docs = new HashMap<String, List<Map<String, Object>>>();		

	// Variables for handling threads
	private List<Thread> currentThreads =new ArrayList<Thread>();
	// maxThreads is configurable. When maxThreads is reached, program waits until they are finished.
	// This is essentially running them in batches

	protected Indexer(String solrIndexName) {
		this.indexName = solrIndexName;
	}

	public void setupConnection() throws Exception {
		logger.info("Setting up the properties");

		InputStream in = Indexer.class.getClassLoader().getResourceAsStream("config.properties");
		Properties props = new Properties();
		if (in== null) {
			logger.info("resource config.properties not found");
		}
		try {
			props.load(in);
			logger.debug(props.toString());
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		logger.info("db connection info: "+ ex);

        String esUrl = props.getProperty("index.url"); // example: http://localhost:9200

        logger.info("Connecting to Elasticsearch: " + esUrl);
        
//        String apiKey = "aXpNZ041c0I0cXgtNUFyT29jTjI6WUF2OUwxNHFBeUVZY3NPaVVLc3hoUQ==";  // id:key, base64 encoded
//
//        Header[] defaultHeaders = new Header[] {
//            new BasicHeader("Authorization", "ApiKey " + apiKey)
//        };  
        
//		 .setDefaultHeaders(defaultHeaders)

        
//        
//        String username = "elastic";
//        String password = "Dh1dEvW5eB5OOS1iH21CeBQD";
//        
//        BasicCredentialsProvider credsProvider = new BasicCredentialsProvider();
//        credsProvider.setCredentials(
//            AuthScope.ANY,
//            new UsernamePasswordCredentials(username, password)
//        );  
        
//        .setHttpClientConfigCallback(httpClientBuilder ->
//        httpClientBuilder
//            .setDefaultCredentialsProvider(credsProvider)
//    )        
        
        
        String username = "elastic";
        String password = "Dh1dEvW5eB5OOS1iH21CeBQD";
        
        BasicCredentialsProvider credsProvider = new BasicCredentialsProvider();
        credsProvider.setCredentials(
            AuthScope.ANY,
            new UsernamePasswordCredentials(username, password)
        );        
		 
        RestClientBuilder builder = RestClient.builder(
        		HttpHost.create(esUrl))

        		 .setHttpClientConfigCallback(httpClientBuilder ->
        	        httpClientBuilder
        	            .setDefaultCredentialsProvider(credsProvider)
        	    )        		
        	    .setRequestConfigCallback(requestConfigBuilder -> 
        	        requestConfigBuilder
        	            .setConnectTimeout(60_000)      // time to establish connection
        	            .setSocketTimeout(120_000)      // time waiting for data (was 30_000)
        	            .setConnectionRequestTimeout(60_000)
        	    );
    	ElasticsearchTransport transport = new RestClientTransport(builder.build(), new JacksonJsonpMapper());
    	client = new ElasticsearchClient(transport);

        // clear existing index
//        try {
//            logger.info("Deleting all docs in index: " + indexName);
//            DeleteByQueryRequest delReq = new DeleteByQueryRequest.Builder()
//                    .index(indexName)
//                    .query(QueryBuilders.matchAll().build()._toQuery())
//                    .build();
//            client.deleteByQuery(delReq);
//        } catch (Exception e) {
//            logger.warn("Delete by query failed (maybe index not present): " + e.getMessage());
//        }
	}

	/*
	 * Code for loading a solr index must be implemented here
	 */
	abstract void index() throws Exception;

	public void setSkipOptimizer(boolean val) {
		this.skipOptimizer = val;
	}

	public void run() {
		try {
			setupConnection();
			deleteIndex();
			createIndex();
			index();
			closeConnection();
			logger.info("Completed run of " + getClass());
		} catch (Exception e) {
			indexPassed = false;
			logger.error("Indexer: " + getClass() + " failed.", e);
		}
	}
	
	// closes down the connection and makes sure a last commit is run
	public void closeConnection() {
		
		logger.info("Indexer: Waiting for " + currentThreads.size() + " Threads to finish, RAM used: " + memoryUsed());
		
		for(Thread t : currentThreads) {
			try {
				t.join();
			} catch (InterruptedException e) {
				logger.error(e.getMessage());
				e.printStackTrace();
			}
		}
		
		commit(true);
		if (!this.skipOptimizer) {
			optimize(true);
		}
		logger.info("Solr Documents are flushed to the server shuting down: " + indexName);
		try {
			client.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void commit() {
		if ( isDoNotWriteDocToES() ) {
			return;
		}		
		commit(true);
	}
	
	public void optimize(boolean wait) {
//		try {
//			logger.info("Waiting for Solr Optimize");
//			if(wait) {
//				client.optimize(wait, wait);
//			} else {
//				client.optimize();
//			}
//		} catch (SolrServerException | IOException e) {
//			e.printStackTrace();
//		}
	}
	
	public void commit(boolean wait) {
//		try {
//			logger.info("Waiting for Solr Commit");
//			checkMemory();
//			if(wait) {
//				client.commit(wait, wait);
//			} else {
//				client.commit();
//			}
//		} catch (SolrServerException | IOException e) {
//			e.printStackTrace();
//		}
	}
	
	private void checkMemory() {
		if(memoryPercent() > 0.95) {
			logger.info("Memory usage is HIGH!!!: " + memoryUsed());
			printMemory();
		}
	}
	
	protected void printMemory() {
		logger.info("Used Mem: " + (runtime.totalMemory() - runtime.freeMemory()) + " " + memoryUsed());
		logger.info("Free Mem: " + runtime.freeMemory());
		logger.info("Total Mem: " + runtime.totalMemory());
		logger.info("Max Memory: " + runtime.maxMemory());
	}

	// returns the percentage of total memory used, as a String
	protected String memoryUsed() {
		return df.format(memoryPercent() * 100) + "%";
	}
	
	// returns the fraction of total memory used, as a Double
	protected double memoryPercent() {
		return ((double)runtime.totalMemory() - (double)runtime.freeMemory()) / (double)runtime.maxMemory();
	}
	
	
	// Create a hashmap, of a key -> hashSet mapping.
	// The hashSet is simply a collection for our 1->N cases.
	// This does not belong in this class. It's a straight up utility function
	protected HashMap <String, HashSet <String>> makeHash(String sql, String keyString, String valueString) {
		HashMap<String, String> allValues = new HashMap<String, String>();
		
		HashMap <String, HashSet <String>> tempMap = new HashMap <String, HashSet <String>> ();

		try {
			ResultSet rs = ex.executeProto(sql);

			String key = null;
			String value = null;

			while (rs.next()) {
				key = rs.getString(keyString);
				value = rs.getString(valueString);
				
				if(allValues.containsKey(value)) {
					value = allValues.get(value);
				} else {
					allValues.put(value, value);
				}
				
				if (tempMap.containsKey(key)) {
					tempMap.get(key).add(value);
				}
				else {
					HashSet <String> temp = new HashSet <String> ();
					temp.add(value);
					tempMap.put(key, temp);
				}
			}
			
		} catch (Exception e) {e.printStackTrace();}
		allValues.clear();
		return tempMap;
	}

	/*
	 * writes documents to solr.
	 * Best practice is to write small batches of documents to Solr
	 * and to commit less frequently. (TIP: this method will commit documents automatically using commitWithin)
	 * Here we also spawn a new process for each batch of documents.
	 */
	
	public void writeDocs(List<Map<String, Object>> docs) {
		if ( isDoNotWriteDocToES() ) {
			return;
		}		
        if (docs == null || docs.isEmpty()) return;

        List<BulkOperation> operations = new ArrayList<>();
        for (Map<String, Object> doc : docs) {
            operations.add(BulkOperation.of(b -> b
                    .index(idx -> idx
                            .index(indexName)
                            .document(doc)
                    )
            ));
        }

        BulkRequest bulkRequest = BulkRequest.of(b -> b
                .operations(operations)
                .refresh(Refresh.True) // equivalent to a commit
        );

        try {
            BulkResponse response = client.bulk(bulkRequest);
            if (response.errors()) {
                System.err.println("Some documents failed to index: " + response.items());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
		
	}

	@Override
	public String toString() {
		return getClass().toString();
	}


	/*
	 * The following are convenience methods for populating lookups to be used in generating multiValued fields
	 *  A String -> Set<String> map is always returned. This is to keep all terms unique, which leads to faster indexing in Solr.
	 *  
	 * Example Usage:
	 * 	 String termSynonymQuery="select t.primary_id term_id,ts.synonym "+
	 *		"from term t,term_synonym ts "+
	 *			"where t.term_key=ts.term_key " +
	 *				"and t.vocab_name in ('Disease Ontology','Mammalian Phenotype') ";
	 *
	 *	 Map<String,Set<String>> synonymLookup = populateLookup(termSynonymQuery,"term_id","synonym","synonyms to term IDs");
	 *
	 * You may also pass in an existing map to add to it
	 *   Map<String,Set<String>> synonymLookup = populateLookup(extraTermSynonymQuery,"term_id","synonym","extra synonyms to term IDs",synonymLookup);
	 *   
	 *   When no map is passed in, by default you will get back HashMap<String,HashSet<String>> as a return type.
	 *   You may pass in a different type of Map<String,Set<String>> if you want to use either a different Map implementation, or a different Set implementation
	 *   	However, if the Set class is not passed in, a HashSet will be used (this is a limitation on java reflection)
	 *   Example:
	 *   	 Map<String,Set<String>> orderedSynonymLookup = 
	 *   		populateLookup(termSynonymQuery,"term_id","synonym","synonyms to term IDs",
	 *   			new HashMap<String,LinkedHashSet<String>>(),LinkedHashSet.class);
	 *   
	 *   Alternatively, use the shortcut methods populateLookupOrdered() if you want the default to be LinkedHashSet
	 */
	protected Map<String,Set<String>> populateLookup(String query,String uniqueFieldName,String secondFieldName,String logText) throws Exception {
		return populateLookup(query,uniqueFieldName,secondFieldName,logText,new HashMap<String,Set<String>>());
	}
	
	protected Map<String,Set<String>> populateLookup(String query,String uniqueFieldName,String secondFieldName,String logText, Map<String,? extends Set<String>> lookupRef) throws Exception {
		return populateLookup(query,uniqueFieldName,secondFieldName,logText,lookupRef,HashSet.class);
	}

	protected Map<String,Set<String>> populateLookupOrdered(String query,String uniqueFieldName,String secondFieldName,String logText) throws Exception {
		return populateLookupOrdered(query,uniqueFieldName,secondFieldName,logText,new HashMap<String,Set<String>>());
	}

	protected Map<String,Set<String>> populateLookupOrdered(String query,String uniqueFieldName,String secondFieldName,String logText, Map<String,? extends Set<String>> lookupRef) throws Exception {
		return populateLookup(query,uniqueFieldName,secondFieldName,logText,lookupRef,LinkedHashSet.class);
	}

	@SuppressWarnings("unchecked")
	protected Map<String,Set<String>> populateLookup(String query,String uniqueFieldName,String secondFieldName,String logText, Map<String,? extends Set<String>> lookupRef,@SuppressWarnings("rawtypes") Class<? extends Set> setClass) throws Exception {
		// do some type-casting magic in order to create a new instance of "? extends Set"
		Map<String,Set<String>> returnLookup = (Map<String,Set<String>>) lookupRef;

		logger.info("populating map of " + logText);
		long start = runtime.freeMemory();
		ResultSet rs = ex.executeProto(query);

		int rows = 0;
		while (rs.next()) {
			String uniqueField = rs.getString(uniqueFieldName);
			String secondField = rs.getString(secondFieldName);
			if(!returnLookup.containsKey(uniqueField)) {
				returnLookup.put(uniqueField, setClass.getDeclaredConstructor().newInstance());
			}
			returnLookup.get(uniqueField).add(secondField);
			rows++;
		}

		rs.close();
		long end = runtime.freeMemory();
		logger.info("finished populating map of "+ logText + " with " + rows + " rows for " + returnLookup.size() + " " + uniqueFieldName + " Memory Change: " + (end - start) + " bytes");
		
		return returnLookup;
	}
	

	protected void populateOMIMNumberPartsForIds(Map<String, Set<String>> idMap) {
		
		for(String termSetKey: idMap.keySet()) {
			
			Set<String> ids = idMap.get(termSetKey);
			
			List<String> newIds = new ArrayList<String>();
			// This adds the Number part of OMIM to the alt ids also for searching
			for (String id: ids) {
				if (id.startsWith("OMIM:")) {
					newIds.add(id.replaceFirst("OMIM:", ""));
				}
			}
			ids.addAll(newIds);
		}
	}

	/*
	 * Convenience method to add the given value for the given solr field.
	 * Is a no-op if either the field or the value are null.
	 */
	protected void addIfNotNull(Map<String, Object> doc, String solrField, Object value) {
		if ((value != null) && (solrField != null)) {
			doc.put(solrField, value);
		}
	}

	/*
	 * Convenience method to add all items from an iterable to a particular solr field.
	 * Ignores input if null.
	 */
	protected void addAll(Map<String, Object> doc,String solrField,Iterable<String> items) {
		if(items != null) {
			for(Object obj : items) {
				doc.put(solrField,obj);
			}
		}
	}

	/*
	 * Convenience method to add all items from a lookup map
	 * to a particular solr field. Ignores input if lookupId doesn't exist.
	 */
	protected void addAllFromLookup(Map<String, Object> doc,String solrField,String lookupId,Map<String,Set<String>> lookupRef) {
		if(lookupRef.containsKey(lookupId)) {
			for(Object obj : lookupRef.get(lookupId)) {
				doc.put(solrField,obj);
			}
		}
	}


	private Map<String,Set<String>> dupTracker = new HashMap<String,Set<String>>();
	protected void addAllFromLookupNoDups(Map<String, Object> doc,String solrField,String lookupId,Map<String,Set<String>> lookupRef) {
		Set<String> uniqueList = getNoDupList(solrField);

		if(lookupRef.containsKey(lookupId)) {
			for(String obj : lookupRef.get(lookupId)) {
				if(uniqueList.contains(obj)) continue;
				else uniqueList.add(obj);
				doc.put(solrField,obj);
			}
		}
	}

	private Set<String> getNoDupList(String solrField) {
		Set<String> uniqueList;
		if(!dupTracker.containsKey(solrField)) {
			uniqueList = new HashSet<String>();
			dupTracker.put(solrField,uniqueList);
		}
		else uniqueList = dupTracker.get(solrField);
		return uniqueList;
	}
	
	protected void addFieldNoDup(Map<String, Object> doc,String solrField,String value) {
		Set<String> uniqueList = getNoDupList(solrField);
		if(uniqueList.contains(value)) return;
		uniqueList.add(value);
		doc.put(solrField,value);
	}

	protected void resetDupTracking() {
		dupTracker = new HashMap<String,Set<String>>();
	}

	// fill rows into a temp table using the given 'cmd'.  (may also create the table, depending
	// on the SQL)
	protected void fillTempTable(String cmd) {
		ex.executeVoid(cmd);
		logger.debug("  - populated table in " + ex.getTimestamp());
	}

	private int indexCounter=0;		// counter of indexes created so far (for unique naming)

	// create an index on the given column in the given table
	protected void createTempIndex(String tableName,String column) {
		indexCounter += 1;
		ex.executeVoid("create index tmp_idx"+indexCounter+" on "+tableName+" ("+column+")");
		logger.debug("  - created index tmp_idx" + indexCounter + " in " + ex.getTimestamp());
	}
	
	// run 'analyze' on the given table
	protected void analyze(String tableName) {
		ex.executeVoid("analyze " + tableName);
		logger.debug("  - analyzed " + tableName + " in " + ex.getTimestamp());
	}

	protected void logFreeMemory() {
		logger.info("  - free memory: " + Runtime.getRuntime().freeMemory() + " bytes");
	}
	
	protected abstract String getIndexMappingJson();
	
    public void createIndex() throws IOException {
    	CreateIndexRequest request = new CreateIndexRequest.Builder()
    		    .index(this.indexName)
    		    .withJson(new StringReader(getIndexMappingJson()))
    		    .build();
    	
        // Send request
        CreateIndexResponse response = client.indices().create(request);

        logger.info("Create Index " + this.indexName + ": " + response.acknowledged() );
    }	
	
    public void deleteIndex() throws IOException {
    	try {
    		this.client.indices().delete(new DeleteIndexRequest.Builder().index(this.indexName).build());
			logger.info("Delete index " + this.indexName + ": deleted");
		} catch (Exception e) {
			logger.info("Delete index " + this.indexName + ": " + e.getMessage());
		}
    }
    
	public boolean isDoNotWriteDocToES() {
		return doNotWriteDocToES;
	}

	public void setDoNotWriteDocToES(boolean doNotWriteDocToES) {
		this.doNotWriteDocToES = doNotWriteDocToES;
	}

	public void addDoc(String key, Map<String, Object> doc) {
		if ( !isDoNotWriteDocToES() ) {
			return;
		}
		List<Map<String, Object>> list;
		if ( docs.containsKey(key) ) {
			list = docs.get(key);			
		} else {
			list = new ArrayList<Map<String, Object>>();
			docs.put(key, list);
		}
		list.add(doc);
	}
	
	public Map<String, List<Map<String, Object>>> getDocs() {
		return docs;
	} 
	
	
	protected void addProfileMarker(Map<String, Object> doc, List<Map<String, Object>> gxdProfileMarker) {
		if ( gxdProfileMarker == null) {
			return;
		}
		List<Object> posCExact = new ArrayList<Object>();
		List<Object> posCAnc = new ArrayList<Object>();
		List<Object> posRExact = new ArrayList<Object>();
		List<Object> posRAnc = new ArrayList<Object>();
		List<Object> posCExactA = new ArrayList<Object>();
		List<Object> posCAncA = new ArrayList<Object>();
		List<Object> posRExactA = new ArrayList<Object>();
		List<Object> posRAncA = new ArrayList<Object>();
		for (Map<String, Object> profile: gxdProfileMarker) {
			add(profile, posCExact, "posCExact");
			add(profile, posCAnc, "posCAnc");
			add(profile, posRExact, "posRExact");
			add(profile, posRAnc, "posRAnc");
			add(profile, posCExactA, "posCExactA");
			add(profile, posCAncA, "posCAncA");
			add(profile, posRExactA, "posRExactA");
			add(profile, posRAncA, "posRAncA");
		}
		doc.put("posCExact", posCExact);
		doc.put("posCAnc", posCAnc);
		doc.put("posRExact", posRExact);
		doc.put("posRAnc", posRAnc);
		doc.put("posCExactA", posCExactA);
		doc.put("posCAncA", posCAncA);
		doc.put("posRExactA", posRExactA);
		doc.put("posRAncA", posRAncA);	
	}
	
	private void add(Map<String, Object> profile, List<Object> list, String key) {
		Object obj = profile.get(key);
		if ( obj != null ) {
			list.add(obj);
		}
	}
}
