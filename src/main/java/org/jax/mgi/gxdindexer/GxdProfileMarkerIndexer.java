package org.jax.mgi.gxdindexer;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.solr.common.SolrInputDocument;
import org.jax.mgi.shr.fe.indexconstants.GxdResultFields;

/**
 * GxdProfileMarkerIndexer
 * @author joel richardson
 * This index is used for performing GXD profile searches.
 * Each document represents a marker with a marker key,
 * 	which will then be used to filter results from the gxdResult index in a two step process.
 * 
 */

public class GxdProfileMarkerIndexer extends Indexer 
{   
	//--- instance variables ---//
	
	// shared empty set object to make code simpler later on
	private Set<String> emptySet = new HashSet<String>();
	
	// maps from EMAPS structure keys to EMAPA structure keys
	private Map<String,String> emaps2emapa = null;

	// maps from EMAPS structure key to all of its EMAPS ancestor keys 
	private Map<String,Set<String>> emapsAncestors = null;
	
	//--- constructors ---//
	
	public GxdProfileMarkerIndexer () {
		super("gxdProfileMarker");
	}

	//--- methods ---//
	
	// get a lookup for marker IDs (official mouse markers between the specified keys)
	public Map<Integer,String> getMarkerIDs(Integer startKey, Integer endKey) throws Exception {
		Map<Integer,String> markerIDs = new HashMap<Integer,String>();

		String cmd = "select marker_key, primary_id "
			+ "from marker "
			+ "where marker_key >= " + startKey
			+ " and marker_key < " + endKey
			+ " and organism = 'mouse' "
			+ " and status = 'official'";

		ResultSet rs = ex.executeProto(cmd);
		while (rs.next()) {
			markerIDs.put(rs.getInt("marker_key"), rs.getString("primary_id"));
		}
		rs.close();
		logger.info("Got IDs for " + markerIDs.size() + " markers");
		return markerIDs;
	}

	// get the mapping from each EMAPS term key to its EMAPA equivalent
	public void fillEmaps2Emapa() throws Exception {
		emaps2emapa = new HashMap<String,String>();
		
		String cmd = "select te.term_key, te.emapa_term_key "
			+ "from term_emap te "
			+ "where te.emapa_term_key is not null " ;
		
		ResultSet rs = ex.executeProto(cmd);
		while (rs.next()) {
			emaps2emapa.put(rs.getString("term_key"), rs.getString("emapa_term_key"));
		}
		rs.close();
		logger.info("Got EMAPS to EMAPA mappings for " + emaps2emapa.size() + " EMAPS terms");
	}

	public String getEmapaKey (String emapsKey) {
		return emaps2emapa.get(emapsKey);
	}
	
	public Set<String> getEmapaKeys (Set<String> emapsKeys) {
		Set<String> emapaKeys = new HashSet<String>();
		for (String emapsKey : emapsKeys) {
		        emapaKeys.add(getEmapaKey(emapsKey));
		}
		return emapaKeys;
	}
	
	// get the mapping from each EMAPS term key to its EMAPS ancestor keys
	// (Reflexive. A term's ancestor keys includes its own key.)
	public void fillEmapsAncestors() throws Exception {
		emapsAncestors = new HashMap<String,Set<String>>();
		
		String cmd = "select t.term_key, a.ancestor_term_key "
			+ "from term t, term_ancestor a "
			+ "where t.term_key = a.term_key "
			+ "and t.vocab_name = 'EMAPS'" ;
		
		ResultSet rs = ex.executeProto(cmd);
		while (rs.next()) {
			String termKey = rs.getString("term_key");
			if (!emapsAncestors.containsKey(termKey)) {
				emapsAncestors.put(termKey, new HashSet<String>());
				emapsAncestors.get(termKey).add(termKey);
			}
			emapsAncestors.get(termKey).add(rs.getString("ancestor_term_key"));
		}
		rs.close();
		logger.info("Got EMAPS ancestors for " + emapsAncestors.size() + " EMAPS terms");
	}
	
	// get the set of EMAPS structure keys that are ancestors of the given EMAPS structure key,
	// (including 'emapsKey' itself)
	public Set<String> getEmapsAncestors(String emapsKey) throws Exception {
		if (emapsAncestors.containsKey(emapsKey)) {
			return emapsAncestors.get(emapsKey);
		}
		return emptySet;
	}
	

	// get an ordered list of markers that have expression data
	public List<Integer> getMarkerKeys() throws Exception {
		logger.info("Getting marker keys");
		List<Integer> markerKeys = new ArrayList<Integer>();

		String cmd = "select m.marker_key from marker m "
			+ "where m.organism = 'mouse' "
			+ "and m.status = 'official' "
			+ "and exists (select 1 from expression_result_summary s "
			+ "  where m.marker_key = s.marker_key) "
			+ " UNION "
			+ "select m.marker_key from marker m "
			+ "where m.organism = 'mouse' "
			+ "and m.status = 'official' "
			+ "and exists (select 1 from expression_ht_consolidated_sample_measurement s "
			+ "  where m.marker_key = s.marker_key) "
			+ "order by 1";

		ResultSet rs = ex.executeProto(cmd);
		while (rs.next()) {
			markerKeys.add(rs.getInt("marker_key"));
		}
		rs.close();
		logger.info(" - found " + markerKeys.size() + " marker keys from " + markerKeys.get(0) + " to " + markerKeys.get(markerKeys.size() - 1));

		return markerKeys;
	}

	// add results for the classical data for markers between the two given keys
	public void addClassicalResults(Integer startMarkerKey, Integer endMarkerKey, Map<Integer,List<Result>> markerResults) throws Exception {
		logger.info("Getting classical results (markers " + startMarkerKey + " to " + endMarkerKey + ")");

		String query = "select ers.is_expressed, ers.structure_key, "
			+ " ers.structure_printname, "
			+ " emaps.primary_id emaps_id, "
			+ " ers.theiler_stage, ers.marker_key "
			+ "from expression_result_summary ers "
			+ " join term emaps on ers.structure_key=emaps.term_key "
			+ "where ers.marker_key >= " + startMarkerKey
			+ " and ers.marker_key < " + endMarkerKey
			+ " and ers.is_expressed != 'Unknown/Ambiguous' "
			+ " and ers.assay_type != 'Recombinase reporter' "
			+ " and ers.assay_type != 'In situ reporter (transgenic)' "
			+ " and (ers.is_wild_type = 1 or ers.genotype_key=-1)";

		// "order by is_expressed desc ";
		ResultSet rs = ex.executeProto(query);

		logger.info(" - organizing them");
		while (rs.next()) {           
			int marker_key = rs.getInt("marker_key");
			boolean is_expressed = rs.getString("is_expressed").equals("Yes");
			String structure_key = rs.getString("structure_key");
			String emapsId = rs.getString("emaps_id");
			String stage = rs.getString("theiler_stage");
			if(!markerResults.containsKey(marker_key)) {
				markerResults.put(marker_key,new ArrayList<Result>());
			}
			markerResults.get(marker_key).add(new Result(structure_key,emapsId,stage,is_expressed,false));
		}
		rs.close();
		logger.info(" - returning data for " + markerResults.size() + " markers");
	}

	// add results for the RNA-seq data for markers between the two given keys
	public void addRnaSeqResults(Integer startMarkerKey, Integer endMarkerKey, Map<Integer,List<Result>> markerResults) throws Exception {
		logger.info("Getting RNA-seq results (markers " + startMarkerKey + " to " + endMarkerKey + ")");
		String query = ""
			+ "select  "
                        + "  ht.marker_key, "
                        + "  case  "
                        + "    when ht.level = 'Below Cutoff' then 0 "
                        + "    else 1 "
                        + "  end as is_expressed, "
                        + "  te.term_key as structure_key, "
                        + "  t1.term as structure_printname, "
                        + "  t2.primary_id as emaps_id, "
                        + "  cs.theiler_stage "
                        + "from  "
                        + "  expression_ht_consolidated_sample_measurement ht, "
                        + "  expression_ht_consolidated_sample cs, "
			+ "  genotype g, "
                        + "  term_emap te, "
                        + "  term t1, "
                        + "  term t2 "
			+ "where ht.marker_key >= " + startMarkerKey
			+ "  and ht.marker_key < " + endMarkerKey
                        + "  and ht.consolidated_sample_key = cs.consolidated_sample_key "
                        + "  and cs.emapa_key = te.emapa_term_key "
                        + "  and cs.theiler_stage::int8 = te.stage "
                        + "  and cs.emapa_key = t1.term_key "
                        + "  and te.term_key = t2.term_key "
			+ "  and cs.genotype_key = g.genotype_key "
			+ "  and g.combination_1 is null "
			;
		ResultSet rs = ex.executeProto(query);
		logger.info(" - organizing them");
		while (rs.next()) {           
			int marker_key = rs.getInt("marker_key");
			boolean is_expressed = rs.getInt("is_expressed") == 1;
			String structure_key = rs.getString("structure_key");
			String emapsId = rs.getString("emaps_id");
			String stage = rs.getString("theiler_stage");
			if(!markerResults.containsKey(marker_key)) {
				markerResults.put(marker_key,new ArrayList<Result>());
			}
			markerResults.get(marker_key).add(new Result(structure_key,emapsId,stage,is_expressed,true));
		}
		rs.close();
		logger.info(" - returning data for " + markerResults.size() + " markers");
	}

	// get all the Results for markers between the two given keys
	public Map<Integer,List<Result>> getMarkerResults(Integer startKey, Integer endKey) throws Exception {
		Map<Integer,List<Result>> markerResults = new HashMap<Integer,List<Result>>();
		addClassicalResults(startKey, endKey, markerResults);
		addRnaSeqResults(startKey, endKey, markerResults);
		return markerResults;
	}
	
	private Set<Integer> toInts (Set<String> strs) {
		Set<Integer> ans = new HashSet();
		for (String s : strs) {
		    ans.add(Integer.valueOf(s));
		}
		return ans;
	}

	// build and return a solr document for the given marker key and its
	// (given) results, pulling data from the two maps as well
	public SolrInputDocument buildSolrDoc(
		Integer markerKey, 
		String markerID,
		List<Result> markerResults
		) throws Exception
	{

		String mrkKey = "" + markerKey;
		
		SolrInputDocument doc = new SolrInputDocument();
		// Add the single value fields
		doc.addField(GxdResultFields.KEY, mrkKey);
		doc.addField(GxdResultFields.MARKER_KEY, markerKey);
		doc.addField(GxdResultFields.MARKER_MGIID, markerID);
				
		// iterate this marker's results to build various search fields.

		Set<String> posCexact = new HashSet<String>();
		Set<String> posCanc   = new HashSet();
		Set<String> posRexact = new HashSet<String>();
		Set<String> posRanc   = new HashSet();

		Set<String> posCexactA = new HashSet<String>();
		Set<String> posCancA   = new HashSet();
		Set<String> posRexactA = new HashSet<String>();
		Set<String> posRancA   = new HashSet();

		for(Result result : markerResults) {
		    if(result.expressed) {
			String emapsKey = result.structureKey;
			Set<String> emapsAncestors = getEmapsAncestors(result.structureKey);
			String emapaKey = getEmapaKey(emapsKey);
			Set<String> emapaAncestors = getEmapaKeys(emapsAncestors);
			if (result.isRnaSeq) {
			    posRexact.add(emapsKey);
			    posRanc.addAll(emapsAncestors);
			    posRexactA.add(emapaKey);
			    posRancA.addAll(emapaAncestors);
			} else {
			    posCexact.add(emapsKey);
			    posCanc.addAll(emapsAncestors);
			    posCexactA.add(emapaKey);
			    posCancA.addAll(emapaAncestors);
			}
		    }
		} 
		doc.addField(GxdResultFields.PROF_POS_C_EXACT,   toInts(posCexact));
		doc.addField(GxdResultFields.PROF_POS_C_ANC,     toInts(posCanc));
		doc.addField(GxdResultFields.PROF_POS_C_EXACT_A, toInts(posCexactA));
		doc.addField(GxdResultFields.PROF_POS_C_ANC_A,   toInts(posCancA));

		doc.addField(GxdResultFields.PROF_POS_R_EXACT,   toInts(posRexact));
		doc.addField(GxdResultFields.PROF_POS_R_ANC,     toInts(posRanc));
		doc.addField(GxdResultFields.PROF_POS_R_EXACT_A, toInts(posRexactA));
		doc.addField(GxdResultFields.PROF_POS_R_ANC_A,   toInts(posRancA));

		return doc;
	}

	// main logic for building the index
	public void index() throws Exception
	{    
		List<Integer> markerKeys = getMarkerKeys();
		fillEmaps2Emapa();
		fillEmapsAncestors();
		Collection<SolrInputDocument> docs = new ArrayList<SolrInputDocument>();

		int startIndex = 0;
		int numMarkers = markerKeys.size();
		int chunkSize = 2000;	// number of markers to process at once
		int cacheSize = 1000;	// number of solr docs to keep in memory

		Integer startMarkerKey = 0;
		Integer endMarkerKey = 0;
                
		while (startIndex < numMarkers) {
			// get a slice of markers to work on
			startMarkerKey = markerKeys.get(startIndex);
			int endIndex = startIndex + chunkSize;

			if (endIndex >= numMarkers) {
				endMarkerKey = markerKeys.get(markerKeys.size() - 1);
			} else {
				endMarkerKey = markerKeys.get(endIndex);
			}

			Map<Integer,String> markerIDs = getMarkerIDs(startMarkerKey, endMarkerKey); 

			Map<Integer,List<Result>> markerResults = getMarkerResults(startMarkerKey, endMarkerKey);

			// now build & handle solr documents (one per marker)
			for (Integer markerKey : markerResults.keySet()) {
				docs.add(buildSolrDoc(markerKey, markerIDs.get(markerKey), markerResults.get(markerKey)));
				if (docs.size() > cacheSize) {
                                        logger.info(" - writing " + docs.size() + " docs.");
					writeDocs(docs);
					docs = new ArrayList<SolrInputDocument>();
				}
			}
			logger.info(" - built solr docs");

			// prepare for the next slice of markers
			startIndex = endIndex;
		} // end while (walking through chunks of markers)

		if (docs.size() > 0) {
                        logger.info(" - writing " + docs.size() + " docs.");
			writeDocs(docs);
		}
		commit();
	}

	// helper classes
	public class Result
	{
		public String structureKey;
		public String structureId;
		public String stage;
		public boolean expressed;
		public boolean isRnaSeq;

		public Result(String structureKey, String structureId, String stage, boolean expressed, boolean isrnaseq)
		{
			this.structureKey=structureKey;
			this.structureId=structureId;
			this.stage=stage;
			this.expressed=expressed;
			this.isRnaSeq = isrnaseq;
		}
	}
	public class Structure
	{
		public String structureId;
		public String stage;
		public Structure(String structureId,String stage)
		{
			this.structureId=structureId;
			this.stage=stage;
		}
		@Override
		public String toString() {
			return structureToSolrString(stage,structureId);
		}
	}

	public static String structureToSolrString(String stage,String structureId)
	{
		return "TS"+stage+":"+structureId;
	}
}
