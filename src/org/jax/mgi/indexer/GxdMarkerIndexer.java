package org.jax.mgi.gxdindexer.indexer;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.ArrayList;
import java.util.Set;
import java.util.HashSet;
import java.util.Map;
import java.util.HashMap;
import java.util.Arrays;
import org.apache.solr.common.SolrInputDocument;

import org.jax.mgi.shr.fe.IndexConstants;
import org.jax.mgi.shr.fe.indexconstants.GxdResultFields;
import org.jax.mgi.shr.fe.query.SolrLocationTranslator;

import org.jax.mgi.gxddatamodel.GxdMarker;
import org.jax.mgi.gxdindexer.shr.VocabTerm;
import org.jax.mgi.gxdindexer.shr.Fetcher;
import org.jax.mgi.gxdindexer.shr.VocabTermCache;
import org.jax.mgi.gxdindexer.shr.MarkerMPCache;
import org.jax.mgi.gxdindexer.shr.MarkerGOCache;
import org.jax.mgi.gxdindexer.shr.MarkerDOCache;
import org.jax.mgi.gxdindexer.shr.MarkerTypeCache;

public class GxdMarkerIndexer extends Indexer {

	// list of Solr documents collected in memory
	ArrayList<SolrInputDocument> docs = new ArrayList<SolrInputDocument>();

	// used to fetch some standard items for cache
	private Fetcher fetcher;

	// cache of basic marker data and marker synonyms for all non-withdrawn
	// mouse markers
	private Map<String, GxdMarker> markers;
	private Map<String, List<String>> markerSynonyms;

	// caches of basic vocabulary term data
	private VocabTermCache goTerms;
	private VocabTermCache doTerms;
	private VocabTermCache mpTerms;
	private VocabTermCache emapaTerms;
	private VocabTermCache emapsTerms;

	// caches of annotated terms (and their parents) per marker
        public MarkerMPCache markerMpCache = null;
        public MarkerGOCache markerGoCache = null;
        public MarkerDOCache markerDoCache = null;
        public MarkerTypeCache markerTypeCache = null;

	// ordered list of marker keys for official mouse markers that have
	// expression data (classical, RNA-Seq, lit index)
	private List<Integer> gxdMarkerKeys;

	// number of markers to process in a batch
	private int batchSize = 2500;

	// map of mouse markers we're currently working on (used to build docs)
	private Map<String,MarkerInfo> markerInfo;

	// list of age values from the query form (as doubles)
	private List<Double> ageDoubles = Arrays.asList(0.5, 1.0, 1.5, 2.0,
		2.5, 3.0, 3.5, 4.0, 4.5, 5.0, 5.5, 6.0, 6.5, 7.0, 7.5, 8.0,
		8.5, 9.0, 9.5, 10.0, 10.5, 11.0, 11.5, 12.0, 12.5, 13.0, 13.5,
		14.0, 14.5, 15.0, 15.5, 16.0, 16.5, 17.0, 17.5, 18.0, 18.5,
		19.0, 19.5, 20.0, 20.5);

	// list of age values (as strings), parallel to 
	private List<String> ageStrings = Arrays.asList("0.5", "1", "1.5", "2",
		"2.5", "3", "3.5", "4", "4.5", "5", "5.5", "6", "6.5", "7",
		"7.5", "8", "8.5", "9", "9.5", "10", "10.5", "11", "11.5", "12",
		"12.5", "13", "13.5", "14", "14.5", "15", "15.5", "16", "16.5",
		"17", "17.5", "18", "18.5", "19", "19.5", "20", "20.5");



	private HashMap<Integer, String> variationMap = new HashMap<Integer, String>();
	
	private HashMap<Integer, String> functionMap = new HashMap<Integer, String>();
	private HashMap<Integer, String> markerMap = new HashMap<Integer, String>();
	private HashMap<Integer, String> strainMap = new HashMap<Integer, String>();
	
	private HashMap<Integer, ArrayList<String>> strainsMap = new HashMap<Integer, ArrayList<String>>();
	private HashMap<Integer, ArrayList<String>> functionClassesMap = new HashMap<Integer, ArrayList<String>>();
	private HashMap<Integer, ArrayList<String>> markersMap = new HashMap<Integer, ArrayList<String>>();

	private StringBuffer excludeFunctionClasses = new StringBuffer();
	
	public GxdMarkerIndexer() {
		super("gxdMarker");
	}

	// populate caches of standard objects
	private void buildCaches() throws Exception {
		this.fetcher = new Fetcher(ex);
		this.markers = fetcher.getMouseMarkers();
		this.gxdMarkerKeys = fetcher.getGxdMouseMarkerKeys();
		this.goTerms = fetcher.getVocabTermCache("GO");
		this.doTerms = fetcher.getVocabTermCache("Disease Ontology");
		this.mpTerms = fetcher.getVocabTermCache("Mammalian Phenotype");
		this.emapaTerms = fetcher.getVocabTermCache("EMAPA");
		this.emapsTerms = fetcher.getVocabTermCache("EMAPS");
		this.markerMpCache = new MarkerMPCache();
		this.markerGoCache = new MarkerGOCache();
		this.markerDoCache = new MarkerDOCache();
		this.markerTypeCache = new MarkerTypeCache();
		this.markerSynonyms = fetcher.getMouseMarkerSynonyms();
	}

	// get the MarkerInfo object for the marker with the given ID, or a new
	// one if there's not already one
	private MarkerInfo getMarkerInfo(String markerID) {
		if (!this.markerInfo.containsKey(markerID)) {
			this.markerInfo.put(markerID, new MarkerInfo());
		}
		return this.markerInfo.get(markerID);
	}

	// Get the set of DPC age strings that can be selected on the query form
	// that would be within the range of ages specified.
	private Set<String> getAges(Double ageMin, Double ageMax) {
		Set<String> ages = new HashSet<String>();

		if (ageMin > 20.5) {
			// If the min age is postnatal, then we can quit here.
			ages.add("Postnatal");
		} else {
			if (ageMax > 20.5) {
				// ageMin is embryonic, but ageMax is postnatal.
				// Deal with postnatal here.
				ages.add("Postnatal");
			}
			ages.add("Embryonic");

			// Potential performance improvement:  Rather than
			// iterating over the whole list of ageDoubles, we
			// could use a binary search to find the lowest &
			// highest indices that match the range, then just
			// iterate over that part of the lists.

			// Deal with embryonic ages.
			for (int i = 0; i < ageDoubles.size(); i++) {
				double dblAge = ageDoubles.get(i);
				if ((ageMin <= dblAge) && (dblAge <= ageMax)) {
					ages.add(ageStrings.get(i));
				}
			}
		}
		return ages;
	}

	// retrieve the classical data for the set (start <= key < end)
	// and fill it in instance-level variables
	private void fillClassicalData(int startKey, int endKey) throws SQLException {
		logger.info(" - about to retrieve classical data");
		String cmd = "select m.primary_id as marker_id, t.primary_id as emaps_id, "
			+ " ers.assay_type, ers.theiler_stage, ers.age_min, ers.age_max, "
			+ " ers.is_expressed, ers.jnum_id, ers.is_wild_type, ers.genotype_key, "
			+ " gm.primary_id as mutated_marker_id, ms.by_symbol, ms.by_location "
			+ "from expression_result_summary ers "
			+ "inner join marker m on (ers.marker_key = m.marker_key) "
			+ "inner join marker_sequence_num ms on (ers.marker_key = ms.marker_key) "
			+ "inner join term t on (ers.structure_key = t.term_key) "
			+ "left outer join marker_to_genotype mtg on (ers.genotype_key = mtg.genotype_key) "
			+ "left outer join marker gm on (mtg.marker_key = gm.marker_key) "
			+ "where ers.marker_key >= " + startKey
			+ " and ers.marker_key < " + endKey;

		ResultSet rs = ex.executeProto(cmd);
		while (rs.next()) {
			String markerID = rs.getString("marker_id");
			MarkerInfo marker = getMarkerInfo(markerID);
			String mutatedMarkerID = rs.getString("mutated_marker_id");

			// If this is the first time we've seen this marker, we
			// need to populate its basic data.
			if (marker.bySymbol == null) {
				marker.bySymbol = rs.getInt("by_symbol");
				marker.byLocation = rs.getInt("by_location");
				marker.gxdMarker = markers.get(markerID);
			}

			// Now populate the data that change by result.
			marker.emapsID.add(rs.getString("emaps_id"));
			marker.assayType.add(rs.getString("assay_type"));
			marker.theilerStage.add(rs.getString("theiler_stage"));
			marker.isExpressed.add(rs.getString("is_expressed"));
			marker.jnumID.add(rs.getString("jnum_id"));
			marker.ages.addAll(getAges(rs.getDouble("age_min"), rs.getDouble("age_max")));

			if (mutatedMarkerID != null) {
				marker.mutatedMarkerID = new HashSet<String>();
				marker.mutatedMarkerID.add(mutatedMarkerID);
			}

			String isWildType = "mutant";
			if ("1".equals(rs.getString("is_wild_type")) || "-1".equals(rs.getString("genotype_key"))) {
				isWildType = "wild type";
			}
			marker.isWildType.add(isWildType);
		}
		rs.close();
		logger.info(" - finished classical data");
	}

	// retrieve the RNA-Seq data for the set (start <= key < end)
	// and fill it in instance-level variables
	private void fillRnaSeqData(int startKey, int endKey) throws SQLException {
		logger.info(" - about to retrieve RNA-Seq data");
		String cmd = "select m.primary_id as marker_id, t.primary_id as emaps_id, "
			+ " cs.theiler_stage, cs.age_min, cs.age_max, gg.combination_1, "
			+ " sm.level as detection_level, exp.primary_id as jnum_id, "
			+ " cs.genotype_key, gm.primary_id as mutated_marker_id, "
			+ " ms.by_symbol, ms.by_location "
			+ "from expression_ht_consolidated_sample_measurement sm "
			+ "inner join expression_ht_consolidated_sample cs on (sm.consolidated_sample_key = cs.consolidated_sample_key) "
			+ "inner join marker m on (sm.marker_key = m.marker_key) "
			+ "inner join marker_sequence_num ms on (sm.marker_key = ms.marker_key) "
			+ "inner join term_emap em on (cs.theiler_stage::integer = em.stage and cs.emapa_key = em.emapa_term_key) "
			+ "inner join term t on (em.term_key = t.term_key) "
			+ "inner join expression_ht_experiment exp on (cs.experiment_key = exp.experiment_key) "
			+ "inner join genotype gg on (cs.genotype_key = gg.genotype_key) "
			+ "left outer join marker_to_genotype mtg on (cs.genotype_key = mtg.genotype_key) "
			+ "left outer join marker gm on (mtg.marker_key = gm.marker_key) "
			+ "where m.marker_key >= " + startKey + " "
			+ " and m.marker_key < " + endKey;

		ResultSet rs = ex.executeProto(cmd);
		while (rs.next()) {
			String markerID = rs.getString("marker_id");
			MarkerInfo marker = getMarkerInfo(markerID);
			String mutatedMarkerID = rs.getString("mutated_marker_id");
			String level = rs.getString("detection_level");

			// If this is the first time we've seen this marker, we
			// need to populate its basic data.
			if (marker.bySymbol == null) {
				marker.bySymbol = rs.getInt("by_symbol");
				marker.byLocation = rs.getInt("by_location");
				marker.gxdMarker = markers.get(markerID);
			}

			// Now populate the data that change by result.
			marker.emapsID.add(rs.getString("emaps_id"));
			marker.assayType.add("RNA-Seq");
			marker.theilerStage.add(rs.getString("theiler_stage"));
			marker.jnumID.add(rs.getString("jnum_id"));
			marker.ages.addAll(getAges(rs.getDouble("age_min"), rs.getDouble("age_max")));

			String isExpressed = "Unknown/Ambiguous";
			if ("Below Cutoff".equals(level)) {
				isExpressed = "No";
			} else {
				isExpressed = "Yes";
			}
			marker.isExpressed.add(isExpressed);

			if (mutatedMarkerID != null) {
				marker.mutatedMarkerID = new HashSet<String>();
				marker.mutatedMarkerID.add(mutatedMarkerID);
			}

			// Genotypes with no allele pairs to be wild-type.
			String isWildType = "mutant";
			if (null == rs.getString("combination_1")) {
				isWildType = "wild type";
			}
			marker.isWildType.add(isWildType);
		}
		rs.close();
		logger.info(" - finished RNA-Seq data");
	}

	// go through the current set of markers in this.markerInfo and build
	// them into Solr documents in this.solrDocs
	public void buildSolrDocs() {
		for (String markerID : this.markerInfo.keySet()) {
			MarkerInfo marker = this.markerInfo.get(markerID);
			String markerKey = marker.gxdMarker.getMarkerKey().toString();
			SolrInputDocument doc = new SolrInputDocument();

			// basic marker fields
			doc.addField(GxdResultFields.KEY, markerKey);
			doc.addField(GxdResultFields.MARKER_KEY, marker.gxdMarker.getMarkerKey());
			doc.addField(GxdResultFields.MARKER_MGIID, marker.gxdMarker.getPrimaryID());
			doc.addField(GxdResultFields.MARKER_SYMBOL, marker.gxdMarker.getSymbol());
			doc.addField(GxdResultFields.MARKER_NAME, marker.gxdMarker.getName());
			doc.addField(GxdResultFields.NOMENCLATURE, marker.gxdMarker.getSymbol());
			doc.addField(GxdResultFields.NOMENCLATURE, marker.gxdMarker.getName());
			doc.addField(GxdResultFields.MARKER_TYPE, marker.gxdMarker.getMarkerSubType());
			doc.addField(IndexConstants.MRK_BY_SYMBOL, marker.bySymbol);
			doc.addField(GxdResultFields.M_BY_LOCATION, marker.byLocation);

			if (this.markerSynonyms.containsKey(markerKey)) {
				for (String synonym : markerSynonyms.get(markerKey)) {
					doc.addField(GxdResultFields.NOMENCLATURE, synonym);
				}
			}

			// add fields for filtering by marker-associated vocabularies
			for (String mpTerm : markerMpCache.getTerms(markerKey)) {
				doc.addField(GxdResultFields.MP_HEADERS, mpTerm);
			}
			for (String goTerm : markerGoCache.getTerms(markerKey)) {
				doc.addField(GxdResultFields.GO_HEADERS, goTerm);
			}
			for (String doTerm : markerDoCache.getTerms(markerKey)) {
				doc.addField(GxdResultFields.DO_HEADERS, doTerm);
			}
			for (String featureType : markerTypeCache.getTerms(markerKey)) {
				doc.addField(GxdResultFields.FEATURE_TYPES, featureType);
			}

			// marker location fields

			String startCoord = marker.gxdMarker.getStartCoord();
			String endCoord = marker.gxdMarker.getEndCoord();

			doc.addField(GxdResultFields.CHROMOSOME, marker.gxdMarker.getChromosome());
			doc.addField(GxdResultFields.CENTIMORGAN, marker.gxdMarker.getCentimorgans());
			doc.addField(GxdResultFields.START_COORD, startCoord);
			doc.addField(GxdResultFields.END_COORD, endCoord);
			doc.addField(GxdResultFields.STRAND, marker.gxdMarker.getStrand());

			if ((startCoord != null) && (endCoord != null)) {
				String spatialString = SolrLocationTranslator.getIndexValue(marker.gxdMarker.getChromosome(), Long.parseLong(startCoord), Long.parseLong(endCoord), true);
				if ((spatialString != null) && !"".equals(spatialString)) {
					doc.addField(GxdResultFields.MOUSE_COORDINATE, spatialString);
				}
			}

			// possibly multi-valued fields from the results
			doc.addField(GxdResultFields.RESULT_TYPE, marker.assayType);
			doc.addField(GxdResultFields.ASSAY_TYPE, marker.assayType);
			doc.addField(GxdResultFields.THEILER_STAGE, marker.theilerStage);
			doc.addField(GxdResultFields.EMAPS_ID, marker.emapsID);
			doc.addField(GxdResultFields.IS_EXPRESSED, marker.isExpressed);
			doc.addField(GxdResultFields.DETECTION_LEVEL, marker.isExpressed);
			doc.addField(GxdResultFields.AGES, marker.ages);
			doc.addField(GxdResultFields.IS_WILD_TYPE, marker.isWildType);
			doc.addField(GxdResultFields.JNUM, marker.jnumID);

			docs.add(doc);
			if (docs.size() >= batchSize) {
				writeDocs(docs);
				docs = new ArrayList<SolrInputDocument>();
			}
		}
	}

	@Override
	public void index() throws Exception {
		// build caches of standard data items (markers, vocab terms)
		buildCaches();

		// We have an ordered list of marker keys in this.gxdMarkerKeys.
		// Step through this in batches of size this.batchSize.
		// For each batch:
		// 1. get the classical data and build into marker-centric
		// 	objects
		// 2. get the RNA-Seq data and add to (or build) marker-centric
		// 	objects

		// Then iterate over this batch's marker-centric objects:
		// 1. add annotation info for each marker
		// 2. build Solr documents
		// 3. send to Solr

		int markerCount = this.gxdMarkerKeys.size();
		logger.info("Working with " + markerCount + " markers");

		int batchNum = 1;
		String batchCount = new Double(1 + Math.ceil(markerCount / this.batchSize)).toString().replaceAll("\\..*", "");

		// indexes into this.gxdMarkerKeys
		int startPos = 0;	// start of this slice
		int endPos = 0;		// end of this slice; will initialize in loop
		// marker keys at the corresponding positions
		// Note: slice is >= startKey and < endKey
		int startKey = this.gxdMarkerKeys.get(startPos);
		int endKey = startKey + 1;

		while (startPos < markerCount) {
			endPos = startPos + this.batchSize;

			// For most slices, we can look up the marker key
			// from the array.  For the last one, it's too far.
			// In that case, just go beyond the last one.
			if (endPos < markerCount) {
				endKey = this.gxdMarkerKeys.get(endPos);
			} else {
				endKey = 1 + this.gxdMarkerKeys.get(this.gxdMarkerKeys.size() - 1);
			}

			logger.info("Starting batch " + batchNum + " of " + batchCount);
			logger.info(" - " + startKey + " <= markerKey < " + endKey);
			
			this.markerInfo = new HashMap<String,MarkerInfo>();
			fillClassicalData(startKey, endKey);
			fillRnaSeqData(startKey, endKey);
			buildSolrDocs();

			logger.info(" - finished batch " + batchNum);

			startPos = endPos;
			startKey = endKey;
			batchNum++;
		}
		ex.cleanup();
		logger.info("Closed database connection");

		if (docs.size() > 0) {
			writeDocs(docs);
			logger.info("Wrote last docs to Solr");
		}
		commit();
		logger.info("Closed Solr connections");
	} // end index() method

// Is: container for marker data that will go into making up our Solr documents.
// Note: Instance variables are public to save using setters/getters.
private class MarkerInfo {
	public Integer bySymbol;
	public Integer byLocation;
	public GxdMarker gxdMarker;
	public Set<String> emapsID = new HashSet<String>();
	public Set<String> assayType = new HashSet<String>();
	public Set<String> theilerStage = new HashSet<String>();
	public Set<String> ages = new HashSet<String>();
	public Set<String> isExpressed = new HashSet<String>();
	public Set<String> jnumID = new HashSet<String>();
	public Set<String> isWildType = new HashSet<String>();
	public Set<String> mutatedMarkerID = new HashSet<String>();
} // end inner class MarkerInfo

} // end class GxdMarkerIndexer
