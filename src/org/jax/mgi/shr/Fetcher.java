package org.jax.mgi.gxdindexer.shr;

import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;
import java.util.Set;
import java.util.HashSet;
import java.sql.ResultSet;
import java.sql.SQLException;
import org.apache.log4j.Logger;

// provides methods for fetching certain types of standard data, as a
// convenience to all the GXD indexers
public class Fetcher {

	private Logger log = Logger.getLogger(SQLExecutor.class);
	private SQLExecutor sql;
	private Map<String,VocabTermCache> vocabs = new HashMap<String,VocabTermCache>();
	private Map<String,List<String>> markerSynonyms;
	
	// hide the default constructor
	private Fetcher() {}

	// must provide a SQLExecutor for database access
	public Fetcher(SQLExecutor sql) {
		this.sql = sql;
		log.info("Initialized " + this.toString());
	}

	// clear any cached data sets
	public void clearCaches() {
		this.vocabs = new HashMap<String,VocabTermCache>();
		this.markerSynonyms = null;
		log.info("Cleared caches in " + this.toString());
	}

	// get a list of synonyms for each non-withdrawn mouse marker 
	public Map<String, List<String>> getMouseMarkerSynonyms() throws SQLException {
		// if we already looked them up, just return from cache
		if (this.markerSynonyms != null) {
			return this.markerSynonyms;
		}

		this.markerSynonyms = new HashMap<String, List<String>>();

                String cmd = "select distinct msn.marker_key, msn.term "
                        + "from marker_searchable_nomenclature msn "
                        + "inner join marker m on (msn.marker_key = m.marker_key) "
                        + "where msn.term_type in ('synonym','related synonym') "
                        + " and m.organism = 'mouse' "
                        + " and m.status != 'withdrawn' ";

                log.info("About to retrieve marker synonyms");
                ResultSet rs = this.sql.executeProto(cmd);

                while (rs.next()) {
			String markerKey = rs.getString("marker_key");
			if (!markerSynonyms.containsKey(markerKey)) {
				markerSynonyms.put(markerKey, new ArrayList<String>());
			}
			markerSynonyms.get(markerKey).add(rs.getString("term"));
                }
                rs.close();
                log.info(" - Cached synonyms for " + this.markerSynonyms.size() + " markers");
		return this.markerSynonyms;
	}

	// get an ordered list of keys for official mouse markers that have
	// expression data (classical, RNA-Seq, or literature)
	public List<Integer> getGxdMouseMarkerKeys() throws SQLException {
		List<Integer> keys = new ArrayList<Integer>();

		log.info("About to collect marker keys");
		String cmd = "select m.marker_key "
			+ "from marker m "
			+ "where m.organism = 'mouse' and m.status = 'official' "
			+ "and ( "
			+ "exists (select 1 from expression_result_summary s "
			+ "  where m.marker_key = s.marker_key) "
			+ "or exists (select 1 from expression_ht_consolidated_sample_measurement s "
			+ "  where m.marker_key = s.marker_key) "
			+ "or exists (select 1 from expression_index s " 
			+ "  where m.marker_key = s.marker_key) "
			+ ")"
			+ "order by marker_key";

		ResultSet rs = this.sql.executeProto(cmd);
		while (rs.next()) {
			keys.add(rs.getInt("marker_key"));
		}
		rs.close();

		log.info(" - Got " + keys.size() + " marker keys");
		return keys;
	}

	// get a VocabTermCache object for the given vocabulary name
	public VocabTermCache getVocabTermCache(String vocabName) throws Exception {
		if (!this.vocabs.containsKey(vocabName)) {
			this.vocabs.put(vocabName, new VocabTermCache(vocabName, this.sql));
		}
		return this.vocabs.get(vocabName);
	}

	@Override
	public String toString() {
		return "[Fetcher " + this.vocabs.size() + " vocabs]";
	}
}
