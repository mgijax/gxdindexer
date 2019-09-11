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

import org.jax.mgi.gxddatamodel.GxdMarker;

// provides methods for fetching certain types of standard data, as a
// convenience to all the GXD indexers
public class Fetcher {

	private Logger log = Logger.getLogger(SQLExecutor.class);
	private SQLExecutor sql;
	private Map<String,Map<String,VocabTerm>> vocabs = new HashMap<String,Map<String,VocabTerm>>();
	private Map<String,GxdMarker> markers;
	
	// hide the default constructor
	private Fetcher() {}

	// must provide a SQLExecutor for database access
	public Fetcher(SQLExecutor sql) {
		this.sql = sql;
		log.info("Initialized " + this.toString());
	}

	// clear any cached data sets
	public void clearCaches() {
		this.vocabs = new HashMap<String,Map<String,VocabTerm>>();
		this.markers = null;
		log.info("Cleared caches in " + this.toString());
	}

	// get a GxdMarker object for each non-withdrawn mouse marker
	public Map<String,GxdMarker> getMouseMarkers() throws SQLException {
		// if we already looked them up, just return from cache
		if (this.markers != null) {
			return this.markers;
		}

		this.markers = new HashMap<String,GxdMarker>();

                String cmd = "select m.marker_key, m.symbol, m.name, m.primary_id, "
                        + " m.marker_type, m.marker_subtype, "
                        + " coalesce(coord.chromosome, cm.chromosome, band.chromosome) as chromosome, "
                        + " coord.start_coordinate::text, coord.end_coordinate::text, coord.strand, "
                        + " cm.cm_offset::text, band.cytogenetic_offset "
                        + "from marker m "
                        + "left outer join marker_location coord on ( "
                        + " m.marker_key = coord.marker_key "
                        + " and coord.location_type = 'coordinates') "
                        + "left outer join marker_location cm on ( "
                        + " m.marker_key = cm.marker_key "
                        + " and cm.location_type = 'centimorgans') "
                        + "left outer join marker_location band on ( "
                        + " m.marker_key = band.marker_key "
                        + " and band.location_type = 'cytogenetic') "
                        + "where m.organism = 'mouse' "
                        + " and m.status != 'withdrawn'";

                log.info("About to retrieve GxdMarkers");
                ResultSet rs = this.sql.executeProto(cmd);

                while (rs.next()) {
                        GxdMarker marker = new GxdMarker();
                        marker.setMarkerKey(rs.getInt("marker_key"));
                        marker.setSymbol(rs.getString("symbol"));
                        marker.setName(rs.getString("name"));
                        marker.setPrimaryID(rs.getString("primary_id"));
                        marker.setMarkerType(rs.getString("marker_type"));
                        marker.setMarkerSubType(rs.getString("marker_subtype"));
                        marker.setChromosome(rs.getString("chromosome"));
                        marker.setStartCoord(rs.getString("start_coordinate"));
                        marker.setEndCoord(rs.getString("end_coordinate"));
                        marker.setStrand(rs.getString("strand"));
                        marker.setCentimorgans(rs.getString("cm_offset"));
                        this.markers.put(marker.getPrimaryID(), marker);
                }
                rs.close();
                log.info(" - Cached " + this.markers.size() + " markers");
		return this.markers;
	}

	// get an ordered list of keys for official mouse markers
	public List<Integer> getMouseMarkerKeys() throws SQLException {
		List<Integer> keys = new ArrayList<Integer>();

		log.info("About to collect marker keys");
		String cmd = "select marker_key "
			+ "from marker "
			+ "where organism = 'mouse' and status = 'official' "
			+ "order by marker_key";

		ResultSet rs = this.sql.executeProto(cmd);
		while (rs.next()) {
			keys.add(rs.getInt("marker_key"));
		}
		rs.close();

		log.info(" - Got " + keys.size() + " marker keys");
		return keys;
	}

	// get a mapping from term ID to VocabTerm object for the given
	// vocabulary name
	public Map<String,VocabTerm> getVocabTerms(String vocabName) throws SQLException {
		// If we've already looked up terms for this vocab, then we can just 
		// return them from cache.
		if (this.vocabs.containsKey(vocabName)) {
			return this.vocabs.get(vocabName);
		}

		log.info("About to collect " + vocabName + " VocabTerms");
		HashMap<String,VocabTerm> terms = new HashMap<String,VocabTerm>();

		String cmd = "select t.term_key, t.term, t.primary_id, t.term_key, a.ancestor_primary_id "
			+ "from term t "
			+ "left outer join term_ancestor a on (t.term_key = a.term_key) "
			+ "where t.vocab_name = '" + vocabName + "' "
			+ " and t.is_obsolete = 0";

		ResultSet rs = this.sql.executeProto(cmd);

		while (rs.next()) {
			String termID = rs.getString("primary_id");
			if (!terms.containsKey(termID)) {
				VocabTerm term = new VocabTerm();
				term.setTermKey(rs.getInt("term_key"));
				term.setTermID(termID);
				term.setTerm(rs.getString("term"));
				terms.put(termID, term);
			}

			String ancestorID = rs.getString("ancestor_primary_id");
			if (ancestorID != null) {
				terms.get(termID).addAncestorID(ancestorID);
			}
		}

		rs.close();
		log.info(" = Got Map with " + terms.size() + " VocabTerms");

		// cache them for the future
		this.vocabs.put(vocabName, terms);
		return terms;
	}

	@Override
	public String toString() {
		return "Fetcher";
	}
}
