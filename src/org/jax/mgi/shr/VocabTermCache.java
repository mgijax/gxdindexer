package org.jax.mgi.gxdindexer.shr;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/* Is: a cache of VocabTerm objects that are part of a single vocabulary,
 * 	including information about their ancestors
 * Has: a cache of 0-n VocabTerms cached for a single vocabulary
 * Does: knows how to initialize itself and look up terms by primary ID
 * Notes: is abstract to force the initialize method to be defined in a subclass
 */
public class VocabTermCache {
	/*--- instance variables ---*/

	// name of the vocabulary contained in this cache
	private String vocabName;

	// cache of ID : VocabTerm for this vocabulary
	private Map<String,VocabTerm> terms;

	// for logging messages
	public Logger logger = LoggerFactory.getLogger(this.getClass());

	// how many database records to handle at once
	protected int cursorLimit = 50000;
	
	/*--- public methods ---*/
	
	// public constructor - identify what vocab this should contain
	public VocabTermCache(String vocabName, SQLExecutor sql) throws Exception {
		this.vocabName = vocabName;
		this.populate(sql);
	}
	
	// return true if we have a terms for the given term ID, false if not
	public boolean hasTerm(String accID) {
		return this.terms.containsKey(accID);
	}
	
	// return the term associated with the given primary ID, or null if none
	public VocabTerm getTerm(String accID) {
		if (!this.terms.containsKey(accID)) {
			return null;
		}
		return this.terms.get(accID);
	}

	// return VocabTerms that are ancestors of the given term ID
	public List<VocabTerm> getAncestors(String termID) {
		List<VocabTerm> ancestors = new ArrayList<VocabTerm>();
		VocabTerm term = this.terms.get(termID);
		if (term != null) {
			for (String ancestorID : term.getAncestorIDs()) {
				VocabTerm t = this.terms.get(ancestorID);
				if (t != null) {
					ancestors.add(t);
				}
			}
		}
		return ancestors;
	}

	// simple string to represent this object
	public String toString() {
		return "[VocabTermCache " + this.vocabName + "]";
	}

	/*--- private methods ---*/

	// populate this cache with data from the database
	private void populate(SQLExecutor sql) throws Exception {
		logger.info("initializing " + this.toString());
		this.terms = new HashMap<String,VocabTerm>();

		// gather VocabTerm object for each term ID
		String cmd = "select t.term_key, t.term, t.primary_id, t.term_key "
			+ "from term t "
			+ "where t.vocab_name = '" + vocabName + "' "
			+ " and t.is_obsolete = 0";
			
		ResultSet rs = sql.executeProto(cmd, cursorLimit);
		while (rs.next()) {
			String termID = rs.getString("primary_id");
			VocabTerm term = new VocabTerm();
			term.setTermKey(rs.getInt("term_key"));
			term.setTermID(termID);
			term.setTerm(rs.getString("term"));
			this.terms.put(termID, term);
		}
		rs.close(); 
		logger.info(" - got " + this.terms.size() + " terms");

		// gather ancestor IDs for each term ID (self-referential)
		String ancestorCmd = "select t.primary_id, a.ancestor_primary_id "
			+ "from term t, term_ancestor a "
			+ "where t.term_key = a.term_key "
			+ " and t.is_obsolete = 0 "
			+ " and t.vocab_name = '" + vocabName + "'";

		ResultSet rs2 = sql.executeProto(ancestorCmd, cursorLimit);
		while (rs2.next()) {
			String termID = rs.getString("term_id");
			if (this.terms.containsKey(termID)) {
				this.terms.get(termID).addAncestorID(rs.getString("ancestor_primary_id"));
			}
		}
		rs2.close();
		logger.info(" - got ancestors");
	}
}
