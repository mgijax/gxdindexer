package org.jax.mgi.gxdindexer.shr;

import java.util.Set;
import java.util.HashSet;

public class VocabTerm {

	private Integer termKey;
	private String term;
	private String termID;
	private Set<String> ancestorIDs;
	
	public Integer getTermKey() {
		return this.termKey;
	}

	public void setTermKey(Integer termKey) {
		this.termKey = termKey;
	}

	public String getTerm() {
		return this.term;
	}

	public void setTerm(String term) {
		this.term = term;
	}

	public String getTermID() {
		return this.termID;
	}

	public void setTermID(String termID) {
		this.termID = termID;
	}

	public Set<String> getAncestorIDs() {
		return this.ancestorIDs;
	}

	public void setAncestorIDs(Set<String> ids) {
		this.ancestorIDs = ids;
	}

	public void addAncestorID(String ancestorID) {
		if (this.ancestorIDs == null) {
			this.ancestorIDs = new HashSet<String>();
		}
		this.ancestorIDs.add(ancestorID);
	}

	@Override
	public String toString() {
		return "VocabTerm[" + this.termID + ", " + this.term + "]";
	}

}
