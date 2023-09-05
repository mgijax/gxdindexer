package org.jax.mgi.gxdindexer.shr;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.HashSet;
import java.sql.ResultSet;

/* Is: a TermAssociationCache that maps from each mouse marker key to a list of associated GO header terms
 */
public class MarkerGOCache extends TermAssociationCache {
	// sets of possible headers for each DAG
	private Set<String> ccHeaders = null;	// cellular component
	private Set<String> bpHeaders = null;	// biological process
	private Set<String> mfHeaders = null;	// molecular function

	// initialize this cache upon instantiation of the object, propagating any Exception
	// raised in the initialization process
	public MarkerGOCache() throws Exception {
		String cmd = "select m.marker_key as object_key, h.heading_abbreviation as term "
			+ "from marker m, marker_grid_cell c, marker_grid_heading h "
			+ "where m.organism = 'mouse' and m.status = 'official' "
			+ " and m.marker_key = c.marker_key "
			+ " and c.heading_key = h.heading_key "
			+ " and h.grid_name in ('Molecular Function', 'Cellular Component', 'Biological Process') "
			+ " and c.value > 0 "
			+ "order by 1, 2";
		this.populate(cmd);
	}

	// populate the three caches of GO header terms (one per DAG), so we
	// can filter out an individual marker's GO headers into the categories
	private void loadFilterCaches() throws Exception {
		bpHeaders = new HashSet<String>();
		ccHeaders = new HashSet<String>();
		mfHeaders = new HashSet<String>();

		String cmd = "select heading_abbreviation, grid_name "
			+ "from marker_grid_heading "
			+ "where grid_name in ('Molecular Function', 'Cellular Component', 'Biological Process')";

		SQLExecutor sql = new SQLExecutor();
		ResultSet rs = sql.executeProto(cmd, cursorLimit);
		while (rs.next()) {
			String grid = rs.getString("grid_name");
			String heading = rs.getString("heading_abbreviation");

			if ("Molecular Function".equals(grid)) {
				mfHeaders.add(heading);
			} else if ("Biological Process".equals(grid)) {
				bpHeaders.add(heading);
			} else {
				ccHeaders.add(heading);
			}
		}
		rs.close();
	}

	// look up the full set of GO headers for 'markerKey' and filter it down
	// to return only those appropriate for the specified DAG
	private List<String> filter(String markerKey, String dag) throws Exception {
		// If we've not yet loaded these caches, do it.
		if (bpHeaders == null) {
			loadFilterCaches();
		}

		List<String> selected = new ArrayList<String>();
		Set<String> dagHeaders = null;
		if ("BP".equals(dag)) {
			dagHeaders = bpHeaders;
		} else if ("CC".equals(dag)) {
			dagHeaders = ccHeaders;
		} else {
			dagHeaders = mfHeaders;
		}

		for (String s : this.getTerms(markerKey)) {
			if (dagHeaders.contains(s)) {
				selected.add(s);
			} 
		}
		return selected;
	}

	// return the marker's headers for the molecular function DAG
	public List<String> getTermsMF(String markerKey) throws Exception {
		return filter(markerKey, "MF");
	}

	// return the marker's headers for the cellular component DAG
	public List<String> getTermsCC(String markerKey) throws Exception {
		return filter(markerKey, "CC");
	}

	// return the marker's headers for the biological process DAG
	public List<String> getTermsBP(String markerKey) throws Exception {
		return filter(markerKey, "BP"); }
}
