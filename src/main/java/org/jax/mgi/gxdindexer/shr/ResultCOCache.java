package org.jax.mgi.gxdindexer.shr;

/* Is: a TermAssociationCache that maps from each result key to a list of associated cell ontology header terms
 */
public class ResultCOCache extends TermAssociationCache {
	// initialize this cache upon instantiation of the object, propagating any Exception
	// raised in the initialization process
	public ResultCOCache() throws Exception {
		String cmd = "select ct.result_key as object_key, tth.label as term "
			+ "from expression_result_cell_type ct, term t1, term_to_header tth, term t2 "
			+ "where ct.cell_type = t1.term "
			+ " and t1.term_key = tth.term_key "
			+ " and tth.header_term_key = t2.term_key "
			+ " and tth.accid != 'CL:0000000' "
			+ "order by 1, 2";
		this.populate(cmd);
	}
}
