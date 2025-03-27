package org.jax.mgi.gxdindexer;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

import org.apache.solr.common.SolrInputDocument;
import org.jax.mgi.shr.fe.indexconstants.GxdResultFields;

/**
 * GxdConsolidatedSampleIndexer This index is intended to be a lookup for
 * consolidated samples for RNA-Seq data
 */

public class GxdConsolidatedSampleIndexer extends Indexer {
	public GxdConsolidatedSampleIndexer() {
		super("gxdConsolidatedSample");
	}

	public void index() throws Exception {
		List<SolrInputDocument> docs = new ArrayList<SolrInputDocument>();
		String cmd =
			"with counts as ( " +
			"select sm.consolidated_sample_key, count(*) as bioreplicateCount " +
			"from expression_ht_sample_map sm " +
			"group by  sm.consolidated_sample_key " +
			") " +
			"select s.consolidated_sample_key, t.primary_id as structure_exact, " +
			" s.theiler_stage, g.background_strain, g.combination_1 as genotype, " + 
			" e.primary_id as assay_id, s.age, t.term as printname, s.sex, c.bioreplicateCount " + 
			"from expression_ht_consolidated_sample s	" +
			"inner join genotype g on (s.genotype_key = g.genotype_key) " + 
			"inner join expression_ht_experiment e on (s.experiment_key = e.experiment_key) " + 
			"inner join term t on (s.emapa_key = t.term_key) " +
			"inner join counts c on (s.consolidated_sample_key = c.consolidated_sample_key)";

		ResultSet rs = ex.executeProto(cmd);
		while (rs.next()) {
			SolrInputDocument doc = new SolrInputDocument();
			doc.addField(GxdResultFields.CONSOLIDATED_SAMPLE_KEY, rs.getString("consolidated_sample_key"));
			doc.addField(GxdResultFields.STRUCTURE_EXACT, rs.getString("structure_exact"));
			doc.addField(GxdResultFields.THEILER_STAGE, rs.getString("theiler_stage"));
			doc.addField(GxdResultFields.STRAIN, rs.getString("background_strain"));
			doc.addField(GxdResultFields.GENOTYPE, rs.getString("genotype"));
			doc.addField(GxdResultFields.ASSAY_MGIID, rs.getString("assay_id"));
			doc.addField(GxdResultFields.AGE, rs.getString("age"));
			doc.addField(GxdResultFields.STRUCTURE_PRINTNAME, rs.getString("printname"));
			doc.addField(GxdResultFields.SEX, rs.getString("sex"));
			doc.addField(GxdResultFields.BIOREPLICATE_COUNT, rs.getString("bioreplicateCount"));
			docs.add(doc);
		}
		writeDocs(docs);
		rs.close();

		commit();
		logger.info("load completed");
	}
}
