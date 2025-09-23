package org.jax.mgi.gxdindexer;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.jax.mgi.shr.fe.indexconstants.GxdResultFields;

/**
 * GxdConsolidatedSampleIndexer This index is intended to be a lookup for
 * consolidated samples for RNA-Seq data
 */

public class GxdConsolidatedSampleIndexer extends Indexer {
	public GxdConsolidatedSampleIndexer() {
		super("gxd_consolidated_sample");
	}

	public void index() throws Exception {
		List<Map<String, Object>> docs = new ArrayList<>();
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
			Map<String, Object> doc = new HashMap<>();
			doc.put(GxdResultFields.CONSOLIDATED_SAMPLE_KEY, rs.getString("consolidated_sample_key"));
			doc.put(GxdResultFields.STRUCTURE_EXACT, rs.getString("structure_exact"));
			doc.put(GxdResultFields.THEILER_STAGE, rs.getString("theiler_stage"));
			doc.put(GxdResultFields.STRAIN, rs.getString("background_strain"));
			doc.put(GxdResultFields.GENOTYPE, rs.getString("genotype"));
			doc.put(GxdResultFields.ASSAY_MGIID, rs.getString("assay_id"));
			doc.put(GxdResultFields.AGE, rs.getString("age"));
			doc.put(GxdResultFields.STRUCTURE_PRINTNAME, rs.getString("printname"));
			doc.put(GxdResultFields.SEX, rs.getString("sex"));
			doc.put(GxdResultFields.BIOREPLICATE_COUNT, rs.getString("bioreplicateCount"));
			docs.add(doc);
		}
		writeDocs(docs);
		rs.close();

		commit();
		logger.info("load completed");
	}

	@Override
	protected String getIndexMappingJson() {
		String mappingJson = """
		{
		  "settings": {
			"index.mode": "lookup",		  
		    "number_of_shards": 1,
		    "number_of_replicas": 0,
		    "refresh_interval": "10s",
		    "analysis": {
		      "analyzer": {
		        "lowercase_keyword": {
		          "tokenizer": "keyword",
		          "filter": ["lowercase"]
		        },
		        "path_hierarchy": {
		          "tokenizer": "path_hierarchy"
		        },
		        "custom_english": {
		          "tokenizer": "standard",
		          "filter": [
		            "lowercase",
		            "custom_stop"
		          ]
		        }
		      },
		      "filter": {
		        "custom_stop": {
		          "type": "stop",
		          "stopwords": ["and", "from", "of", "or", "the", "their", "to"]
		        }
		      }
		    }
		  },
		  "mappings": {
		    "properties": {
		      "consolidatedSampleKey": {
		        "type": "keyword"
		      },
		      "structureExact": {
		        "type": "text",
		        "analyzer": "lowercase_keyword"
		      },
		      "theilerStage": {
		        "type": "integer"
		      },
		      "strain": {
		        "type": "keyword"
		      },
		      "genotype": {
		        "type": "keyword"
		      },
		      "assayMgiid": {
		        "type": "keyword"
		      },
		      "age": {
		        "type": "keyword"
		      },
		      "printname": {
		        "type": "keyword"
		      },
		      "sex": {
		        "type": "keyword"
		      },
		      "bioreplicateCount": {
		        "type": "integer"
		      },
		      "_version_": {
		        "type": "long"
		      },
		      "ancestor_path": {
		        "type": "text",
		        "analyzer": "keyword",
		        "search_analyzer": "path_hierarchy"
		      },
		      "descendent_path": {
		        "type": "text",
		        "analyzer": "path_hierarchy",
		        "search_analyzer": "keyword"
		      },
		      "location": {
		        "type": "geo_point"
		      }
		    }
		  }
		}
		""";
		return mappingJson;
	}
}
