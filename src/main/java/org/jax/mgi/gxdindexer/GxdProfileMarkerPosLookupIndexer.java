package org.jax.mgi.gxdindexer;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * GxdProfileMarkerPosLookupIndexer
 * 
 * @author joel richardson This index is used for performing GXD profile
 *         searches. Each document represents a marker with a marker key, which
 *         will then be used to filter results from the gxdResult index in a two
 *         step process.
 * 
 */

public class GxdProfileMarkerPosLookupIndexer extends Indexer {
	int batchSize = 100;	
	
	public GxdProfileMarkerPosLookupIndexer() {
		super("gxd_profile_marker_pos_lookup");
	}
	public void index() throws Exception {
		
		GxdProfileMarkerIndexer gxdProfileMarkerIndexer = new GxdProfileMarkerIndexer();
		gxdProfileMarkerIndexer.setDoNotWriteDocToES(true);
		gxdProfileMarkerIndexer.setCollectPosMap(true);
		gxdProfileMarkerIndexer.index();
		Map<String, List<Map<String, Object>>> fieldPosMgiidMap = gxdProfileMarkerIndexer.getFieldPosMgiidMap();
		gxdProfileMarkerIndexer = null;	
		
		for (String field: GxdProfileMarkerIndexer.FIELD_NAMES) {
			int count = 0;
			int total = 0;
			List<Map<String, Object>> docs = new ArrayList<Map<String, Object>>();
			for ( Map<String, Object> doc: fieldPosMgiidMap.get(field) ) {				
				doc.put("field", field);
				docs.add(doc);
				count++;
				total++;
				if (count % batchSize == 0) {
					writeDocs(docs);
					commit();
					docs.clear();
					logger.info("write " + field + " " + total);
				}
				
			}
			writeDocs(docs);
	    	commit();
		}		
		logger.info("load completed");
	}

	@Override
	protected String getIndexMappingJson() {
		String mappingJson = """
		{
		  "settings": {  
		    "number_of_shards": 4,
		    "number_of_replicas": 0,
		    "refresh_interval": "10s",
		    "max_result_window": 1000000
		  },
		  "mappings": {
			"properties": {
		      "pos_id": {
		        "type": "integer"
		      },
		      "markerMgiid": {
		        "type": "keyword"
		      },
		      "field": {
		        "type": "keyword"
		      }
		    }
		  }
		}
		""";
		//"_source": { "enabled": false },
		return mappingJson;
	}
}
