package org.jax.mgi.gxdindexer.indexer;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.solr.common.SolrInputDocument;
import org.codehaus.jackson.map.ObjectMapper;
import org.jax.mgi.shr.fe.IndexConstants;
import org.jax.mgi.shr.fe.indexconstants.GxdResultFields;
import org.jax.mgi.shr.fe.indexconstants.ImagePaneFields;
import org.jax.mgi.shr.fe.sort.SmartAlphaComparator;
import org.jax.mgi.shr.jsonmodel.GxdImageMeta;

/**
 * GxdImagePaneIndexer
 * @author kstone
 * This index is has the primary responsibility of populating the gxdImagePane solr index.
 * Each document in this index represents an image pane. This index can/will have fields to group by
 * assayKey and markerKey
 * 
 */

public class GxdImagePaneIndexer extends Indexer 
{   
	// class variables
	private final ObjectMapper objectMapper = new ObjectMapper();
	
	public Map<String,Integer> assayTypeSeqMap = new HashMap<String,Integer>();
	public SmartAlphaComparator sac = new SmartAlphaComparator();
	
    public GxdImagePaneIndexer () 
    { super("gxdImagePane"); }
    
    
    private void initAssayTypeSeqMap() throws SQLException {
    	
    	String assayTypeSeqSQL = "select distinct assay_type, assay_type_seq "
    			+ "from expression_assay ";
    	
    	ResultSet rs = ex.executeProto(assayTypeSeqSQL);
    	
    	while(rs.next()) {
    		this.assayTypeSeqMap.put(rs.getString("assay_type"), rs.getInt("assay_type_seq"));
    	}
    }
    
    public void index() throws Exception
    {    
    	
    		// get assay type sequences for image meta data sorting
    		initAssayTypeSeqMap();
    	
    	
    	
        	String imageQuery="select ei.result_key,ei.imagepane_key,ers.assay_id "+
        			"from expression_result_to_imagepane ei,expression_result_summary ers "+
        			"where ei.result_key=ers.result_key";
        	Map<Integer,List<Integer>> imagePaneResultMap = new HashMap<Integer,List<Integer>>();
        	Map<Integer,String> assayIDMap = new HashMap<Integer,String>();
        	logger.info("building map of image pane keys to result keys");
        	
            ResultSet rs = ex.executeProto(imageQuery);

	        while (rs.next())
	        {
	        	int ipKey = rs.getInt("imagepane_key");
	        	int resultKey = rs.getInt("result_key");
	        	if(!imagePaneResultMap.containsKey(ipKey))
	        	{
	        		imagePaneResultMap.put(ipKey, new ArrayList<Integer>());
	        	}
	        	imagePaneResultMap.get(ipKey).add(resultKey);
	        	
	        	assayIDMap.put(ipKey, rs.getString("assay_id"));
	        }
	        logger.info("done building map of image pane keys to result keys");
	        
        	ResultSet rs_tmp = ex.executeProto("select max(imagepane_key) as max_ip_key from expression_imagepane");
        	rs_tmp.next();
        	
        	Integer start = 0;
            Integer end = rs_tmp.getInt("max_ip_key");
        	int chunkSize = 15000;
            
            int modValue = end.intValue() / chunkSize;
            
            // Perform the chunking, this might become a configurable value later on

            logger.info("Getting all image panes");
            
            for (int i = 0; i <= modValue; i++) {
            
	            start = i * chunkSize;
	            end = start + chunkSize;
	            
	            String geneQuery="select eri.imagepane_key,ers.assay_type,ers.marker_symbol,ers.assay_id, "+
	            		"s.hybridization, s.specimen_label " +
		        		"from expression_result_to_imagepane eri,  " +
		        		"expression_result_summary ers LEFT OUTER JOIN " +
		        		"assay_specimen s ON ers.specimen_key=s.specimen_key "+
		        		"where eri.result_key=ers.result_key " +
		        		"and eri.imagepane_key > "+start+" and eri.imagepane_key <= "+end+" ";
	        	Map<Integer,Map<String,GxdImageMeta>> imagePaneMetaMap = new HashMap<Integer,Map<String,GxdImageMeta>>();
	        	logger.info("building map of image pane keys to meta data, ie. gene symbols + assay types + specimen labels");
	        	
	            rs = ex.executeProto(geneQuery);

		        while (rs.next())
		        {
		        	int ipKey = rs.getInt("imagepane_key");
		        	String assayType = rs.getString("assay_type");
		        	String markerSymbol = rs.getString("marker_symbol");
		        	String hybridization = rs.getString("hybridization");
		        	
		        	GxdImageMeta imageMeta = new GxdImageMeta();
		        	imageMeta.setMarkerSymbol(markerSymbol);
		        	imageMeta.setAssayType(assayType);
		        	imageMeta.setHybridization(hybridization);
		        	
		        	// init the meta data map for this pane
		        	if(!imagePaneMetaMap.containsKey(ipKey))
		        	{
		        		imagePaneMetaMap.put(ipKey, new HashMap<String,GxdImageMeta>());
		        	}
		        	Map<String,GxdImageMeta> metaMap = imagePaneMetaMap.get(ipKey);
		        	
		        	//init the meta data for this gene/assay type combo
		        	if(!metaMap.containsKey(imageMeta.toKey()))
		        	{
		        		metaMap.put(imageMeta.toKey(), imageMeta);
		        	}
		        	else {
		        		imageMeta = metaMap.get(imageMeta.toKey());
		        	}
		        	
		        	// append and specimen labels
		        	String specLabel = rs.getString("specimen_label");
		        	String assayID = rs.getString("assay_id");
		        	imageMeta.addSpecimenLabel(specLabel, assayID);
		        }
		        logger.info("done building map of image pane keys to meta data");
		        
		        logger.info("sorting map of image pane keys to meta data");
		        Map<Integer,List<GxdImageMeta>> imagePaneSortedMetaMap = new HashMap<Integer,List<GxdImageMeta>>();
		        for(Integer key : imagePaneMetaMap.keySet())
		        {
		        	List<GxdImageMeta> sortedMeta = new ArrayList<GxdImageMeta>(imagePaneMetaMap.get(key).values());
		        	// actually sort the meta
		        	Collections.sort(sortedMeta,new ImageMetaComparator<GxdImageMeta>());
		        	imagePaneSortedMetaMap.put(key,sortedMeta);
		        }
		        imagePaneMetaMap = null; // mark for garbage collection
		        logger.info("done sorting map of image pane keys to meta data");
		        
		        
	            logger.info ("Processing imagepane key > " + start + " and <= " + end);
	            String query = "select i.mgi_id,ip.imagepane_key, " +
	            		"i.figure_label, i.pixeldb_numeric_id, ip.pane_label, " +
	            		"ip.x,ip.y,ip.width,ip.height, " +
	            		"ip.by_assay_type, ip.by_marker, ip.by_hybridization_asc, " +
	            		"ip.by_hybridization_desc, " +
	            		"i.width image_width, i.height image_height " +
	            		" from image i,expression_imagepane ip where i.image_key=ip.image_key " +
	            		"and i.pixeldb_numeric_id is not null "+
	                    "and ip.imagepane_key > "+start+" and ip.imagepane_key <= "+end+" ";
	            rs = ex.executeProto(query);
	            
	            //Map<String,SolrInputDocument> docs = new HashMap<String,SolrInputDocument>();
	            Collection<SolrInputDocument> docs = new ArrayList<SolrInputDocument>();
	            
	            while (rs.next()) 
	            {           
	            	int imagepane_key = rs.getInt("imagepane_key");
	            	String imageID = rs.getString("mgi_id");
	            	String assayID = assayIDMap.containsKey(imagepane_key) ? assayIDMap.get(imagepane_key): "";

	            	SolrInputDocument doc = new SolrInputDocument();
	            	doc.addField(ImagePaneFields.IMAGE_PANE_KEY, imagepane_key);
	            	doc.addField(IndexConstants.UNIQUE_KEY, ""+imagepane_key);
	            	doc.addField(IndexConstants.IMAGE_ID, imageID);
	            	doc.addField(GxdResultFields.ASSAY_MGIID,assayID);
	            	
	            	doc.addField(ImagePaneFields.IMAGE_PIXELDBID, rs.getString("pixeldb_numeric_id"));
	            	String paneLabel = rs.getString("pane_label")!=null ? rs.getString("pane_label") : "";
	            	doc.addField(ImagePaneFields.IMAGE_LABEL, rs.getString("figure_label")+paneLabel);
	            	
	            	doc.addField(ImagePaneFields.IMAGE_WIDTH, rs.getInt("image_width"));
	            	doc.addField(ImagePaneFields.IMAGE_HEIGHT, rs.getInt("image_height"));
	            	doc.addField(ImagePaneFields.PANE_WIDTH, rs.getInt("width"));
	            	doc.addField(ImagePaneFields.PANE_HEIGHT, rs.getInt("height"));
	            	doc.addField(ImagePaneFields.PANE_X, rs.getInt("x"));
	            	doc.addField(ImagePaneFields.PANE_Y, rs.getInt("y"));
	            	
	            	// add the sort fields
	            	doc.addField(ImagePaneFields.BY_ASSAY_TYPE, rs.getInt("by_assay_type"));
	            	doc.addField(ImagePaneFields.BY_MARKER, rs.getInt("by_marker"));
	            	doc.addField(ImagePaneFields.BY_HYBRIDIZATION_ASC, rs.getInt("by_hybridization_asc"));
	            	doc.addField(ImagePaneFields.BY_HYBRIDIZATION_DESC, rs.getInt("by_hybridization_desc"));

	            	//get results
	            	// if this lookup fails, then there is probably a data inconsistency
	            	// we need all the expression_gatherers run at the same time to get the db keys in line
	            	List<Integer> expressionResultKeys = imagePaneResultMap.get(imagepane_key);
	            	if(expressionResultKeys == null)
	            	{
	            		// these keys are the whole point of this index. Without them, we can't join to it to get images.
	            		// This may happen in a case with inconsistent image data.
	            		continue;
	            	}
	            	
	            	for(Integer result_key : expressionResultKeys)
	            	{
	            		doc.addField(GxdResultFields.RESULT_KEY,result_key);
	            	}
	            	
	            	if(imagePaneSortedMetaMap.containsKey(imagepane_key))
	            	{
	            		for(GxdImageMeta imageMeta : imagePaneSortedMetaMap.get(imagepane_key))
	            		{
	            			// save image meta data as JSON
	            			doc.addField(ImagePaneFields.IMAGE_META, objectMapper.writeValueAsString(imageMeta));
	            		}
	            	
	            	}
	            	
		                
                    docs.add(doc);
	                if (docs.size() > 1000) {
	                    //logger.info("Adding a stack of the documents to Solr");
	                	startTime();
	                    writeDocs(docs);
	                    long endTime = stopTime();
	                    if(endTime > 500)
	                    {
	                    	logger.info("time to call writeDocs() "+stopTime());
	                    }
	                    docs = new ArrayList<SolrInputDocument>();

	                }
	            }
	            if (! docs.isEmpty()) {
	                writeDocs(docs);
	            }
	            
	            commit();
            }
            
    }
    /*
     * For debugging purposes only
     */
    private long startTime = 0;
    public void startTime()
    {
    	startTime = System.nanoTime();
    }
    public long stopTime()
    {
    	long endTime = System.nanoTime();
    	return (endTime - startTime)/1000000;
    	
    }
   
    
    
    /**
     * Sort all meta data by marker symbol, then by assay type
     * 
     * NOTE: This sorts the meta information inside each image pane row
     * 	It is unrelated to the column sorts: BY_ASSAY_TYPE, BY_MARKER, and BY_HYBRIDIZATION
     */
    private class ImageMetaComparator<T> implements Comparator<GxdImageMeta> {

		@Override
		public int compare(GxdImageMeta o1, GxdImageMeta o2) {
			
			// sort gene symbols first
			int symbolCompare = sac.compare(o1.getMarkerSymbol(), o2.getMarkerSymbol());
			if (symbolCompare != 0){
				return symbolCompare;
			}
			
			// sort assayType first
			int assayTypeSeq1 = assayTypeSeqMap.containsKey(o1.getAssayType()) ? assayTypeSeqMap.get(o1.getAssayType()): 99;
			int assayTypeSeq2 = assayTypeSeqMap.containsKey(o2.getAssayType()) ? assayTypeSeqMap.get(o2.getAssayType()): 99;
			if(assayTypeSeq1 > assayTypeSeq2) return 1;
			else if (assayTypeSeq1 < assayTypeSeq2) return -1;
			
			return 0;
		}
    	
    }
    
}
