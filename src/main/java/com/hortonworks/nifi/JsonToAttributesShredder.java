package com.hortonworks.nifi;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.Map.Entry;

import org.apache.commons.io.IOUtils;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.provenance.ProvenanceReporter;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonParser;

@SideEffectFree
@Tags({"JSON", "Parse"})
@CapabilityDescription("Gather lineage information to register with Atlas")
public class JsonToAttributesShredder extends AbstractProcessor {
	//private List<PropertyDescriptor> properties;
	private Set<Relationship> relationships;
	//private ComponentLog logger = null;
	final Map<String,String> flattenedPaylod = new HashMap<String,String>();
	
	public static final String MATCH_ATTR = "match";

	/*public static final PropertyDescriptor JSON_PATH = new PropertyDescriptor.Builder()
	        .name("Json Path")
	        .required(true)
	        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
	        .build();
	*/
	public static final Relationship SUCCESS = new Relationship.Builder()
	        .name("SUCCESS")
	        .description("Succes relationship")
	        .build();
	
	public void init(final ProcessorInitializationContext context){
	    /*List<PropertyDescriptor> properties = new ArrayList<>();
	    properties.add(JSON_PATH);
	    this.properties = Collections.unmodifiableList(properties);
		*/
	    Set<Relationship> relationships = new HashSet<Relationship>();
	    relationships.add(SUCCESS);
	    this.relationships = Collections.unmodifiableSet(relationships);
	}

	@Override
	public Set<Relationship> getRelationships(){
	    return relationships;
	}
	
	@Override
	public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
		final AtomicReference<String> flowFileContents = new AtomicReference<>();
		//ProvenanceReporter provRep = session.getProvenanceReporter();
		getLogger();
		FlowFile flowFile = session.get();

	    session.read(flowFile, new InputStreamCallback() {
	        @Override
	        public void process(InputStream in) throws IOException {
	            try{
	                String json = IOUtils.toString(in);
	                flowFileContents.set(json); 
	            }catch(Exception ex){
	                ex.printStackTrace();
	                getLogger().error("Failed to read json string.");
	            }
	        }
	    });
		
		String json = flowFileContents.get();
		System.out.println(json);
				
		JsonFactory jsonFactory = new JsonFactory();
		try {
			JsonParser jp = jsonFactory.createJsonParser(json);
			//jp.setCodec(new ObjectMapper());
			JsonNode jsonNode = jp.readValueAsTree();
			List<String> fqnPath = new ArrayList<String>();
			Iterator<Entry<String, JsonNode>> jsonFieldsIterator = jsonNode.getFields();
			while(jsonFieldsIterator.hasNext()){
				Entry<String, JsonNode> currentNodeEntry = jsonFieldsIterator.next(); 
				if(currentNodeEntry.getValue().isArray()){
					getLogger().debug("Current Field: " + currentNodeEntry.getKey() + " | Data Type: Array");
					fqnPath.add(currentNodeEntry.getKey());
					handleArray(currentNodeEntry.getValue(), fqnPath);
				}else if(currentNodeEntry.getValue().isObject()){
					getLogger().debug("Current Field: " + currentNodeEntry.getKey() + " | Data Type: Object");
					fqnPath.add(currentNodeEntry.getKey());
					handleObject(currentNodeEntry.getValue(), fqnPath);
				}else{
					if(currentNodeEntry.getValue().isNumber()){
						getLogger().debug("Current Field: " + getFQN(fqnPath, currentNodeEntry.getKey()) + " | " + currentNodeEntry.getValue().getNumberValue() + " | Data Type: Numberic Primitive");
						flattenedPaylod.put(getFQN(fqnPath, currentNodeEntry.getKey()), String.valueOf(currentNodeEntry.getValue().getNumberValue()));
					}else if(currentNodeEntry.getValue().isBoolean()){
						getLogger().debug("Current Field: " + getFQN(fqnPath, currentNodeEntry.getKey()) + " | " + currentNodeEntry.getValue().getBooleanValue() + " | Data Type: Booleen Primitive");
						flattenedPaylod.put(getFQN(fqnPath, currentNodeEntry.getKey()), String.valueOf(currentNodeEntry.getValue().getBooleanValue()));
					}else{
						getLogger().debug("Current Field: " + getFQN(fqnPath, currentNodeEntry.getKey()) + " | " + currentNodeEntry.getValue().getTextValue() + " | Data Type: String Primitive");
						flattenedPaylod.put(getFQN(fqnPath, currentNodeEntry.getKey()), currentNodeEntry.getValue().getTextValue());
					}
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		flowFile = session.putAllAttributes(flowFile, flattenedPaylod);
		session.transfer(flowFile);
	}
			
	private ArrayList<Object> handleArray(JsonNode json, List<String> fqnPath){
		//System.out.println("Array Length: " + json.size());
		for(int i=0; i<json.size(); i++){
			fqnPath.add(String.valueOf(i));
			JsonNode currentElement = json.path(i); 
			Iterator<Entry<String, JsonNode>> jsonFieldsIterator = currentElement.getFields();
			while(jsonFieldsIterator.hasNext()){
				Entry<String, JsonNode> currentNodeEntry = jsonFieldsIterator.next(); 
				if(currentNodeEntry.getValue().isArray()){
					getLogger().debug("Current Field: " + getFQN(fqnPath, currentNodeEntry.getKey()) + " | Data Type: Array");
					fqnPath.add(currentNodeEntry.getKey());
					handleArray(currentNodeEntry.getValue(), fqnPath);
				}else if(currentNodeEntry.getValue().isObject()){
					getLogger().debug("Current Field: " + getFQN(fqnPath, currentNodeEntry.getKey()) + " | Data Type: Object");
					fqnPath.add(currentNodeEntry.getKey());
					handleObject(currentNodeEntry.getValue(), fqnPath);
				}else{
					if(currentNodeEntry.getValue().isNumber()){
						getLogger().debug("Current Field: " + getFQN(fqnPath, currentNodeEntry.getKey()) + " | " + currentNodeEntry.getValue().getNumberValue() + " | Data Type: Numberic Primitive");
						flattenedPaylod.put(getFQN(fqnPath, currentNodeEntry.getKey()), String.valueOf(currentNodeEntry.getValue().getNumberValue()));
					}else if(currentNodeEntry.getValue().isBoolean()){
						getLogger().debug("Current Field: " + getFQN(fqnPath, currentNodeEntry.getKey()) + " | " + currentNodeEntry.getValue().getBooleanValue() + " | Data Type: Booleen Primitive");
						flattenedPaylod.put(getFQN(fqnPath, currentNodeEntry.getKey()), String.valueOf(currentNodeEntry.getValue().getBooleanValue()));
					}else{
						getLogger().debug("Current Field: " + getFQN(fqnPath, currentNodeEntry.getKey()) + " | " + currentNodeEntry.getValue().getTextValue() + " | Data Type: String Primitive");
						flattenedPaylod.put(getFQN(fqnPath, currentNodeEntry.getKey()), currentNodeEntry.getValue().getTextValue());
					}
				}
			}
			fqnPath.remove(fqnPath.size()-1);
		}
		fqnPath.remove(fqnPath.size()-1);
		return null;
	}
			
	private Map<String, Object> handleObject(JsonNode json, List<String> fqnPath){
		//System.out.println("Array Length: " + json.size());
		Iterator<Entry<String, JsonNode>> jsonFieldsIterator = json.getFields();
		while(jsonFieldsIterator.hasNext()){
			Entry<String, JsonNode> currentNodeEntry = jsonFieldsIterator.next(); 
			if(currentNodeEntry.getValue().isArray()){
				getLogger().debug("Current Field: " + getFQN(fqnPath, currentNodeEntry.getKey()) + " | Data Type: Array");
				fqnPath.add(currentNodeEntry.getKey());
				handleArray(currentNodeEntry.getValue(), fqnPath);
			}else if(currentNodeEntry.getValue().isObject()){
				getLogger().debug("Current Field: " + getFQN(fqnPath, currentNodeEntry.getKey()) + " | Data Type: Object");
				fqnPath.add(currentNodeEntry.getKey());
				handleObject(currentNodeEntry.getValue(), fqnPath);
			}else{
				if(currentNodeEntry.getValue().isNumber()){
					getLogger().debug("Current Field: " + getFQN(fqnPath, currentNodeEntry.getKey()) + " | " + currentNodeEntry.getValue().getNumberValue() + " | Data Type: Numberic Primitive");
					flattenedPaylod.put(getFQN(fqnPath, currentNodeEntry.getKey()), String.valueOf(currentNodeEntry.getValue().getNumberValue()));
				}else if(currentNodeEntry.getValue().isBoolean()){
					getLogger().debug("Current Field: " + getFQN(fqnPath, currentNodeEntry.getKey()) + " | " + currentNodeEntry.getValue().getBooleanValue() + " | Data Type: Booleen Primitive");
					flattenedPaylod.put(getFQN(fqnPath, currentNodeEntry.getKey()), String.valueOf(currentNodeEntry.getValue().getBooleanValue()));
				}else{
					getLogger().debug("Current Field: " + getFQN(fqnPath, currentNodeEntry.getKey()) + " | " + currentNodeEntry.getValue().getTextValue() + " | Data Type: String Primitive");
					flattenedPaylod.put(getFQN(fqnPath, currentNodeEntry.getKey()), currentNodeEntry.getValue().getTextValue());
				}
			}					
		}
		fqnPath.remove(fqnPath.size()-1);
		return null;
	}
			
	private String getFQN(List<String> fqnPathList, String fieldName){
		String[] fqnPathArray = new String[fqnPathList.size()];
		fqnPathArray = fqnPathList.toArray(fqnPathArray);
		String fqnString = String.join(".", fqnPathArray) + "." + fieldName;
		return fqnString;
	}	   
}