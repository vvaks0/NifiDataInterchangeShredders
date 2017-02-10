package com.hortonworks.nifi;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.ProtocolException;
import java.net.URL;
import java.net.UnknownHostException;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;
import org.apache.atlas.AtlasClient;
import org.apache.atlas.AtlasServiceException;
import org.apache.atlas.typesystem.Referenceable;
import org.apache.atlas.typesystem.json.InstanceSerialization;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.requests.MetadataResponse.PartitionMetadata;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import kafka.admin.AdminUtils;
import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;

@SideEffectFree
@Tags({"JSON", "XML", "Parse"})
@CapabilityDescription("Shred deeply nested JSON payload into flattened attributes")
public class StreamingSelfService extends AbstractProcessor {
	private List<PropertyDescriptor> properties;
	private Set<Relationship> relationships;
	final Map<String,String> flattenedPaylod = new HashMap<String,String>();
	public static final String DEFAULT_ATLAS_REST_ADDRESS = "http://sandbox.hortonworks.com";
	public static final String DEFAULT_ADMIN_USER = "admin";
	public static final String DEFAULT_ADMIN_PASS = "admin";
	private static AtlasClient atlasClient;
	private String[] basicAuth = {DEFAULT_ADMIN_USER, DEFAULT_ADMIN_PASS};
	private Properties props = System.getProperties();
	private static java.sql.Connection phoenixConnection;
	private static String shredderUrl = "http://loanmaker01-238-3-2.field.hortonworks.com:8095";
	private static String nifiUrl = "http://hdf01.field.hortonworks.com:9090";
	private static String registryUrl = "http://hdf01.field.hortonworks.com:9095";
	private static String atlasUrl = "http://loanmaker01-238-3-2.field.hortonworks.com:21000";
	private static String zkKafkaUri = "hdf01.field.hortonworks.com:2181";
	private static String zkHbaseUri = "hdf01.field.hortonworks.com:2182:/hbase";
	private String hostName;
	
	public static final String MATCH_ATTR = "match";

	static final PropertyDescriptor SHREDDER_URL = new PropertyDescriptor.Builder()
            .name("Shredder Url")
            .description("Url where the Avro Schema Shredder is running")
            .required(true)
            //.allowableValues("json", "xml")
            .defaultValue("http://loanmaker01-238-3-2.field.hortonworks.com:8095")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
	
	static final PropertyDescriptor NIFI_URL = new PropertyDescriptor.Builder()
            .name("NIFI Url")
            .description("Url where Nifi is running")
            .required(true)
            .defaultValue("http://hdf01.field.hortonworks.com:9090")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
	
	static final PropertyDescriptor REGISTRY_URL = new PropertyDescriptor.Builder()
            .name("Registry Url")
            .description("Url where the Avro Schema Registry is running")
            .required(true)
            .defaultValue("http://hdf01.field.hortonworks.com:9095")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
	
	static final PropertyDescriptor ATLAS_URL = new PropertyDescriptor.Builder()
            .name("Atlas Url")
            .description("Url where Atlas is running")
            .required(true)
            .defaultValue("http://loanmaker01-238-3-2.field.hortonworks.com:21000")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
	
	static final PropertyDescriptor ZK_KAFKA_URL = new PropertyDescriptor.Builder()
            .name("Kafka Zookeeper Uri")
            .description("Url where Kafka is running")
            .required(true)
            .defaultValue("hdf01.field.hortonworks.com:2181")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
	
	static final PropertyDescriptor ZK_PHOENIX_URL = new PropertyDescriptor.Builder()
            .name(" Phoenix Zookeeper Uri")
            .description("Url where Phoenix is running")
            .required(true)
            .defaultValue("hdf01.field.hortonworks.com:2182:/hbase")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
	
	public static final Relationship REL_SUCCESS = new Relationship.Builder()
	        .name("SUCCESS")
	        .description("Succes relationship")
	        .build();
	
    public static final Relationship REL_FAIL = new Relationship.Builder()
            .name("FAIL")
            .description("FlowFiles are routed to this relationship when JSON cannot be parsed")
            .build();
	
	public void init(final ProcessorInitializationContext context){
	    List<PropertyDescriptor> properties = new ArrayList<>();
	    properties.add(SHREDDER_URL);
	    properties.add(NIFI_URL);
	    properties.add(REGISTRY_URL);
	    properties.add(ATLAS_URL);
	    properties.add(ZK_KAFKA_URL);
	    properties.add(ZK_PHOENIX_URL);
	    this.properties = Collections.unmodifiableList(properties);
		
	    Set<Relationship> relationships = new HashSet<Relationship>();
	    relationships.add(REL_SUCCESS);
	    relationships.add(REL_FAIL);
	    this.relationships = Collections.unmodifiableSet(relationships);
	}

	@Override
	public Set<Relationship> getRelationships(){
	    return relationships;
	}
	
	@Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }
	
	@Override
	public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {	
		shredderUrl = context.getProperty(SHREDDER_URL).getValue();
		nifiUrl = context.getProperty(NIFI_URL).getValue();
		registryUrl = context.getProperty(REGISTRY_URL).getValue();
		atlasUrl = context.getProperty(ATLAS_URL).getValue();
		zkKafkaUri = context.getProperty(ZK_KAFKA_URL).getValue();
		zkHbaseUri = context.getProperty(ZK_PHOENIX_URL).getValue();
		
		FlowFile flowFile = session.get();
		if ( flowFile != null ) {
        	//flowFile = session.create();
		
		props.setProperty("atlas.conf", "/root");
		try {
			hostName = InetAddress.getLocalHost().getHostName();
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
		String[] atlasURL = {atlasUrl};
		atlasClient = new AtlasClient(atlasURL, basicAuth);
		System.out.println("***************** atlas.conf has been set to: " + props.getProperty("atlas.conf"));
		
		String tableName = flowFile.getAttribute("tableName"); 
		String topicName = flowFile.getAttribute("topicName");
		String schemaGroup = flowFile.getAttribute("schemaGroup");
		String schemaName = topicName + ":v";
		String schemaText = flowFile.getAttribute("payload");
		
		System.out.println("Table Name: " + tableName);
		System.out.println("Topic Name: " + topicName);
		System.out.println("Schema Group: " + schemaGroup);
		System.out.println("Schema Text: " + schemaText);
		
		/*
		final ObjectMapper mapper = new ObjectMapper();
		final AtomicReference<JsonNode> data = new AtomicReference<>(null);
		try {
			session.read(flowFile, new InputStreamCallback() {
				@Override
				public void process(final InputStream in) throws IOException {
					try (final InputStream bufferedIn = new BufferedInputStream(in)) {
						data.set(mapper.readTree(bufferedIn));
					}
				}
			});
		} catch (final ProcessException pe) {
			getLogger().error("Failed to parse {} due to {}; routing to failure", new Object[] {flowFile, pe.toString()}, pe);
			session.transfer(flowFile, REL_FAIL);
			return;
		}*/
		//String jsonData = data.get().toString();
		try {
			Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
			phoenixConnection = DriverManager.getConnection("jdbc:phoenix:"+ zkHbaseUri);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		}
			
		String schemaDescription = null;
		String topicUri = null;
			
		createPhoenixTable(tableName);
		    
		String schemaGroupId = registerNewSchemaGroup(schemaName, schemaGroup);
		String schemaVersionId = registerNewSchemaVersion(schemaName, schemaText, schemaDescription);
		System.out.println("Schema Group Id: " + schemaGroupId + " Schema Version Id:" + schemaVersionId);
		Referenceable schemaReferenceable = storeSchemaInAtlas(schemaText);
		Referenceable kafkaTopicReferenceable = createKafkaTopic(topicName, schemaReferenceable);
		topicUri = kafkaTopicReferenceable.get("uri").toString();
		System.out.println("Topic Uri: " + topicUri);
		createFlow(tableName, topicUri, topicName, schemaGroup, schemaName);
		session.transfer(flowFile, REL_SUCCESS);
		}
	}
		
		private Referenceable createKafkaTopic(String topicName, Referenceable schemaReferenceable){
		    int sessionTimeoutMs = 10 * 1000;
		    int connectionTimeoutMs = 8 * 1000;
		    ZkClient zkClient = new ZkClient(zkKafkaUri,sessionTimeoutMs,connectionTimeoutMs, ZKStringSerializer$.MODULE$);

		    boolean isSecureKafkaCluster = false;
		    ZkUtils zkUtils = new ZkUtils(zkClient, new ZkConnection(zkKafkaUri), isSecureKafkaCluster);

		    int partitions = 1;
		    int replication = 1;
		    Properties topicConfig = new Properties();
		    Referenceable kafkaTopic = new Referenceable("kafka_topic");
		    
		    try{
		    	//System.out.println("Checking if Kafka Topic exists: " +AdminUtils.topicExists(zkUtils, topicName));
		    	AdminUtils.createTopic(zkUtils, topicName, partitions, replication, topicConfig, null);
		    	topicName = AdminUtils.fetchTopicMetadataFromZk(topicName, zkUtils).topic();
		    }catch(TopicExistsException e){
		    	System.out.println("Checking if Kafka Topic exists: true");
			}
		    List<Referenceable> schemas = new ArrayList<Referenceable>();
		    schemas.add(schemaReferenceable);
		    List<PartitionMetadata> topicParitionData = AdminUtils.fetchTopicMetadataFromZk(topicName, zkUtils).partitionMetadata();
		    
		    String host = topicParitionData.get(0).leader().host();
		    String port = String.valueOf(topicParitionData.get(0).leader().port());
		    zkClient.close();
		    
		    System.out.println("Kafka Topic Name: " + topicName);
	    	System.out.println("Kafka Topic Host: " + host);
	    	System.out.println("Kafka Topic Port: " + port);
	    
	    	kafkaTopic.set("name", topicName);
	    	kafkaTopic.set("description", topicName);
	    	kafkaTopic.set("uri", host + ":" + port);
	    	kafkaTopic.set("qualifiedName", topicName + "@" + hostName);
	    	kafkaTopic.set("avro_schema", schemas);
	    	kafkaTopic.set("topic", topicName);
	    	kafkaTopic.set("owner", "HDF");
	    
	    	try {
				atlasClient.createEntity(InstanceSerialization.toJson(kafkaTopic, true));
			} catch (AtlasServiceException e) {
				e.printStackTrace();
			}
		    
			return kafkaTopic;
		}
		
		private String registerNewSchemaGroup(String schemaName, String schemaGroup){
			String output = null;
	    	String string = null;
			String payload = "{" +
								"\"type\": \"avro\", " +
								"\"schemaGroup\": \"" + schemaGroup + "\"," +
								"\"name\": \"" + schemaName +"\"," + 
								"\"description\": null," +
								"\"compatibility\": \"BACKWARD\" " +
							 "}";
			String schemaEntityId = null;
			try{
		    	URL url = new URL(registryUrl + "/api/v1/schemaregistry/schemas");
		    	HttpURLConnection conn = (HttpURLConnection) url.openConnection();
		    	conn.setDoOutput(true);
		    	conn.setRequestMethod("POST");
		    	conn.setRequestProperty("Content-Type", "application/json");
		        System.out.println("Schema Group Payload: " + payload);    
		    	OutputStream os = conn.getOutputStream();
		    	os.write(payload.getBytes());
		    	os.flush();
		    	BufferedReader br = null;    
		    	if (conn.getResponseCode() >= 200 && conn.getResponseCode() < 205){
		    		br = new BufferedReader(new InputStreamReader((conn.getInputStream())));
		    		while ((output = br.readLine()) != null) {
		            	string = output;
		            }
		            System.out.println(string);
		    	}else{
		    		br = new BufferedReader(new InputStreamReader((conn.getErrorStream())));
		            while ((output = br.readLine()) != null) {
		            	string = output;
		            }
		            System.out.println(string);
		            throw new RuntimeException("Failed : HTTP error code : " + conn.getResponseCode());
		    	}
		    	schemaEntityId = new JSONObject(string).get("entity").toString();
		    } catch (Exception e) {
		            e.printStackTrace();
		    }
			System.out.println("Response: " + string);
			return schemaEntityId;
		}
		
		private String registerNewSchemaVersion(String schemaName, String schemaText, String schemaDescription){
			String output = null;
	    	String string = null;
	    	String schemaVersionEntityId = null;
	    	System.out.println(schemaText.toString());
			
		    String payload = "{\"description\": \"" + schemaDescription + "\"," +
		    				  "\"schemaText\": " + JSONObject.quote(schemaText) + "}";
		    System.out.println(payload.toString());
			try{
		    	URL url = new URL(registryUrl + "/api/v1/schemaregistry/schemas/" + schemaName + "/versions");
		    	System.out.println("Url: " + url);
		    	HttpURLConnection conn = (HttpURLConnection) url.openConnection();
		    	conn.setDoOutput(true);
		    	conn.setRequestMethod("POST");
		    	conn.setRequestProperty("Content-Type", "application/json");
		            
		    	OutputStream os = conn.getOutputStream();
		    	os.write(payload.getBytes());
		    	os.flush();
		            
		    	BufferedReader br = null;    
		    	if (conn.getResponseCode() >= 200 && conn.getResponseCode() < 205){
		    		br = new BufferedReader(new InputStreamReader((conn.getInputStream())));
		    		while ((output = br.readLine()) != null) {
		            	string = output;
		            }
		            System.out.println(string);
		    	}else{
		    		br = new BufferedReader(new InputStreamReader((conn.getErrorStream())));
		            while ((output = br.readLine()) != null) {
		            	string = output;
		            }
		            System.out.println(string);
		            throw new RuntimeException("Failed : HTTP error code : " + conn.getResponseCode());
		    	}	
		    	
		    	schemaVersionEntityId = new JSONObject(string).get("entity").toString();
		    } catch (Exception e) {
		            e.printStackTrace();
		    }
			System.out.println("Response: " + string);
			return schemaVersionEntityId;
		}
		
		private void createPhoenixTable(String tableName) {
			String sql = " CREATE TABLE IF NOT EXISTS \"" + tableName +"\" ("
												+ "\"firstname\" VARCHAR NOT NULL, "
												+ "\"lastname\" VARCHAR NOT NULL, "
												+ "CONSTRAINT \"pk\" PRIMARY KEY (\"firstname\",\"lastname\"))";
			System.out.println("Phoenix Table DDL: " + sql);
			try {
				Statement create = phoenixConnection.createStatement();
				int createRS = create.executeUpdate(sql);
				create.close();			
				if (createRS > 0) {
					System.out.println("Successfully created table");
				}
				phoenixConnection.commit();
				phoenixConnection.close();
			} catch (SQLException e) {
				try {
					phoenixConnection.rollback();
				} catch (SQLException e1) {
					e1.printStackTrace();
				}
				e.printStackTrace();
			}
		}
		
		private void createFlow(String tableName, String kafkaUri, String topicName, String schemaGroup, String schemaName){
			List<String> processorIds = new ArrayList<String>();
			String payload = "{\"revision\":" +
					"{\"clientId\":\"x\"," +
					 "\"version\":0" +
					 "}," +
					 "\"component\":" +
					 	"{\"position\":{\"x\":0.0,\"y\":600.0}," +
					 	 "\"config\":" +
							"{\"properties\":" +
								"{\"Base Path\":\"" + topicName + "\"," +
									"\"Listening Port\": \"8083\"" +
								"}," +
								"\"autoTerminatedRelationships\": [\"failure\"]" +
							"}," +
						"\"name\": \"Listen on HTTP\"," +
						"\"type\": \"org.apache.nifi.processors.standard.ListenHTTP\"," +
						"\"state\":\"STOPPED\"" +
					 "}" +
				 "}";
			System.out.println("Creating ListenHTTP Processor: " + payload.toString());
			String listenerId = null;
			try {
				listenerId = new JSONObject(postRequestNifi(payload, "processors")).get("id").toString();
				processorIds.add(listenerId);
			} catch (JSONException e) {
				e.printStackTrace();
			}
			
			payload = "{\"revision\":" +
					"{\"clientId\":\"x\"," +
					 "\"version\":0" +
					 "}," +
					 "\"component\":" +
					 	"{\"position\":{\"x\":600.0,\"y\":600.0}," +
					 	 "\"config\":" +
							"{\"properties\":" +
								"{\"schema-registry-service\":\"71287625-0159-1000-ffff-ffff99d8795c\"," +
									"\"schema-name\": \"" + schemaName + "\"," +
									"\"schema-version\": null," +
									"\"schema-type\":\"avro\"," +
									"\"schema-group\":\"NIFI\"" +
								"}," +
								"\"autoTerminatedRelationships\": [\"failure\"]" +	
							"}," +		
						"\"name\": \"Serializer\"," +
						"\"type\": \"com.hortonworks.nifi.registry.TransformJsonToAvroViaSchemaRegistry\"," +
						"\"state\":\"STOPPED\"" +
					 "}" +
				 "}";
			System.out.println("Creating Serializer Processor: " + payload.toString());
			String serializerId = null;
			try {
				serializerId = new JSONObject(postRequestNifi(payload, "processors")).get("id").toString();
				processorIds.add(serializerId);
			} catch (JSONException e) {
				e.printStackTrace();
			}
			
			payload = "{\"revision\":" +
					"{\"clientId\":\"x\"," +
					 "\"version\":0" +
					 "}," +
					 "\"component\":" +
					 	"{\"source\":" +
							"{\"id\": \"" + listenerId + "\"," +
								"\"type\": \"PROCESSOR\"" +
							"}," +
						"\"destination\":" +
							"{\"id\": \"" + serializerId + "\"," +
								"\"type\": \"PROCESSOR\"" +
							"}," +
						"\"selectedRelationships\": [\"success\"]," +
						"\"availableRelationships\": [\"success\"] " +
						"}" +
				 "}";
			System.out.println("Connecting Deserializer and KafkaProducer Processors: " + payload.toString());
			postRequestNifi(payload, "connections");
			
			payload = "{\"revision\":" +
					"{\"clientId\":\"x\"," +
					 "\"version\":0" +
					 "}," +
					 "\"component\":" +
					 	"{\"position\":{\"x\":1200.0,\"y\":600.0}," +
					 	"\"config\":" +
							"{\"properties\":" +
								"{\"bootstrap.servers\":\"" + kafkaUri + "\"," +
									"\"topic\": \"" + topicName + "\"" +
								"}," +
								"\"autoTerminatedRelationships\": [\"failure\",\"success\"]" +
							"}," +
							"\"name\": \"Publish To Custom Topic\"," +
							"\"type\": \"org.apache.nifi.processors.kafka.pubsub.PublishKafka_0_10\"," +
							"\"state\":\"STOPPED\"" +
						"}" +
				 "}";
			System.out.println("Creating Kafka Producer Processor: " + payload.toString());
			String kafkaProducerId = null;
			try {
				kafkaProducerId = new JSONObject(postRequestNifi(payload, "processors")).get("id").toString();
				processorIds.add(kafkaProducerId);
			} catch (JSONException e) {
				e.printStackTrace();
			}
			
			payload = "{\"revision\":" +
					"{\"clientId\":\"x\"," +
					 "\"version\":0" +
					 "}," +
					 "\"component\":" +
					 	"{\"source\":" +
							"{\"id\": \"" + serializerId + "\"," +
								"\"type\": \"PROCESSOR\"" +
							"}," +
						"\"destination\":" +
							"{\"id\": \"" + kafkaProducerId + "\"," +
								"\"type\": \"PROCESSOR\"" +
							"}," +
						"\"selectedRelationships\": [\"success\"]," +
						"\"availableRelationships\": [\"success\"] " +
						"}" +
				 "}";
			System.out.println("Connecting Deserializer and KafkaProducer Processors: " + payload.toString());
			postRequestNifi(payload, "connections");
			
			payload = "{\"revision\":" +
					"{\"clientId\":\"x\"," +
					 "\"version\":0" +
					 "}," +
					 "\"component\":" +
					 	"{\"position\":{\"x\":0.0,\"y\":300.0}," +
					 	"\"config\":" +
							"{\"properties\":" +
								"{\"bootstrap.servers\":\"" + kafkaUri + "\"," +
									"\"topic\": \"" + topicName + "\"," +
									"\"group.id\":\"NIFI\"" +
								"}" +
							"}," +
							"\"name\": \"Consume Custom Topic\"," +
							"\"type\": \"org.apache.nifi.processors.kafka.pubsub.ConsumeKafka_0_10\"," +
							"\"state\":\"STOPPED\"" +
						"}" +
				 "}";
			System.out.println("Creating Kafka Consumer Processor: " + payload.toString());
			String kafkaConsumerId = null;
			try {
				kafkaConsumerId = new JSONObject(postRequestNifi(payload, "processors")).get("id").toString();
				processorIds.add(kafkaConsumerId);
			} catch (JSONException e) {
				e.printStackTrace();
			}
			
			payload = "{\"revision\":" +
					"{\"clientId\":\"x\"," +
					 "\"version\":0" +
					 "}," +
					 "\"component\":" +
					 	"{\"position\":{\"x\":600.0,\"y\":300.0}," +
					 	 "\"config\":" +
							"{\"properties\":" +
								"{\"schema-registry-service\":\"71287625-0159-1000-ffff-ffff99d8795c\"," +
									"\"schema-name\": \"" + schemaName + "\"," +
									"\"schema-version\": null," +
									"\"schema-type\":\"avro\"," +
									"\"schema-group\":\"NIFI\"" +
								"}," +
								"\"autoTerminatedRelationships\": [\"failure\"]" +
							"}," +	
						"\"name\": \"Deserializer\"," +
						"\"type\": \"com.hortonworks.nifi.registry.TransformAvroToJsonViaSchemaRegistry\"," +
						"\"state\":\"STOPPED\"" +
					 "}" +
				 "}";
			System.out.println("Creating Deserializer Processor: " + payload.toString());
			String deserializerId = null;
			try {
				deserializerId = new JSONObject(postRequestNifi(payload, "processors")).get("id").toString();
				processorIds.add(deserializerId);
			} catch (JSONException e) {
				e.printStackTrace();
			}
			
			payload = "{\"revision\":" +
					"{\"clientId\":\"x\"," +
					 "\"version\":0" +
					 "}," +
					 "\"component\":" +
					 	"{\"source\":" +
							"{\"id\": \"" + kafkaConsumerId + "\"," +
								"\"type\": \"PROCESSOR\"" +
							"}," +
						"\"destination\":" +
							"{\"id\": \"" + deserializerId + "\"," +
								"\"type\": \"PROCESSOR\"" +
							"}," +
						"\"selectedRelationships\": [\"success\"]," +
						"\"availableRelationships\": [\"success\"] " +
						"}" +
				 "}";
			System.out.println("Connecting KafkaConsumer and Deserializer Processors: " + payload.toString());
			postRequestNifi(payload, "connections");
			
			payload = "{\"revision\":" +
					"{\"clientId\":\"x\"," +
					 "\"version\":0" +
					 "}," +
					 "\"component\":" +
					 	"{\"position\":{\"x\":1200.0,\"y\":300.0}," +
					 		"\"config\":" +
							"{\"properties\":" +
								"{\"JDBC Connection Pool\":\"cc925e5a-0159-1000-ffff-ffffa600695a\"," +
									"\"Statement Type\": \"INSERT\"," +
									"\"Table Name\": \""+ tableName +"\"," +
									"\"jts-quoted-identifiers\": true" +
								"}," +
								"\"autoTerminatedRelationships\": [\"failure\",\"original\"]" +
							"}," +	
							"\"name\": \"Create Insert Statement\"," +
							"\"type\": \"org.apache.nifi.processors.standard.ConvertJSONToSQL\"," +
							"\"state\":\"STOPPED\"" +
						"}" +
				 "}";
			System.out.println("Creating JsonToSQL Processor: " + payload.toString());
			String jsonToSQLId = null;
			try {
				jsonToSQLId = new JSONObject(postRequestNifi(payload, "processors")).get("id").toString();
				processorIds.add(jsonToSQLId);
			} catch (JSONException e) {
				e.printStackTrace();
			}
			
			payload = "{\"revision\":" +
					"{\"clientId\":\"x\"," +
					 "\"version\":0" +
					 "}," +
					 "\"component\":" +
					 	"{\"source\":" +
							"{\"id\": \"" + deserializerId + "\"," +
								"\"type\": \"PROCESSOR\"" +
							"}," +
						"\"destination\":" +
							"{\"id\": \"" + jsonToSQLId + "\"," +
								"\"type\": \"PROCESSOR\"" +
							"}," +
						"\"selectedRelationships\": [\"success\"]," +
						"\"availableRelationships\": [\"success\"] " +
						"}" +
				 "}";
			System.out.println("Connecting Deserializer and JsonToSQL Processors: " + payload.toString());
			postRequestNifi(payload, "connections");
			
			payload = "{\"revision\":" +
					"{\"clientId\":\"x\"," +
					 "\"version\":0" +
					 "}," +
					 "\"component\":" +
					 	"{\"position\":{\"x\":1800.0,\"y\":300.0}," +
					 		"\"config\":" +
							"{\"properties\":" +
								"{\"Regular Expression\":\"INSERT INTO " + tableName +"\"," +
								 "\"Replacement Value\": \"UPSERT INTO \\\"" + tableName + "\\\"\"" +
								"}," +
								"\"autoTerminatedRelationships\": [\"failure\",\"original\"]" +
							"}," +	
							"\"name\": \"Modify Insert Syntax\"," +
							"\"type\": \"org.apache.nifi.processors.standard.ReplaceText\"," +
							"\"state\":\"STOPPED\"" +
						"}" +
				 "}";
			System.out.println("Creating ReplaceText Processor: " + payload.toString());
			String replaceTextId = null;
			try {
				replaceTextId = new JSONObject(postRequestNifi(payload, "processors")).get("id").toString();
				processorIds.add(replaceTextId);
			} catch (JSONException e) {
				e.printStackTrace();
			}
			
			payload = "{\"revision\":" +
					"{\"clientId\":\"x\"," +
					 "\"version\":0" +
					 "}," +
					 "\"component\":" +
					 	"{\"source\":" +
							"{\"id\": \"" + jsonToSQLId + "\"," +
								"\"type\": \"PROCESSOR\"" +
							"}," +
						"\"destination\":" +
							"{\"id\": \"" + replaceTextId + "\"," +
								"\"type\": \"PROCESSOR\"" +
							"}," +
						"\"selectedRelationships\": [\"sql\"]," +
						"\"availableRelationships\": [\"sql\"] " +
						"}" +
				 "}";
			System.out.println("Connecting JsonToSQL to ReplaceText Processors: " + payload.toString());
			postRequestNifi(payload, "connections");
			
			payload = "{\"revision\":" +
					"{\"clientId\":\"x\"," +
					 "\"version\":0" +
					 "}," +
					 "\"component\":" +
					 	"{\"position\":{\"x\":2400.0,\"y\":300.0}," +
					 		"\"config\":" +
							"{\"properties\":" +
								"{\"JDBC Connection Pool\":\"cc925e5a-0159-1000-ffff-ffffa600695a\"" +
								"}," +
								"\"autoTerminatedRelationships\": [\"failure\",\"retry\",\"success\"]" +
							"}," +	
							"\"name\": \"Persist to Phoenix\"," +
							"\"type\": \"org.apache.nifi.processors.standard.PutSQL\"," +
							"\"state\":\"STOPPED\"" +
						"}" +
				 "}";
			System.out.println("Creating PutToSQL Processor: " + payload.toString());
			String putSQLId = null;
			try {
				putSQLId = new JSONObject(postRequestNifi(payload, "processors")).get("id").toString();
				processorIds.add(putSQLId);
			} catch (JSONException e) {
				e.printStackTrace();
			}
			
			payload = "{\"revision\":" +
					"{\"clientId\":\"x\"," +
					 "\"version\":0" +
					 "}," +
					 "\"component\":" +
					 	"{\"source\":" +
							"{\"id\": \"" + replaceTextId + "\"," +
								"\"type\": \"PROCESSOR\"" +
							"}," +
						"\"destination\":" +
							"{\"id\": \"" + putSQLId + "\"," +
								"\"type\": \"PROCESSOR\"" +
							"}," +
						"\"selectedRelationships\": [\"success\"]," +
						"\"availableRelationships\": [\"success\"] " +
						"}" +
				 "}";
			System.out.println("Connecting JsonToSQL to PutSQL Processors: " + payload.toString());
			postRequestNifi(payload, "connections");
			
			Iterator<String> idIterator = processorIds.iterator();
			while(idIterator.hasNext()){
				String currentProcessorId = idIterator.next();
				payload = "{\"revision\":" +
							"{" +
								"\"clientId\":\"x\"," +
								"\"version\":1" +
							"}," +
							"\"id\": \""+ currentProcessorId +"\"," +
							"\"component\":" +
						 	"{" +
						 		"\"id\": \""+ currentProcessorId +"\"," +
								"\"state\":\"RUNNING\"" +
							"}" +
						"}";
				putRequestNifi(currentProcessorId, payload, "processors");
			}
		}
		
		private String postRequestNifi(String payload, String requestType){
			String output = null;
			String string = null;
			try {
				URL url = new URL(nifiUrl + "/nifi-api/process-groups/root/" + requestType);
				HttpURLConnection conn = (HttpURLConnection) url.openConnection();
				System.out.println("Url: " + url);
		    	conn.setDoOutput(true);
		    	conn.setRequestMethod("POST");
		    	conn.setRequestProperty("Content-Type", "application/json");
		            
		    	OutputStream os = conn.getOutputStream();
		    	os.write(payload.getBytes());
		    	os.flush();
		            
		    	BufferedReader br = null;    
		    	if (conn.getResponseCode() >= 200 && conn.getResponseCode() < 205){
		    		br = new BufferedReader(new InputStreamReader((conn.getInputStream())));
		    		while ((output = br.readLine()) != null) {
		            	string = output;
		            }
		            System.out.println(string);
		    	}else{
		    		br = new BufferedReader(new InputStreamReader((conn.getErrorStream())));
		            while ((output = br.readLine()) != null) {
		            	string = output;
		            }
		            System.out.println(string);
		            throw new RuntimeException("Failed : HTTP error code : " + conn.getResponseCode());
		    	}	
		    } catch (Exception e) {
		            e.printStackTrace();
		    }
			System.out.println("Response: " + string);
			
			return string;
		}
		
		private String putRequestNifi(String id, String payload, String requestType){
			String output = null;
			String string = null;
			try {
				URL url = new URL(nifiUrl + "/nifi-api/" + requestType + "/" + id);
				HttpURLConnection conn = (HttpURLConnection) url.openConnection();
				System.out.println("Url: " + url);
		    	conn.setDoOutput(true);
		    	conn.setRequestMethod("PUT");
		    	conn.setRequestProperty("Content-Type", "application/json");
		            
		    	OutputStream os = conn.getOutputStream();
		    	os.write(payload.getBytes());
		    	os.flush();
		            
		    	BufferedReader br = null;    
		    	if (conn.getResponseCode() >= 200 && conn.getResponseCode() < 205){
		    		br = new BufferedReader(new InputStreamReader((conn.getInputStream())));
		    		while ((output = br.readLine()) != null) {
		            	string = output;
		            }
		            System.out.println(string);
		    	}else{
		    		br = new BufferedReader(new InputStreamReader((conn.getErrorStream())));
		            while ((output = br.readLine()) != null) {
		            	string = output;
		            }
		            System.out.println(string);
		            throw new RuntimeException("Failed : HTTP error code : " + conn.getResponseCode());
		    	}	
		    } catch (Exception e) {
		            e.printStackTrace();
		    }
			System.out.println("Response: " + string);
			
			return string;
		}
		
		private JSONArray getAllAvroSchemasFromRegistry(){
			URL url;
			JSONArray schemas = null;
			try {
				url = new URL(registryUrl + "/api/v1/schemaregistry/schemas");
				HttpURLConnection conn = (HttpURLConnection) url.openConnection();
				conn.setDoOutput(true);
				conn.setRequestMethod("GET");
				conn.setRequestProperty("Accept", "application/json");
	    
	    		if (conn.getResponseCode() != 200) {
					throw new RuntimeException("Failed : HTTP error code : " + conn.getResponseCode());
				}	

	    		BufferedReader br = new BufferedReader(new InputStreamReader((conn.getInputStream())));

				String output = null;
				String string = null;
				System.out.println("Output from Server .... \n");
				while ((output = br.readLine()) != null) {
					string = output;
				}
				JSONObject schemasResponse = new JSONObject(string);
				schemas = schemasResponse.getJSONArray("entities");
				System.out.println(schemas);
			} catch (MalformedURLException e) {
				e.printStackTrace();
			} catch (ProtocolException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			} catch (JSONException e) {
				e.printStackTrace();
			}
			
			return schemas;
		}
		
		private String getAvroSchemaFromRegistry(String schemaName){
			String schemaText = null;
			URL url;
			try {
				url = new URL(registryUrl + "/api/v1/schemaregistry/schemas/"+schemaName+"/versions/latest");
				HttpURLConnection conn = (HttpURLConnection) url.openConnection();
	    		conn.setDoOutput(true);
		    	conn.setRequestMethod("GET");
		    	conn.setRequestProperty("Accept", "application/json");
		            
		        if (conn.getResponseCode() != 200) {
		        	throw new RuntimeException("Failed : HTTP error code : " + conn.getResponseCode());
		    	}

		        BufferedReader br = new BufferedReader(new InputStreamReader((conn.getInputStream())));
		        String output = null;
		        String string = null;
		        System.out.println("Output from Server .... \n");
		        while ((output = br.readLine()) != null) {
		        	string = output;
		        }
		        //System.out.println(string);
		        JSONObject schemasResponse = new JSONObject(string);
			    JSONObject schema = schemasResponse.getJSONObject("entity");
			    schemaText = schema.getString("schemaText").replaceAll("\\n", "").replaceAll("\\s+","");
			    System.out.println(schemaText);
			} catch (MalformedURLException e) {
					e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			} catch (JSONException e) {
				e.printStackTrace();
			}     
		        	
		    return schemaText;    	
		}
		
		private Referenceable storeSchemaInAtlas(String payload){
	        System.out.println("Storing Schema in Atlas...");
	        System.out.println(payload.toString());
	        String output = null;
	    	String string = null;
	    	Referenceable schemaRefernceable = null;
	        try{
	        	URL url = new URL(shredderUrl + "/schemaShredder/storeSchema");
	    		HttpURLConnection conn = (HttpURLConnection) url.openConnection();
	    		conn.setDoOutput(true);
		    	conn.setRequestMethod("POST");
		    	conn.setRequestProperty("Content-Type", "application/json");
		            
		    	OutputStream os = conn.getOutputStream();
		    	os.write(payload.getBytes());
		    	os.flush();
		            
		    	BufferedReader br = null;    
		    	if (conn.getResponseCode() >= 200 && conn.getResponseCode() < 205){
		    		br = new BufferedReader(new InputStreamReader((conn.getInputStream())));
		    		while ((output = br.readLine()) != null) {
		            	string = output;
		            }
		            System.out.println("Code: " + conn.getResponseCode() + " : " + string);
		    	}else{
		    		br = new BufferedReader(new InputStreamReader((conn.getErrorStream())));
		            while ((output = br.readLine()) != null) {
		            	string = output;
		            }
		            System.out.println("Code: " + conn.getResponseCode() + " : " + string);
		            throw new RuntimeException("Failed : HTTP error code : " + conn.getResponseCode());
		    	}	
		    	
		    } catch (Exception e) {
		            e.printStackTrace();
		    }
	        
	        try {
	        	System.out.println(atlasClient.getAdminStatus());
	        	schemaRefernceable = atlasClient.getEntity(string);
			} catch (AtlasServiceException e1) {
				e1.printStackTrace();
			} catch(Exception e){
				e.printStackTrace();
			}
	        
			System.out.println("Response: " + string);
	        
	        return schemaRefernceable; 
	    }
}
