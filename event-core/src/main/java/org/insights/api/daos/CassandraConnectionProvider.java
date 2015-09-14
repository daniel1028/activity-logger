package org.insights.api.daos;

import java.io.IOException;
import java.util.Properties;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;

import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.connectionpool.NodeDiscoveryType;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolType;
import com.netflix.astyanax.connectionpool.impl.CountingConnectionPoolMonitor;
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl;
import com.netflix.astyanax.thrift.ThriftFamilyFactory;

@Component
public class CassandraConnectionProvider {

	private static Keyspace logKeyspace;
	private static String hosts;
	private static String clusterName;
	private static String logKeyspaceName;
	private static String logDataCentre;
	private static String elsHost;
	private static Client client;

	private static final Logger logger = LoggerFactory
			.getLogger(CassandraConnectionProvider.class);

	@Resource(name = "cassandra")
	private Properties cassandra;

	@PostConstruct
	private void initConnection() {
		if (logKeyspace == null) {
			logger.info("Loading cassandra properties");
			hosts = this.getCassandraConstant().getProperty("cluster.hosts");
			elsHost = this.getCassandraConstant().getProperty("elasticsearch.hosts");
			clusterName = this.getCassandraConstant().getProperty(
					"cluster.name");
			logKeyspaceName = this.getCassandraConstant().getProperty(
					"log.keyspace");
			logDataCentre = this.getCassandraConstant().getProperty(
					"log.datacentre");
			System.out.print("hosts : " + elsHost);
			initInsights();
		}
	}

	private static void initInsights() {
		try {
			logger.info("Loading cassandra connection properties");
			ConnectionPoolConfigurationImpl poolConfig = new ConnectionPoolConfigurationImpl(
					"MyConnectionPool").setPort(9160).setMaxConnsPerHost(30)
					.setSeeds(hosts);
			if (!hosts.startsWith("127.0")) {
				poolConfig.setLocalDatacenter(logDataCentre);
			}

			AstyanaxContext<Keyspace> logContext = new AstyanaxContext.Builder()
					.forCluster(clusterName)
					.forKeyspace(logKeyspaceName)
					.withAstyanaxConfiguration(
							new AstyanaxConfigurationImpl().setDiscoveryType(
									NodeDiscoveryType.NONE)
									.setConnectionPoolType(
											ConnectionPoolType.TOKEN_AWARE))
					.withConnectionPoolConfiguration(poolConfig)
					.withConnectionPoolMonitor(
							new CountingConnectionPoolMonitor())
					.buildKeyspace(ThriftFamilyFactory.getInstance());
			logContext.start();
			logKeyspace = (Keyspace) logContext.getClient();
			logger.info("Initialized connection to " + logKeyspaceName
					+ " keyspace");
		} catch (Exception e) {
			logger.error("Error while initializing cassandra : {}", e);
		}

		if (client == null) {
			// Elastic search connection provider
			Settings settings = ImmutableSettings.settingsBuilder()
					.put("cluster.name", "")
					.put("client.transport.sniff", true).build();
			TransportClient transportClient = new TransportClient(settings);
			transportClient.addTransportAddress(new InetSocketTransportAddress(
					elsHost, 9300));
			client = transportClient;			
			registerIndices();
		}
	}

	  public final static void registerIndices() {
			for (ESIndices esIndex : ESIndices.values()) {
				String indexName = esIndex.getIndex();
					String mapping = EsMappingUtil.getMappingConfig(esIndex.getType());
					try {
						CreateIndexRequestBuilder prepareCreate = getESClient().admin().indices().prepareCreate(indexName);
						prepareCreate.addMapping(esIndex.getType(), mapping);
						prepareCreate.execute().actionGet();
						logger.info("Index created : " + indexName + "\n");
					} catch (Exception exception) {
						logger.info("Already Index availble : " + indexName + "\n");
					}
			}
		}
	public static Keyspace getKeyspace() {

		if (logKeyspace == null) {
			initInsights();
		}
		return logKeyspace;
	}

	private Properties getCassandraConstant() {
		return cassandra;
	}

	public static Client getESClient() throws IOException {
		if (client == null) {
			throw new IOException("Elastic Search is not initialized.");
		}
		return client;
	}

	
}
