package org.infinispan.tutorial.simple.remote.query;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.Search;
import org.infinispan.client.hotrod.configuration.ClientIntelligence;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.hotrod.impl.ConfigurationProperties;
import org.infinispan.protostream.GeneratedSchema;
import org.infinispan.query.dsl.Query;
import org.infinispan.query.dsl.QueryFactory;
import org.infinispan.tutorial.simple.connect.Infinispan;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.infinispan.query.remote.client.ProtobufMetadataManagerConstants.PROTOBUF_METADATA_CACHE_NAME;

/**
 * The Remote Query simple tutorial.
 *
 * Infinispan Server includes a default property realm that requires
 * authentication. Create some credentials before you run this tutorial.
 *
 * @author Katia Aresti, karesti@redhat.com
 */
public class InfinispanRemoteQuery {

	public static final String KEY_CACHE_SUFFIX = "_key";

	public static void main(String[] args) throws Exception {
		ConfigurationBuilder builder = Infinispan.connectionConfig();

		// Add the Protobuf serialization context in the client
		builder.addContextInitializer(new QuerySchemaBuilderImpl());

		// Connect to the server
		RemoteCacheManager client = new RemoteCacheManager(builder.build());

		String keyCacheName = Infinispan.TUTORIAL_CACHE_NAME + KEY_CACHE_SUFFIX;
		client.administration().getOrCreateCache(keyCacheName, (String) null);

		// Create and add the Protobuf schema in the server
		addPersonSchema(client);

		// Get the people cache, create it if needed with the default configuration
		RemoteCache<Person, String> peopleCache = client.getCache(Infinispan.TUTORIAL_CACHE_NAME);
		peopleCache.addClientListener(new KeyCacheClientListener<Person>(client, keyCacheName));

		// Create the persons dataset to be stored in the cache
		Map<Person, String> people = new HashMap<>();
		people.put(new Person("Oihana", "Rossignol", 2016, "Paris"), "1");
		people.put(new Person("Elaia", "Rossignol", 2018, "Paris"), "2");
		people.put(new Person("Yago", "Steiner", 2013, "Saint-Mandé"), "3");
		people.put(new Person("Alberto", "Steiner", 2016, "Paris"), "4");

		// Put all the values in the cache
		peopleCache.putAll(people);
		
		//Listener is async, so wait to complete
		Thread.sleep(1000);

		// Get a query factory from the cache
		QueryFactory queryFactory = Search.getQueryFactory(client.getCache(keyCacheName));

		// Create a query with lastName parameter
		Query<Person> query = queryFactory.create("FROM tutorial.Person p where p.lastName = :lastName");

		// Set the parameter value
		query.setParameter("lastName", "Rossignol");

		// Execute the query
		List<Person> rossignols = query.execute().list();

		// Print the results
		System.out.printf("Matching query results: %s%n", rossignols);
		System.out.printf("People cache size: %d%n", peopleCache.size());

		System.out.println("Removing queried entries");
		rossignols.stream().forEach(key -> peopleCache.remove(key));
		/*
		 * peopleCache.removeAll(rossignols); //"removeAll" operation doesnt exist
		 */
		/*
		 * //following logic doesn't work, i.e. "null" values aren't considered removals
		 * Map<Person, String> keysToRemove = new HashMap<>();
		 * rossignols.stream().forEach(key -> keysToRemove.put(key, null));
		 * peopleCache.putAll(keysToRemove);
		 */

		//Listener is async, so wait to complete
		Thread.sleep(1000);

		rossignols = query.execute().list();
		System.out.printf("Matching query results: %s%n", rossignols);
		System.out.printf("People cache size: %d%n", peopleCache.size());

		// Stop the client and release all resources
		client.stop();
	}

	private static void addPersonSchema(RemoteCacheManager cacheManager) {
		// Retrieve metadata cache
		RemoteCache<String, String> metadataCache = cacheManager.getCache(PROTOBUF_METADATA_CACHE_NAME);

		// Define the new schema on the server too
		GeneratedSchema schema = new QuerySchemaBuilderImpl();
		metadataCache.put(schema.getProtoFileName(), schema.getProtoFile());
	}
}
