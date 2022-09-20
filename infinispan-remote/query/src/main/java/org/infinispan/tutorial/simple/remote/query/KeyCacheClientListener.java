package org.infinispan.tutorial.simple.remote.query;

import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.annotation.ClientCacheEntryCreated;
import org.infinispan.client.hotrod.annotation.ClientCacheEntryRemoved;
import org.infinispan.client.hotrod.annotation.ClientListener;
import org.infinispan.client.hotrod.event.ClientCacheEntryCreatedEvent;
import org.infinispan.client.hotrod.event.ClientCacheEntryRemovedEvent;

@ClientListener
public class KeyCacheClientListener<K> {

	private RemoteCacheManager rcm;
	private final String keyCacheName;

	public KeyCacheClientListener(RemoteCacheManager rcm, String keyCacheName) {
		this.rcm = rcm;
		this.keyCacheName = keyCacheName;
	}

	@ClientCacheEntryCreated
	public void handleCreated(ClientCacheEntryCreatedEvent<K> e) {
		System.out.println("firing handleCreated");
		rcm.getCache(this.keyCacheName).putAsync(e.getKey(), e.getKey());
	}

	@ClientCacheEntryRemoved
	public void handleRemoved(ClientCacheEntryRemovedEvent<K> e) {
		System.out.println("firing handleRemoved");
		rcm.getCache(this.keyCacheName).removeAsync(e.getKey());
	}

}
