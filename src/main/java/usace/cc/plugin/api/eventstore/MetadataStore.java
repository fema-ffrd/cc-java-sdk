package usace.cc.plugin.api.eventstore;

import usace.cc.plugin.api.eventstore.EventStoreException;

public interface MetadataStore {
    public <T> T getMetadata(String key, Class<?> clazz) throws EventStoreException;
    public void putMetadata(String key, Object val) throws EventStoreException;
    //public void deleteMetadata(String key) throws EventStoreException;
}
