package usace.cc.plugin.api;

import usace.cc.plugin.api.DataStore.DataStoreException;

/**
 * Represents a cloud compute internal store interface for storing and retrieving objects.
 * <p>
 * Implementations of this interface provide mechanisms for persisting data objects,
 * retrieving them, and managing associated metadata or payloads.
 * The store could be backed by cloud storage (e.g., S3), local filesystems, or other storage backends.
 */
//@TODO this should not be a part of the public API
public interface CcStore {

    public static final String PAYLOAD_FILE_NAME = "payload";
    
    
    void putObject(PutObjectInput input) throws DataStoreException;
    
    void pullObject(PullObjectInput input) throws DataStoreException;
    
    byte[] getObject(GetObjectInput input) throws Exception;

    Payload getPayload() throws Exception;

    String rootPath();
    
    boolean handlesDataStoreType(StoreType datastoretype);
}
