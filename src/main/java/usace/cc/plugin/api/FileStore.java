package usace.cc.plugin.api;

import java.io.InputStream;
import usace.cc.plugin.api.DataStore.DataStoreException;

public interface FileStore {
    public void copy(FileStore destStore, String srcPath, String destPath) throws DataStoreException;
    public GetObjectOutput get(String path) throws DataStoreException;
    public PutObjectOutput put(InputStream data, String path) throws DataStoreException;
    public void delete(String path) throws DataStoreException;
}