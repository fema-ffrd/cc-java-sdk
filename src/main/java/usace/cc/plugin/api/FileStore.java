package usace.cc.plugin.api;

import java.io.InputStream;

public interface FileStore {
    public Boolean copy(FileStore destStore, String srcPath, String destPath);
    public InputStream get(String path);
    public Boolean put(InputStream data, String path);
    public Boolean delete(String path);
}