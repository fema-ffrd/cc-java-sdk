package usace.cc.plugin.api;

public interface CcStore {
    public boolean putObject(PutObjectInput input);
    public boolean pullObject(PullObjectInput input);
    public byte[] getObject(GetObjectInput input) throws Exception;
    public Payload getPayload() throws Exception;
    //public void SetPayload(Payload payload); only used in the go sdk to support cloudcompute which is written in go.
    public String rootPath();
    public boolean handlesDataStoreType(StoreType datastoretype);
}
