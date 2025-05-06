package usace.cc.plugin.api;

public interface ConnectionDataStore {

    public static class FailedToConnectError extends Exception{
        public FailedToConnectError(Exception ex){
            super(ex);
        }
    }

    public ConnectionDataStore connect(DataStore ds) throws FailedToConnectError;
    
    public Object rawSession(); //@TODO...can I switch Object to a generic type?
}
