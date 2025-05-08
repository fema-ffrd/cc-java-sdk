package usace.cc.plugin.api.eventstore;

public class EventStoreException extends RuntimeException{
    public EventStoreException(Exception ex){
        super(ex);
    }

    public EventStoreException(String msg) {
        super(msg);
    }
}
