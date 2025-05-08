package usace.cc.plugin.api.eventstore;

import usace.cc.plugin.api.eventstore.EventStore.CreateArrayInput;
import usace.cc.plugin.api.eventstore.EventStore.GetArrayInput;
import usace.cc.plugin.api.eventstore.EventStore.PutArrayInput;

public interface MultiDimensionalArrayStore {
    void createArray(CreateArrayInput input) throws Exception;
    void putArray(PutArrayInput input) throws Exception;
    ArrayResult getArray(GetArrayInput input) throws Exception; 
}
