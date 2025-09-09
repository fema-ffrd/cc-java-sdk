package usace.cc.plugin.api;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import usace.cc.plugin.api.IOManager.InvalidDataStoreException;

public class PayloadTest {

    private PluginManager pm;
    private Payload payload;

    @BeforeEach
    void setUp() throws InvalidDataStoreException {
        pm = PluginManager.getInstance();
        payload = pm.getPayload();            
    }

    @Test 
    void getActions(){
        var actions = payload.getActions();
        int expectedNumberOfActions = 1;
        int numberOfActions = actions.length;
        if (numberOfActions!=expectedNumberOfActions){
            Assertions.fail(String.format("Expected %d actions, found %d",expectedNumberOfActions,numberOfActions));
        }
    }
}
