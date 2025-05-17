package usace.cc.plugin.api;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.Optional;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.amazonaws.services.s3.AmazonS3;

import usace.cc.plugin.api.IOManager.InvalidDataStoreException;

public class IOManagerTest {

    private PluginManager pm;
    private Payload payload;

    @BeforeEach
    void setUp() throws InvalidDataStoreException {
        pm = PluginManager.getInstance();
        payload = pm.getPayload();            
    }

    @Test
    public void testGetStore() {
        Optional<DataStore> storeOpt = payload.getStore("S3STORE");
        if (storeOpt.isPresent()){
            var store = storeOpt.get();
            if (!(store.getRawSession() instanceof AmazonS3)){
                Assertions.fail("Session is not an S3 instance");    
            }
        } else {
            Assertions.fail("Unable to load S3STORE");
        }
    }
    
}
