package usace.cc.plugin.api;

import org.junit.jupiter.api.Test;

import usace.cc.plugin.api.eventstore.Recordset;
import usace.cc.plugin.api.eventstore.Recordset.EventStoreAttr;

public class EventStoreTest {

        public static class DataTest{

        @EventStoreAttr("es-name1")
        private String name;

        @EventStoreAttr("es-name2")
        private int val1;
        
        @EventStoreAttr("station")
        private float val2;

        public DataTest(){}

        public DataTest(String name, int val1, float val2){
            this.name=name;
            this.val1=val1;
            this.val2=val2;
        }
    }

    @Test void eventStoreTest() throws Exception{

        PluginManager pm = PluginManager.getInstance();

        Payload payload = pm.getPayload();

        var dt = new DataTest[]{
            new DataTest("asdf",1,1f),
            new DataTest("randy1",2,2f),
            new DataTest("randy2",3,3f),
            new DataTest("randy3",4,4f),
            new DataTest("randy4",4,5f)
        };

        

        var storeOpt  = payload.getStore("EVENT_STORE");
        if(storeOpt.isPresent()){
            Recordset<DataTest> rs = new Recordset<>(storeOpt.get(), "/thepath");
            rs.create(dt);
            var out = rs.read(DataTest.class, 1l,4l);
            System.out.println(out);
        }
    }
    
}
