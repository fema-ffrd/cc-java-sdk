package usace.cc.plugin.api.eventstore;

import usace.cc.plugin.api.eventstore.EventStore.CreateArrayInput;

/*
 * Constructor...
 * Create() throws Exception
 * Write(buffer) throws Exception
 * ArrayResult Read(recrange) throws Exception 
 */

public class Recordset {


    public Recordset(String arrayPath) throws EventStoreException{
        var input = buildCreateArrayInput(arrayPath);



        
        System.out.println(input);
    }

    private CreateArrayInput buildCreateArrayInput(String arrayPath) throws EventStoreException{
        var input = new CreateArrayInput();



        return input;
    }
    
}
