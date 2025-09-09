package usace.cc.plugin.api;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import usace.cc.plugin.api.IOManager.InvalidDataStoreException;

public class PayloadAttributeTest {

    private PluginManager pm;
    private Payload payload;

    @BeforeEach
    void setUp() throws InvalidDataStoreException {
        pm = PluginManager.getInstance();
        payload = pm.getPayload();            
    }

    @Test 
    void getAttributesTest(){
        var payloadAttributes = payload.getAttributes();
        int expectedNumberOfAttributes = 13;
        int numberOfAttributes = payloadAttributes.entrySet().size();
        if (numberOfAttributes!=expectedNumberOfAttributes){
            Assertions.fail(String.format("Expected %d attributes, found %d",expectedNumberOfAttributes,numberOfAttributes));
        }
    }

    @Test
    void getStringAttributeTest(){
        var payloadAttributes = payload.getAttributes();
        String expectedResult = "simulations";
        Optional<String> resultOpt = payloadAttributes.get("outputroot");
        if(resultOpt.isPresent()){
            String result = resultOpt.get();
            if (!expectedResult.equals(result)){
                Assertions.fail(String.format("Expected %s, found %s",expectedResult,result));
            }
        }
    }

    @Test
    void getTestArrayAttributeTest(){
        var payloadAttributes = payload.getAttributes();
        List<Integer> expectedResult = List.of(1, 2, 3, 4);
        Optional<List<?>> resultOpt = payloadAttributes.get("testarray");
        if(resultOpt.isPresent()){
            List<?> result = resultOpt.get();
            if (!result.equals(expectedResult)){
                Assertions.fail(String.format("Expected %s, found %s", expectedResult, result));
            }
        } else {
            Assertions.fail("Attribute 'testarray' not found");
        }
    }

    @Test
    void getTestStringArrayAttributeTest(){
        var payloadAttributes = payload.getAttributes();
        List<String> expectedResult = List.of("9", "8", "7", "6");
        Optional<List<?>> resultOpt = payloadAttributes.get("teststringarray");
        if(resultOpt.isPresent()){
            List<?> result = resultOpt.get();
            if (!result.equals(expectedResult)){
                Assertions.fail(String.format("Expected %s, found %s", expectedResult, result));
            }
        } else {
            Assertions.fail("Attribute 'teststringarray' not found");
        }
    }

    @Test
    void getTestNumberArrayAttributeTest(){
        var payloadAttributes = payload.getAttributes();
        List<Double> expectedResult = List.of(9.9, 8.8, 7.7, 6.6);
        Optional<List<?>> resultOpt = payloadAttributes.get("testnumberarray");
        if(resultOpt.isPresent()){
            List<?> result = resultOpt.get();
            if (!result.equals(expectedResult)){
                Assertions.fail(String.format("Expected %s, found %s", expectedResult, result));
            }
        } else {
            Assertions.fail("Attribute 'testnumberarray' not found");
        }
    }

    @Test
    void getTestBoolArrayAttributeTest(){
        var payloadAttributes = payload.getAttributes();
        List<Boolean> expectedResult = List.of(true, false, true, false);
        Optional<List<?>> resultOpt = payloadAttributes.get("testboolarray");
        if(resultOpt.isPresent()){
            List<?> result = resultOpt.get();
            if (!result.equals(expectedResult)){
                Assertions.fail(String.format("Expected %s, found %s", expectedResult, result));
            }
        } else {
            Assertions.fail("Attribute 'testboolarray' not found");
        }
    }

    @Test
    void getTestNumberAttributeTest(){
        var payloadAttributes = payload.getAttributes();
        Double expectedResult = 999.8888;
        Optional<Double> resultOpt = payloadAttributes.get("testnumber");
        if(resultOpt.isPresent()){
            Double result = resultOpt.get();
            if (!expectedResult.equals(result)){
                Assertions.fail(String.format("Expected %s, found %s", expectedResult, result));
            }
        } else {
            Assertions.fail("Attribute 'testnumber' not found");
        }
    }

    @Test
    void getTestIntAttributeTest(){
        var payloadAttributes = payload.getAttributes();
        Integer expectedResult = 99;
        Optional<Integer> resultOpt = payloadAttributes.get("testint");
        if(resultOpt.isPresent()){
            Integer result = resultOpt.get();
            if (!expectedResult.equals(result)){
                Assertions.fail(String.format("Expected %s, found %s", expectedResult, result));
            }
        } else {
            Assertions.fail("Attribute 'testint' not found");
        }
    }

    @Test
    void getTestBoolAttributeTest(){
        var payloadAttributes = payload.getAttributes();
        Boolean expectedResult = true;
        Optional<Boolean> resultOpt = payloadAttributes.get("testbool");
        if(resultOpt.isPresent()){
            Boolean result = resultOpt.get();
            if (!expectedResult.equals(result)){
                Assertions.fail(String.format("Expected %s, found %s", expectedResult, result));
            }
        } else {
            Assertions.fail("Attribute 'testbool' not found");
        }
    }

    @Test
    void getTestObjectAttributeTest(){
        var payloadAttributes = payload.getAttributes();
        Map<String, Object> expectedResult = Map.of("key1", "val1", "key2", 999.99);
        Optional<Map<?, ?>> resultOpt = payloadAttributes.get("testobject");
        if(resultOpt.isPresent()){
            Map<?, ?> result = resultOpt.get();
            if (!result.equals(expectedResult)){
                Assertions.fail(String.format("Expected %s, found %s", expectedResult, result));
            }
        } else {
            Assertions.fail("Attribute 'testobject' not found");
        }
    }

    //Test payload substitution for payload input
    @Test
    void payloadSubstitutionTest(){
        var pathkey = "default";
        var dsOpt = payload.getInputDataSource("test3");
        var expectedResult = "cc_store/test/987654/hwout.txt";
        if(dsOpt.isPresent()){
            var ds = dsOpt.get();
            var result = ds.getPath(pathkey);
            if (!result.equals(expectedResult)){
                Assertions.fail(String.format("Expected %s, found %s", expectedResult, result));
            }
        } else {
            Assertions.fail("DataSource 'test3' not found");
        }
    }

    //Test payload substitution for action output
    @Test
    void actionPayloadSubstitutionTest(){
        var action = payload.getActions()[0];
        var pathkey = "default";
        var dsOpt = action.getOutputDataSource("seeds");
        var expectedResult = "cc_store/test/987654/hwout.txt";
        if(dsOpt.isPresent()){
            var ds = dsOpt.get();
            var result = ds.getPath(pathkey);
            if (!result.equals(expectedResult)){
                Assertions.fail(String.format("Expected %s, found %s", expectedResult, result));
            }
        } else {
            Assertions.fail("DataSource 'test3' not found");
        }
    }

    //Test action attribute substitution for action input
    @Test
    void actionAttrPayloadSubstitutionTest(){
        var action = payload.getActions()[0];
        var pathkey = "default";
        var dsOpt = action.getInputDataSource("inputseeds");
        var expectedResult = "cc_store/test/conformance/hwout.txt";
        if(dsOpt.isPresent()){
            var ds = dsOpt.get();
            var result = ds.getPath(pathkey);
            if (!result.equals(expectedResult)){
                Assertions.fail(String.format("Expected %s, found %s", expectedResult, result));
            }
        } else {
            Assertions.fail("DataSource 'test3' not found");
        }
    }
    
    //Test that action attrs are not substituted
    @Test
    void actionAttrNoSubstitutionTest(){
        var action = payload.getActions()[0];
        var resultOpt = action.getAttributes().get("myccroot");
        var expectedResult = "{ENV::CC_ROOT}";
        if (resultOpt.isPresent()){
            var result = resultOpt.get();
            if (!result.equals(expectedResult)){
                Assertions.fail(String.format("Expected %s, found %s", expectedResult, result));
            }
        } else {
            Assertions.fail("Action attribute 'myccroot' not found");
        }
    } 
}
