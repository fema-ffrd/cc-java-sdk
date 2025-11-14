package usace.cc.plugin.api;

import org.junit.jupiter.api.Test;
import usace.cc.plugin.api.Substituter.EmbeddedVar;
import static org.junit.jupiter.api.Assertions.assertEquals;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;

import static org.assertj.core.api.Assertions.*;


public class SubstituterTest {

    @Test
    public void testMatcher(){
        //var substituter = new Substituter();
        var embeddedVars = Substituter.match("test me {ATTR::scenario[]}");
        assertEquals(embeddedVars.size(), 1);
        var ev = embeddedVars.get(0);
        assertEquals(ev.type, "ATTR");
        assertEquals(ev.varname, "scenario");
        assertEquals(ev.isArrayOrMap, true);
        assertEquals(ev.arrayIndex, -1);
        assertEquals(ev.mapIndex, null);
    }

     @Test
    public void testMatcherMutlipleVals(){

        var expectedVals = new ArrayList<EmbeddedVar>();

        var ev1 = new EmbeddedVar();
        ev1.type="ATTR";
        ev1.varname="scenario";
        ev1.isArrayOrMap=false;
        ev1.arrayIndex=-1;
        ev1.mapIndex=null;
        expectedVals.add(ev1);

        var ev2 = new EmbeddedVar();
        ev2.type="ENV";
        ev2.varname="CC_EVENT_IDENTIFIER";
        ev2.isArrayOrMap=false;
        ev2.arrayIndex=-1;
        ev2.mapIndex=null;
        expectedVals.add(ev2);

        var ev3 = new EmbeddedVar();
        ev3.type="ATTR";
        ev3.varname="testnumberarray";
        ev3.isArrayOrMap=true;
        ev3.arrayIndex=-1;
        ev3.mapIndex=null;
        expectedVals.add(ev3);

        var ev4 = new EmbeddedVar();
        ev4.type="ATTR";
        ev4.varname="testnumberarray";
        ev4.isArrayOrMap=true;
        ev4.arrayIndex=2;
        ev4.mapIndex=null;
        expectedVals.add(ev4);

        var ev5 = new EmbeddedVar();
        ev5.type="ATTR";
        ev5.varname="testobject";
        ev5.isArrayOrMap=true;
        ev5.arrayIndex=-1;
        ev5.mapIndex="key1";
        expectedVals.add(ev5);

        var ev6 = new EmbeddedVar();
        ev6.type="ATTR";
        ev6.varname="testobject";
        ev6.isArrayOrMap=true;
        ev6.arrayIndex=-1;
        ev6.mapIndex="key1";
        expectedVals.add(ev6);

        var substituter = new Substituter();
        var embeddedVars = substituter.match("test me {ATTR::scenario}, {ENV::CC_EVENT_IDENTIFIER}, {ATTR::testnumberarray[]}, {ATTR::testnumberarray[2]}, {ATTR::testobject['key1']}, {ATTR::testobject[\"key1\"]}");
        assertEquals(embeddedVars.size(), 6);
        assertThat(embeddedVars)
          .usingRecursiveComparison()
          .isEqualTo(expectedVals);
    }

    @Test
    public void testParameterSubstution1(){

        Map<String,String> expectedVals = Map.of(
            "default-0-model1","test me 1, SUBNAME!!!! for basinA with bright red and a number of 2.2",
            "default-1-model1","test me 2, SUBNAME!!!! for basinA with bright red and a number of 2.2",
            "default-2-model1","test me 3, SUBNAME!!!! for basinA with bright red and a number of 2.2",
            "default-3-model1","test me 4, SUBNAME!!!! for basinA with bright red and a number of 2.2",
            "default-4-model1","test me 5, SUBNAME!!!! for basinA with bright red and a number of 2.2",
            "default-0-model2","test me 1, SUBNAME!!!! for basinB with bright red and a number of 2.2",
            "default-1-model2","test me 2, SUBNAME!!!! for basinB with bright red and a number of 2.2",
            "default-2-model2","test me 3, SUBNAME!!!! for basinB with bright red and a number of 2.2",
            "default-3-model2","test me 4, SUBNAME!!!! for basinB with bright red and a number of 2.2",
            "default-4-model2","test me 5, SUBNAME!!!! for basinB with bright red and a number of 2.2"
        );

        var scenarios = Arrays.asList(1, 2, 3, 4, 5);
        String subname = "SUBNAME!!!!";
        Map<String,String> models = Map.of(
            "model1","basinA",
            "model2", "basinB"
        );

        Map<String,String> colors = Map.of(
            "bright","red",
            "dull","grey"
        );
        var numbers = Arrays.asList(1.1,2.2);

        Map<String,Object> attrs = Map.of(
            "scenarios",scenarios,
            "subname", subname,
            "models", models,
            "colors",colors,
            "numbers",numbers
        );

        var tmpl ="test me {ATTR::scenarios[]}, {ATTR::subname} for {ATTR::models[]} with bright {ATTR::colors['bright']} and a number of {ATTR::numbers[1]}";
        var tmplkey = "default";

        //var substituter = new Substituter();
        var out = Substituter.parameterSubstitution(tmplkey, tmpl, attrs,true);

         assertThat(out)
          .usingRecursiveComparison()
          .isEqualTo(expectedVals);
    }

     @Test
    public void testParameterSubstution2(){
         Map<String,String> expectedVals = Map.of(
            "default","test me 1,2,3,4,5"
         );

        var scenarios = Arrays.asList(1, 2, 3, 4, 5);

        Map<String,Object> attrs = Map.of(
            "scenarios", scenarios
        );

        var tmpl ="test me {ATTR::scenarios}";
        var tmplkey = "default";

        //var substituter = new Substituter();
        var out = Substituter.parameterSubstitution(tmplkey, tmpl, attrs, true);

        assertThat(out)
          .usingRecursiveComparison()
          .isEqualTo(expectedVals); 
    }
    
}
