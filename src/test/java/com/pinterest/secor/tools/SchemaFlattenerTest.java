package com.pinterest.secor.tools;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

public class SchemaFlattenerTest {


    @Test
    public void columnGenerateNameTest() {
        Assert.assertEquals(new SchemaFlattener.Column(Arrays.asList("I", "Am", "Hungry"), null, null, null).generateName(), "I_Am_Hungry");
        Assert.assertEquals(new SchemaFlattener.Column(Arrays.asList("bop"), null, null, null).generateName(), "bop");
    }

    @Test
    public void extractValueTest() {

        GenericRecord wurstRecord = new GenericData.Record(new Schema.Parser().parse("" +
                "{\n" +
                "  \"type\" : \"record\",\n" +
                "  \"name\" : \"wurst\",\n" +
                "  \"fields\" : [ {\n" +
                "        \"name\" : \"taste\",\n" +
                "        \"type\" : \"string\"\n" +
                "     } ]\n" +
                "    }\n"));
        wurstRecord.put("taste", "good");

        GenericRecord curryWurstRecord = new GenericData.Record(new Schema.Parser().parse("{\n" +
                "    \"name\": \"curry\",\n" +
                "    \"type\": \"record\",\n" +
                "    \"fields\": [\n" +
                "        {\n" +
                "            \"name\": \"wurst\",\n" +
                "            \"type\": {\n" +
                "                        \"type\" : \"record\",\n" +
                "                        \"name\" : \"something_else\",\n" +
                "                        \"fields\" : [\n" +
                "                            {\"name\": \"taste\", \"type\": \"string\"}\n" +
                "                        ]\n" +
                "                    }\n" +
                "        }\n" +
                "    ]\n" +
                "}"));
        curryWurstRecord.put("wurst", wurstRecord);

        Assert.assertEquals("good", SchemaFlattener.extractValue(new SchemaFlattener.Column( Arrays.asList("wurst", "taste"),null, null, null), curryWurstRecord));
        Assert.assertNull(SchemaFlattener.extractValue(new SchemaFlattener.Column( Arrays.asList("baguette"),null, null, null), curryWurstRecord));
        Assert.assertNull(SchemaFlattener.extractValue(new SchemaFlattener.Column( Arrays.asList("baguette", "taste"),null, null, null), curryWurstRecord));
        Assert.assertNull(SchemaFlattener.extractValue(new SchemaFlattener.Column( Arrays.asList("wurst", "bip"),null, null, null), curryWurstRecord));
    }


    @Test
    public void testFlatteningSchema() {

        String input =  "{\n" +
                "  \"type\" : \"record\",\n" +
                "  \"name\" : \"Impression\",\n" +
                "  \"namespace\" : \"bip.bop.bot\",\n" +
                "  \"fields\" : [ {\n" +
                "    \"name\" : \"generic\",\n" +
                "    \"type\" : {\n" +
                "      \"type\" : \"record\",\n" +
                "      \"name\" : \"Generic\",\n" +
                "      \"fields\" : [ {\n" +
                "        \"name\" : \"timestamp\",\n" +
                "        \"type\" : \"long\"\n" +
                "      }, {\n" +
                "        \"name\" : \"actionId\",\n" +
                "        \"type\" : \"string\"\n" +
                "      }, {\n" +
                "        \"name\" : \"eventType\",\n" +
                "        \"type\" : {\n" +
                "          \"type\" : \"enum\",\n" +
                "          \"name\" : \"EventType\",\n" +
                "          \"symbols\" : [ \"IMPRESSION\", \"WIN_NOTIFICATION\", \"CLICK_NETWORK\" ]\n" +
                "        }\n" +
                "          }, {\n" +
                "            \"name\" : \"extra\",\n" +
                "            \"type\" : [ {\n" +
                "              \"type\" : \"record\",\n" +
                "              \"name\" : \"DeviceExtra\",\n" +
                "              \"fields\" : [ {\n" +
                "                \"name\" : \"connectionType\",\n" +
                "                \"type\" : [ {\n" +
                "                  \"type\" : \"enum\",\n" +
                "                  \"name\" : \"ConnectionType\",\n" +
                "                  \"symbols\" : [ \"WIFI\", \"CELLULAR\" ]\n" +
                "                }, \"null\" ]\n" +
                "              } ]\n" +
                "            }, \"null\" ]\n" +
                "          } ]\n" +
                "        }\n" +
                "      } ]\n" +
                "    }\n" +
                "  }, " +
                "}\n";

        String expected = "{\n" +
                "  \"type\" : \"record\",\n" +
                "  \"name\" : \"Impression\",\n" +
                "  \"namespace\" : \"bip.bop.bot\",\n" +
                "  \"fields\" : [ {\n" +
                "    \"name\" : \"generic_timestamp\",\n" +
                "    \"type\" : [ \"null\", \"long\" ]\n" +
                "  }, {\n" +
                "    \"name\" : \"generic_actionId\",\n" +
                "    \"type\" : [ \"null\", \"string\" ]\n" +
                "  }, {\n" +
                "    \"name\" : \"generic_eventType\",\n" +
                "    \"type\" : [ \"null\", {\n" +
                "      \"type\" : \"enum\",\n" +
                "      \"name\" : \"EventType\",\n" +
                "      \"symbols\" : [ \"IMPRESSION\", \"WIN_NOTIFICATION\", \"CLICK_NETWORK\" ]\n" +
                "    } ]\n" +
                "  }, {\n" +
                "    \"name\" : \"generic_extra_connectionType\",\n" +
                "    \"type\" : [ \"null\", {\n" +
                "      \"type\" : \"enum\",\n" +
                "      \"name\" : \"ConnectionType\",\n" +
                "      \"symbols\" : [ \"WIFI\", \"CELLULAR\" ]\n" +
                "    } ]\n" +
                "  } ]\n" +
                "}";

        Schema schema = SchemaFlattener.flattenAvroSchema(new Schema.Parser().parse(input)).getSchema();
        Assert.assertEquals(expected, schema.toString(true));
    }


}
