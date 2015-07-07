package org.neo4j.bench.shortestpath;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;

public class Config
{
    static
    {
        try
        {
            Map properties = new Properties();
            ( (Properties) properties ).load( Config.class.getResourceAsStream( "/neo4j_config.properties" ) );
            NEO4J_CONFIG = properties;
        }
        catch ( IOException e )
        {
            e.printStackTrace();
        };
    }

    public static Map<String, String> NEO4J_CONFIG;

    public final static String DB_DIR = "/Users/antonpersson/neo4j/shortestpath_bench/db";

    public final static String RAW_RELATIONSHIP_FILE = "/Users/antonpersson/neo4j/shortestpath_bench/data/raw/facebook_combined.txt";

    public final static String NODE_ID_FILE = "/Users/antonpersson/neo4j/shortestpath_bench/data/generated/nodes.csv";
    public final static String RELATIONSHIP_ID_FILE = "/Users/antonpersson/neo4j/shortestpath_bench/data/generated/relationships.csv";
    public final static String PATH_START_END_ID_FILE = "/Users/antonpersson/neo4j/shortestpath_bench/data/generated/path-start-and-end-nodes.csv";
}
