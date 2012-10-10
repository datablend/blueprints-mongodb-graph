package be.datablend.blueprints.impls.mongodb.util;

/**
 * @author Davy Suvee (http://datablend.be)
 */
public class MongoDBUtil {

    public static String GRAPH_DATABASE = "graphdatabase";
    public static String EDGE_COLLECTION = "edgecollection";
    public static String VERTEX_COLLECTION = "vertexcollection";
    public static String IN_VERTEX_PROPERTY = "graph:invertex";
    public static String OUT_VERTEX_PROPERTY = "graph:outvertex";
    public static String MONGODB_ID = "_id";

    public static String createPropertyKey(String key) {
        return key.replace("_",":");
    }

    public static String getPropertyName(String key) {
        return key.replace(":","_");
    }

}
