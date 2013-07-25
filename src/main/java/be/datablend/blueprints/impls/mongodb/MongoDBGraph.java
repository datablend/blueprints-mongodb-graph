package be.datablend.blueprints.impls.mongodb;

import com.mongodb.*;
import com.tinkerpop.blueprints.*;
import be.datablend.blueprints.impls.mongodb.util.MongoDBUtil;
import com.tinkerpop.blueprints.util.ExceptionFactory;
import com.tinkerpop.blueprints.util.StringFactory;
import java.net.UnknownHostException;
import java.util.*;
import static be.datablend.blueprints.impls.mongodb.util.MongoDBUtil.*;
import com.tinkerpop.blueprints.util.DefaultGraphQuery;

/**
 * A Blueprints implementation of a graph on top of MongoDB
 *
 * @author Davy Suvee (http://datablend.be)
 */
public class MongoDBGraph implements MetaGraph<DB>, KeyIndexableGraph {

    private final DB graphDatabase;
    private final DBCollection edgeCollection;
    private final DBCollection vertexCollection;
    public static final String MONGODB_ERROR_EXCEPTION_MESSAGE = "An error occured within the MongoDB datastore";
    public static final String MONGODB_ERROR_AUTH_MESSAGE = "Unable to authenticate with supplied username and password";
    private static final Features FEATURES = new Features();

    /**
     * Construct a graph on top of MongoDB
     */
    public MongoDBGraph(final String host, final int port) {
        try {
            Mongo mongo = new Mongo(host, port);
            graphDatabase = mongo.getDB(GRAPH_DATABASE);
            edgeCollection = graphDatabase.getCollection(EDGE_COLLECTION);
            vertexCollection = graphDatabase.getCollection(VERTEX_COLLECTION);
            edgeCollection.ensureIndex(new BasicDBObject().append(IN_VERTEX_PROPERTY, 1));
            edgeCollection.ensureIndex(new BasicDBObject().append(OUT_VERTEX_PROPERTY, 1));
        } catch (UnknownHostException e) {
            throw new RuntimeException(MongoDBGraph.MONGODB_ERROR_EXCEPTION_MESSAGE);
        }
    }

    /**
     *
     * @param graphDatabase
     */
    public MongoDBGraph(DB graphDatabase) {
        
        this.graphDatabase = graphDatabase;
        edgeCollection = graphDatabase.getCollection(EDGE_COLLECTION);
        vertexCollection = graphDatabase.getCollection(VERTEX_COLLECTION);
        edgeCollection.ensureIndex(new BasicDBObject().append(IN_VERTEX_PROPERTY, 1));
        edgeCollection.ensureIndex(new BasicDBObject().append(OUT_VERTEX_PROPERTY, 1));

    }

    /**
     * Construct a graph on top of MongoDB
     */
    public MongoDBGraph(final String host, final int port, final String username, final String password) {
        try {
            Mongo mongo = new Mongo(host, port);
            graphDatabase = mongo.getDB(GRAPH_DATABASE);
            graphDatabase.authenticateCommand(username, password.toCharArray());
            edgeCollection = graphDatabase.getCollection(EDGE_COLLECTION);
            vertexCollection = graphDatabase.getCollection(VERTEX_COLLECTION);
            edgeCollection.ensureIndex(new BasicDBObject().append(IN_VERTEX_PROPERTY, 1));
            edgeCollection.ensureIndex(new BasicDBObject().append(OUT_VERTEX_PROPERTY, 1));
        } catch (UnknownHostException e) {
            throw new RuntimeException(MongoDBGraph.MONGODB_ERROR_EXCEPTION_MESSAGE);
        } catch (MongoException e) {
            throw new RuntimeException(MongoDBGraph.MONGODB_ERROR_AUTH_MESSAGE, e);
        }
    }

    static {
        FEATURES.supportsDuplicateEdges = true;
        FEATURES.supportsSelfLoops = true;
        FEATURES.isPersistent = true;
        FEATURES.supportsVertexIteration = true;
        FEATURES.supportsEdgeIteration = true;
        FEATURES.supportsVertexIndex = false;
        FEATURES.supportsEdgeIndex = false;
        FEATURES.ignoresSuppliedIds = true;
        FEATURES.supportsEdgeRetrieval = true;
        FEATURES.supportsVertexProperties = true;
        FEATURES.supportsEdgeProperties = true;
        FEATURES.supportsTransactions = false;
        FEATURES.supportsIndices = false;

        FEATURES.supportsSerializableObjectProperty = true;
        FEATURES.supportsBooleanProperty = true;
        FEATURES.supportsDoubleProperty = true;
        FEATURES.supportsFloatProperty = true;
        FEATURES.supportsIntegerProperty = true;
        FEATURES.supportsPrimitiveArrayProperty = true;
        FEATURES.supportsUniformListProperty = true;
        FEATURES.supportsMixedListProperty = true;
        FEATURES.supportsLongProperty = true;
        FEATURES.supportsMapProperty = true;
        FEATURES.supportsStringProperty = true;

        FEATURES.isWrapper = true;
        FEATURES.supportsKeyIndices = true;
        FEATURES.supportsVertexKeyIndex = true;
        FEATURES.supportsEdgeKeyIndex = true;
        FEATURES.supportsThreadedTransactions = false;
    }

    @Override
    public void shutdown() {
        // No actions required
    }

    @Override
    public Features getFeatures() {
        return FEATURES;
    }

    public void clear() {
        edgeCollection.remove(new BasicDBObject());
        edgeCollection.dropIndexes();
        vertexCollection.remove(new BasicDBObject());
        vertexCollection.dropIndexes();
    }

    @Override
    public Edge getEdge(final Object id) {
        if (null == id) {
            throw ExceptionFactory.edgeIdCanNotBeNull();
        }
        try {
            final UUID theId = UUID.fromString(id.toString());
            return new MongoDBEdge(this, theId);
        } catch (Exception e) {
            return null;
        }
    }

    @Override
    public Iterable<Edge> getEdges() {
        DBCursor cursor = edgeCollection.find(new BasicDBObject(), BasicDBObjectBuilder.start(MONGODB_ID, 1).get());
        return new MongoDBIterable<Edge>(cursor, this, Edge.class);
    }

    @Override
    public Iterable<Edge> getEdges(String key, Object value) {
        DBCursor cursor = edgeCollection.find(QueryBuilder.start(MongoDBUtil.createPropertyKey(key)).is(value).get(),
                BasicDBObjectBuilder.start(MONGODB_ID, 1).get());
        return new MongoDBIterable<Edge>(cursor, this, Edge.class);
    }

    @Override
    public Edge addEdge(final Object id, final Vertex outVertex, final Vertex inVertex, final String label) {
        if (label == null) {
            throw new IllegalArgumentException("Cannot set null edge label");
        }
        final MongoDBEdge edge = new MongoDBEdge(this);
        edgeCollection.insert(new BasicDBObject().append(MONGODB_ID, edge.getId())
                .append(IN_VERTEX_PROPERTY, inVertex.getId())
                .append(OUT_VERTEX_PROPERTY, outVertex.getId())
                .append(StringFactory.LABEL, label));
        return edge;
    }

    @Override
    public void removeEdge(final Edge edge) {
        edgeCollection.remove(QueryBuilder.start(MONGODB_ID).is(edge.getId()).get());
    }

    @Override
    public Vertex addVertex(final Object id) {
        MongoDBVertex vertex = new MongoDBVertex(this);
        vertexCollection.insert(new BasicDBObject().append(MONGODB_ID, vertex.getId()));
        return vertex;
    }

    @Override
    public Vertex getVertex(final Object id) {
        if (null == id) {
            throw ExceptionFactory.edgeIdCanNotBeNull();
        }
        try {
            final UUID theId = UUID.fromString(id.toString());
            return new MongoDBVertex(this, theId);
        } catch (Exception e) {
            return null;
        }
    }

    @Override
    public Iterable<Vertex> getVertices() {
        DBCursor cursor = vertexCollection.find(new BasicDBObject(), BasicDBObjectBuilder.start(MONGODB_ID, 1).get());
        return new MongoDBIterable<Vertex>(cursor, this, Vertex.class);
    }

    @Override
    public Iterable<Vertex> getVertices(String key, Object value) {
        DBCursor cursor = vertexCollection.find(QueryBuilder.start(MongoDBUtil.createPropertyKey(key)).is(value).get(),
                BasicDBObjectBuilder.start(MONGODB_ID, 1).get());
        return new MongoDBIterable<Vertex>(cursor, this, Vertex.class);
    }

    @Override
    public void removeVertex(final Vertex vertex) {
        MongoDBVertex thevertex = (MongoDBVertex) vertex;
        Iterator<Edge> inedgesit = thevertex.getInEdges().iterator();
        while (inedgesit.hasNext()) {
            removeEdge(inedgesit.next());
        }
        Iterator<Edge> outedgesit = thevertex.getOutEdges().iterator();
        while (outedgesit.hasNext()) {
            removeEdge(outedgesit.next());
        }
        vertexCollection.remove(QueryBuilder.start(MongoDBUtil.MONGODB_ID).is(vertex.getId()).get());
    }

    @Override
    public DB getRawGraph() {
        return graphDatabase;
    }

    public DBCollection getEdgeCollection() {
        return edgeCollection;
    }

    public DBCollection getVertexCollection() {
        return vertexCollection;
    }

    @Override
    public String toString() {
        return StringFactory.graphString(this, this.graphDatabase.getMongo().getConnectPoint());
    }

    @Override
    public <T extends Element> void dropKeyIndex(String key, Class<T> elementClass) {
        getCollection(elementClass).dropIndex(createPropertyKey(key) + "_1");
    }

    @Override
    public <T extends Element> void createKeyIndex(String key, Class<T> elementClass, Parameter... indexParameters) {
        getCollection(elementClass).ensureIndex(createPropertyKey(key));
    }

    @Override
    public <T extends Element> Set<String> getIndexedKeys(Class<T> elementClass) {
        Set<String> results = new HashSet<String>();
        List<DBObject> indexes = getCollection(elementClass).getIndexInfo();
        for (DBObject index : indexes) {
            String name = ((DBObject) index.get("key")).keySet().iterator().next();
            if (!name.equals(MONGODB_ID) && (!name.startsWith("graph:"))) {
                results.add(name);
            }
        }
        return results;
    }

    private <T extends Element> DBCollection getCollection(Class<T> elementClass) {
        if (Edge.class.isAssignableFrom(elementClass)) {
            return edgeCollection;
        }
        return vertexCollection;
    }

    public GraphQuery query() {
        return new DefaultGraphQuery(this);
    }
}
