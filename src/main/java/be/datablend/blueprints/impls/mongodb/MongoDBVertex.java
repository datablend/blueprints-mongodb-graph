package be.datablend.blueprints.impls.mongodb;

import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DBCollection;
import com.mongodb.QueryBuilder;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.util.MultiIterable;
import com.tinkerpop.blueprints.util.StringFactory;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import static be.datablend.blueprints.impls.mongodb.util.MongoDBUtil.*;
import com.tinkerpop.blueprints.VertexQuery;
import com.tinkerpop.blueprints.util.DefaultVertexQuery;

/**
 * @author Davy Suvee (http://datablend.be)
 */
public class MongoDBVertex extends MongoDBElement implements Vertex {

    public MongoDBVertex(final MongoDBGraph graph) {
        super(graph);
    }

    public MongoDBVertex(final MongoDBGraph graph, final Object id) {
        super(graph);
        this.id = id;
    }

    public Iterable<Edge> getInEdges(final String... labels) {
        QueryBuilder builder = QueryBuilder.start(IN_VERTEX_PROPERTY).is(id);
        if (labels.length != 0) {
            builder.and(StringFactory.LABEL).in(labels);
        }
        return new MongoDBIterable<Edge>(graph.getEdgeCollection().find(builder.get(), BasicDBObjectBuilder.start(MONGODB_ID,1).get()), graph, Edge.class);
    }

    public Iterable<Edge> getOutEdges(final String... labels) {
        QueryBuilder builder = QueryBuilder.start(OUT_VERTEX_PROPERTY).is(id);
        if (labels.length != 0) {
            builder.and(StringFactory.LABEL).in(labels);
        }
        return new MongoDBIterable<Edge>(graph.getEdgeCollection().find(builder.get(), BasicDBObjectBuilder.start(MONGODB_ID,1).get()), graph, Edge.class);
    }

    @Override
    public boolean equals(final Object object) {
        return object instanceof MongoDBVertex && ((MongoDBVertex)object).getId().equals(this.getId());
    }

    @Override
    public String toString() {
        return StringFactory.vertexString(this);
    }

    @Override
    public Iterable<Edge> getEdges(Direction direction, String... labels) {
        if (direction.equals(Direction.OUT)) {
            return this.getOutEdges(labels);
        } else if (direction.equals(Direction.IN))
            return this.getInEdges(labels);
        else {
            return new MultiIterable<Edge>(Arrays.asList(this.getInEdges(labels), this.getOutEdges(labels)));
        }
    }

    @Override
    public Iterable<Vertex> getVertices(Direction direction, String... labels) {
        if (direction.equals(Direction.OUT)) {
            Iterator<Edge> edgesit = this.getOutEdges(labels).iterator();
            List<Object> vertices = new ArrayList<Object>();
            while (edgesit.hasNext()) {
                vertices.add(edgesit.next().getVertex(Direction.IN).getId());
            }
            return new MongoDBIterable(vertices, this.graph, Vertex.class);
        } else if (direction.equals(Direction.IN)) {
            Iterator<Edge> edgesit = this.getInEdges(labels).iterator();
            List<Object> vertices = new ArrayList<Object>();
            while (edgesit.hasNext()) {
                vertices.add(edgesit.next().getVertex(Direction.OUT).getId());
            }
            return new MongoDBIterable(vertices, this.graph, Vertex.class);
        }
        else {
            Iterator<Edge> outEdgesIt = this.getOutEdges(labels).iterator();
            List<Object> outvertices = new ArrayList<Object>();
            while (outEdgesIt.hasNext()) {
                outvertices.add(outEdgesIt.next().getVertex(Direction.IN).getId());
            }
            Iterator<Edge> inEdgesIt = this.getInEdges(labels).iterator();
            List<Object> invertices = new ArrayList<Object>();
            while (inEdgesIt.hasNext()) {
                invertices.add(inEdgesIt.next().getVertex(Direction.OUT).getId());
            }
            return new MultiIterable<Vertex>(Arrays.<Iterable<Vertex>>asList(new MongoDBIterable(outvertices, this.graph, Vertex.class), new MongoDBIterable(invertices, this.graph, Vertex.class)));
        }
    }

    @Override
    public DBCollection getDBCollection() {
        return graph.getVertexCollection();
    }

    @Override
    public VertexQuery query() {
        return new DefaultVertexQuery(this);
    }

    @Override
    public Edge addEdge(String label, Vertex inVertex) {
        return this.graph.addEdge(id, this, inVertex, label);
    }
}
