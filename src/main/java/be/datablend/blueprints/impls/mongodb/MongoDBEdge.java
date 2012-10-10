package be.datablend.blueprints.impls.mongodb;

import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DBCollection;
import com.mongodb.QueryBuilder;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.util.ExceptionFactory;
import com.tinkerpop.blueprints.util.StringFactory;
import static be.datablend.blueprints.impls.mongodb.util.MongoDBUtil.*;

/**
 * @author Davy Suvee (http://datablend.be)
 */
public class MongoDBEdge extends MongoDBElement implements Edge {

    public MongoDBEdge(final MongoDBGraph graph) {
        super(graph);
    }

    public MongoDBEdge(final MongoDBGraph graph, final Object id) {
        super(graph);
        this.id = id;
    }

    @Override
    public Vertex getVertex(Direction direction) throws IllegalArgumentException {
        if (direction.equals(Direction.OUT))
            return new MongoDBVertex(graph, getDBCollection().findOne(QueryBuilder.start(MONGODB_ID).is(id).get(),
                                                                      BasicDBObjectBuilder.start(OUT_VERTEX_PROPERTY, 1).get()).get(OUT_VERTEX_PROPERTY));
        else if (direction.equals(Direction.IN))
            return new MongoDBVertex(graph, getDBCollection().findOne(QueryBuilder.start(MONGODB_ID).is(id).get(),
                                                                      BasicDBObjectBuilder.start(IN_VERTEX_PROPERTY, 1).get()).get(IN_VERTEX_PROPERTY));
        else
            throw ExceptionFactory.bothIsNotSupported();
    }

    @Override
    public String getLabel() {
        return (String)getDBCollection().findOne(QueryBuilder.start(MONGODB_ID).is(id).get(),
                                                 BasicDBObjectBuilder.start(StringFactory.LABEL, 1).get()).get(StringFactory.LABEL);
    }

    public String toString() {
        return StringFactory.edgeString(this);
    }

    public boolean equals(final Object object) {
        return object instanceof MongoDBEdge && ((MongoDBEdge)object).getId().equals(this.getId());
    }

    @Override
    public DBCollection getDBCollection() {
        return graph.getEdgeCollection();
    }

}
