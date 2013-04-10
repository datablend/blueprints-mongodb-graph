package be.datablend.blueprints.impls.mongodb;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.QueryBuilder;
import com.tinkerpop.blueprints.Element;
import be.datablend.blueprints.impls.mongodb.util.MongoDBUtil;
import com.tinkerpop.blueprints.util.StringFactory;
import java.util.*;
import static be.datablend.blueprints.impls.mongodb.util.MongoDBUtil.MONGODB_ID;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.util.ElementHelper;

/**
 * @author Davy Suvee (http://datablend.be)
 */
public abstract class MongoDBElement implements Element {

    protected final MongoDBGraph graph;
    protected Object id;

    protected MongoDBElement(final MongoDBGraph graph) {
        this.graph = graph;
        id = UUID.randomUUID();
    }

    @Override
    public Object getId() {
        return id;
    }

    @Override
    public Set<String> getPropertyKeys() {
        Set<String> finalproperties = new HashSet<String>();
        DBObject element = getDBCollection().findOne(QueryBuilder.start(MONGODB_ID).is(id).get());
        Map<String, Object> elementmap = element.toMap();
        Iterator<String> propertiesit = elementmap.keySet().iterator();
        while (propertiesit.hasNext()) {
            String property = propertiesit.next();
            if (!property.startsWith("graph:") && (!property.startsWith("_id")) && (!property.equals(StringFactory.LABEL))) {
                finalproperties.add(MongoDBUtil.getPropertyName(property));
            }
        }
        return finalproperties;
    }

    @Override
    public Object getProperty(final String key) {
        DBObject element = null;
        if (!key.equals("")) {
            element = getDBCollection().findOne(QueryBuilder.start(MONGODB_ID).is(id).get(),
                    new BasicDBObject().append(MongoDBUtil.createPropertyKey(key), 1));
        }
        else {
            element = getDBCollection().findOne(QueryBuilder.start(MONGODB_ID).is(id).get());
        }
        return element.get(MongoDBUtil.createPropertyKey(key));
    }

    @Override
    public void setProperty(final String key, final Object value) {
        ElementHelper.validateProperty(this, key, value);
        
        getDBCollection().update(QueryBuilder.start(MONGODB_ID).is(id).get(),
                                 new BasicDBObject().append("$set", new BasicDBObject().append(MongoDBUtil.createPropertyKey(key), value)));
    }

    @Override
    public Object removeProperty(final String key) {
        Object oldvalue = getProperty(key);
        getDBCollection().update(QueryBuilder.start(MONGODB_ID).is(id).get(),
                new BasicDBObject().append("$unset", new BasicDBObject().append(MongoDBUtil.createPropertyKey(key), 1)));
        return oldvalue;
    }

    @Override
    public void remove() {
        if (this instanceof Vertex) {
            this.graph.removeVertex((Vertex) this);
        } else {
            this.graph.removeEdge((Edge) this);
        }
    }
    
    @Override
    public int hashCode() {
        return this.getId().hashCode();
    }

    @Override
    public boolean equals(final Object object) {
        return (null != object) && (this.getClass().equals(object.getClass()) && this.getId().equals(((Element)object).getId()));
    }

    public abstract DBCollection getDBCollection();

}