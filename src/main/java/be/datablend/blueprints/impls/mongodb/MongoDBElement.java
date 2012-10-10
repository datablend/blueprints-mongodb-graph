package be.datablend.blueprints.impls.mongodb;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.QueryBuilder;
import com.tinkerpop.blueprints.Element;
import be.datablend.blueprints.impls.mongodb.util.MongoDBUtil;
import com.tinkerpop.blueprints.util.ExceptionFactory;
import com.tinkerpop.blueprints.util.StringFactory;
import java.util.*;
import static be.datablend.blueprints.impls.mongodb.util.MongoDBUtil.MONGODB_ID;

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

    public Object getId() {
        return id;
    }

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

    public void setProperty(final String key, final Object value) {
        if (key.equals(StringFactory.ID))
            throw ExceptionFactory.propertyKeyIdIsReserved();
        if (key.equals(StringFactory.LABEL))
            throw new IllegalArgumentException("Property key is reserved for all nodes and edges: " + StringFactory.LABEL);
        if (key.equals(StringFactory.EMPTY_STRING))
            throw ExceptionFactory.elementKeyCanNotBeEmpty();
        getDBCollection().update(QueryBuilder.start(MONGODB_ID).is(id).get(),
                                 new BasicDBObject().append("$set", new BasicDBObject().append(MongoDBUtil.createPropertyKey(key), value)));
    }

    public Object removeProperty(final String key) {
        Object oldvalue = getProperty(key);
        getDBCollection().update(QueryBuilder.start(MONGODB_ID).is(id).get(),
                new BasicDBObject().append("$unset", new BasicDBObject().append(MongoDBUtil.createPropertyKey(key), 1)));
        return oldvalue;
    }

    public int hashCode() {
        return this.getId().hashCode();
    }

    public boolean equals(final Object object) {
        return (null != object) && (this.getClass().equals(object.getClass()) && this.getId().equals(((Element)object).getId()));
    }

    public abstract DBCollection getDBCollection();

}