package be.datablend.blueprints.impls.mongodb;

import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.tinkerpop.blueprints.CloseableIterable;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Vertex;
import java.util.Iterator;
import java.util.List;
import static be.datablend.blueprints.impls.mongodb.util.MongoDBUtil.MONGODB_ID;
import java.util.NoSuchElementException;

/**
 * @author Davy Suvee (http://datablend.be)
 */
public class MongoDBIterable<T extends Element> implements CloseableIterable<T> {

    private DBCursor cursorIds;
    private List<Object> ids;
    private final MongoDBGraph graph;
    private Class<T> clazz;

    public MongoDBIterable(final DBCursor cursorIds, final MongoDBGraph graph, final Class<T> clazz) {
        this.cursorIds = cursorIds;
        this.graph = graph;
        this.clazz = clazz;
    }

    public MongoDBIterable(final List<Object> ids, final MongoDBGraph graph, final Class<T> clazz) {
        this.graph = graph;
        this.ids = ids;
        this.clazz = clazz;
    }

    @Override
    public Iterator<T> iterator() {
        if (cursorIds != null) {
            return new MongoDBCursorIterator();
        }
        else {
            return new MongoDBIdIterator();
        }
    }

    @Override
    public void close() {
    }

    private class MongoDBCursorIterator implements Iterator<T> {
        private Iterator<DBObject> iterator = cursorIds.iterator();

        @Override
        public boolean hasNext() {
            return iterator.hasNext();
        }

        @Override
        public T next() {
            if (!hasNext())
               throw new NoSuchElementException();

            Object object = this.iterator.next().get(MONGODB_ID);
            T ret = null;
            if (clazz == Vertex.class) {
                ret = (T) new MongoDBVertex(graph, object);
            } else if (clazz == Edge.class) {
                ret = (T) new MongoDBEdge(graph, object);
            } else {
                throw new IllegalStateException();
            }
            return ret;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }

    private class MongoDBIdIterator implements Iterator<T> {
        private Iterator<Object> iterator = ids.iterator();

        @Override
        public boolean hasNext() {
            return iterator.hasNext();
        }

        @Override
        public T next() {
            T ret = null;
            if (clazz == Vertex.class) {
                ret = (T) new MongoDBVertex(graph, iterator.next());
            } else if (clazz == Edge.class) {
                ret = (T) new MongoDBEdge(graph, iterator.next());
            } else {
                throw new IllegalStateException();
            }
            return ret;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }

}
