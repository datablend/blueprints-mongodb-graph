package be.datablend.blueprints.impls.mongodb;

import be.datablend.blueprints.impls.mongodb.MongoDBGraph;
import com.mongodb.BasicDBList;
import com.mongodb.DBCursor;
import com.mongodb.QueryBuilder;
import com.tinkerpop.blueprints.Contains;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.util.DefaultGraphQuery;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * @author Andreas Bannach (andreas.bannach@igd.fraunhofer.de)
 */
public class MongoDBGraphQuery extends DefaultGraphQuery {

    private final MongoDBGraph graph;
    
    public MongoDBGraphQuery(MongoDBGraph graph) {
        super(graph);
        
        this.graph = graph;
    }

    @Override
    public Iterable<Vertex> vertices() {
        
        if (limit == 0) {
            return Collections.emptyList();
        }
        
        QueryBuilder queryBuilder = QueryBuilder.start();
        addFilters(queryBuilder);
        
        DBCursor cursor = graph.getVertexCollection().find(queryBuilder.get());
        if (limit > 0 && limit < Long.MAX_VALUE) {
            cursor.limit(limit);
        }        
        return new MongoDBIterable<Vertex>(cursor, graph, Vertex.class);
    }

    @Override
    public Iterable<Edge> edges() {
        
        if (limit == 0) {
            return Collections.emptyList();
        }
        
        QueryBuilder queryBuilder = QueryBuilder.start();
        addFilters(queryBuilder);
        
        DBCursor cursor = graph.getEdgeCollection().find(queryBuilder.get());
        if (limit > 0 && limit < Long.MAX_VALUE) {
            cursor.limit(limit);
        }        
        return new MongoDBIterable<Edge>(cursor, graph, Edge.class);
    }
    
    private void addFilters(QueryBuilder queryBuilder) {
        
        Set<String> seenKeys = new HashSet<String>();
        for (HasContainer has : hasContainers) {
            QueryBuilder subQuery = null;
            if (seenKeys.contains(has.key)) {
                subQuery = QueryBuilder.start(has.key);
            } else {
                subQuery = queryBuilder.put(has.key);
            }
            
            if (has.predicate instanceof Contains) {
                
                BasicDBList values = new BasicDBList();
                values.addAll((Collection<Object>) has.value);
                
                if (has.predicate == Contains.NOT_IN) {
                    subQuery.notIn(values);
                } else {
                    subQuery.in(values);
                }
            } else {
                
                if (has.predicate instanceof com.tinkerpop.blueprints.Compare) {
                    com.tinkerpop.blueprints.Compare compare = (com.tinkerpop.blueprints.Compare) has.predicate;
                    
                    switch (compare) {
                        case EQUAL:
                            subQuery.is(has.value);
                            break;
                        case NOT_EQUAL:
                            subQuery.notEquals(has.value);
                            break;
                        case GREATER_THAN:
                            subQuery.greaterThan(has.value);
                            break;
                        case GREATER_THAN_EQUAL:
                            subQuery.greaterThanEquals(has.value);
                            break;
                        case LESS_THAN:
                            subQuery.lessThan(has.value);
                            break;
                        case LESS_THAN_EQUAL:
                            subQuery.lessThanEquals(has.value);
                            break;
                    }
                }
            }
            if (seenKeys.contains(has.key)) {
                queryBuilder.and(subQuery.get());
            } else {
                seenKeys.add(has.key);
            }
        }
        
    }
}
