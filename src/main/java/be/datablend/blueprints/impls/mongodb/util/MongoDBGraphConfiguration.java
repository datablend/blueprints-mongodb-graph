package be.datablend.blueprints.impls.mongodb.util;

import be.datablend.blueprints.impls.mongodb.MongoDBGraph;
import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.rexster.Tokens;
import com.tinkerpop.rexster.config.GraphConfiguration;
import com.tinkerpop.rexster.config.GraphConfigurationException;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.HierarchicalConfiguration;
import org.apache.commons.configuration.SubnodeConfiguration;

/**
 * Rexster configuration for MongoDBGraph.  Accepts configuration in rexster.xml as follows:
 *
 * <code>
 *  <graph>
 *    <graph-name>mongoexample</graph-name>
 *    <graph-type>be.datablend.blueprints.impls.mongodb.util.MongoDBGraphConfiguration</graph-type>
 *    <properties>
 *      <host>localhost</host>
 *      <port>27017</port>
 *    </properties>
 *  </graph>
 * </code>
 *
 * To deploy copy the MongoDBGraph jar (with dependencies) to the Rexster ext directory.   Ensure that the MongoDB
 * is running.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class MongoDBGraphConfiguration implements GraphConfiguration {

    @Override
    public Graph configureGraphInstance(Configuration configuration) throws GraphConfigurationException {
        final HierarchicalConfiguration graphSectionConfig = (HierarchicalConfiguration) configuration;
        SubnodeConfiguration orientDbSpecificConfiguration;

        try {
            orientDbSpecificConfiguration = graphSectionConfig.configurationAt(Tokens.REXSTER_GRAPH_PROPERTIES);
        } catch (IllegalArgumentException iae) {
            throw new GraphConfigurationException("Check graph configuration. Missing or empty configuration element: " + Tokens.REXSTER_GRAPH_PROPERTIES);
        }

        try {

            final String host = orientDbSpecificConfiguration.getString("host", "localhost");
            final int port = orientDbSpecificConfiguration.getInt("port", 27017);

            return new MongoDBGraph(host, port);

        } catch (Exception ex) {
            throw new GraphConfigurationException(ex);
        }
    }
}
