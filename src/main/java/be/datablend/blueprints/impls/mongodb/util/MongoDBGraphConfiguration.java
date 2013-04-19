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
 *      <!-- Username and password elements are optional -->
 *      <username>username</username>
 *      <password>password</password>
 *    </properties>
 *  </graph>
 * </code>
 *
 * Note username and password elements are optional
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

        final String username = orientDbSpecificConfiguration.getString("username", null);
        final String password = orientDbSpecificConfiguration.getString("password", null);
        final String host = orientDbSpecificConfiguration.getString("host", "localhost");
        final int port = orientDbSpecificConfiguration.getInt("port", 27017);

        if(username != null && password != null) {
            // create mongo graph with username and password
            try {
                return new MongoDBGraph(host, port, username, password);
            } catch (Exception ex) {
                throw new GraphConfigurationException(ex);
            }
        } else {
            try {
                return new MongoDBGraph(host, port);

            } catch (Exception ex) {
                throw new GraphConfigurationException(ex);
            }   
        }
        
    }
}
