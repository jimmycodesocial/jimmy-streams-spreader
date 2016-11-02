package com.jimmystreams.social;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;

import java.io.IOException;
import org.apache.log4j.Logger;

/**
 * Singleton connection with OrientDB
 */

public class OrientDBConnection
{
    private static final Object lock = new Object();
    private static volatile OrientGraph instance;
    private final static Logger logger = Logger.getLogger(OrientDBConnection.class);


    /**
     * Retrieves a connection with OrientDB
     * @return Connection
     */
    static public OrientGraph getGraph() throws IOException
    {

        OrientGraph graph = instance;

        if (graph == null) {
            logger.info("Create a new instance of OrientGraph");
            // Synchronize the access to the instance
            synchronized (lock) {
                graph = instance;
                // Double-check
                if (graph == null) {
                    // Todo: Use the configuration
                    String dns = "remote:127.0.0.1/social";
                    String user = "root";
                    String password = "ok";
                    // Create & open the database
                    ODatabaseDocumentTx database = new ODatabaseDocumentTx(dns);
                    database.open(user, password);

                    // Create the graph instance
                    graph = new OrientGraph(database, user, password);
                    instance = graph;
                    logger.info("OrientGraph created");
                }
            }
        }

        graph.setStandardElementConstraints(false);
        logger.info("Activate the object to the current thread");
        graph.makeActive();
        return graph;
    }
}
