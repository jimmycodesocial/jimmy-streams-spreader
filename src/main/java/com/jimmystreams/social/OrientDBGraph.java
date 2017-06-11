package com.jimmystreams.social;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.tinkerpop.blueprints.impls.orient.OrientConfigurableGraph;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;

import java.io.IOException;
import org.apache.log4j.Logger;


public class OrientDBGraph
{
    private final static Logger logger = Logger.getLogger(OrientDBGraph.class);

    /**
     *
     * @param dsn DSN to the OrientDB Database
     * @param user Database user
     * @param password User password
     * @return OrientGraph Instance
     * @throws IOException Exception
     */
    static public OrientGraph create(String dsn, String user, String password) throws IOException
    {
        logger.info("Trying to create a new instance of OrientGraph");

        // Create & open the database
        ODatabaseDocumentTx database = new ODatabaseDocumentTx(dsn, true);
        database.open(user, password);

        // Create the graph instance
        OrientGraph graph  = new OrientGraph(database, user, password);
        logger.info("OrientGraph instance created");

        graph.setStandardElementConstraints(false);
        logger.info("Activate the object to the current thread");
        graph.setThreadMode(OrientConfigurableGraph.THREAD_MODE.AUTOSET_IFNULL);
        graph.makeActive();
        return graph;
    }
}
