/**
 * jimmy-streams-api
 * Copyright(c) 2016 Jimmy Code Social (http://jimmycode.com)
 * ISC Licensed
 */

package com.jimmystreams.social.strategies;

import com.jimmystreams.social.ActivityVerbStrategy;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.orient.OrientEdge;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.json.JSONObject;

import java.util.List;

abstract class BaseActivityVerbStrategy implements ActivityVerbStrategy
{
    protected OrientGraph graph;
    protected Boolean isObjectNew;

    protected Logger logger = Logger.getLogger(BaseActivityVerbStrategy.class);

    protected static final int REPUTATION_FACTOR = 2;

    protected abstract Boolean isVerbEdgeable();

    protected abstract Boolean existObjectTypeInGraph(JSONObject object);

    protected abstract double getReputationValueForActor();

    public BaseActivityVerbStrategy(OrientGraph graph) {
        this.graph = graph;
        this.isObjectNew = false;
    }

    public void handleActivity(JSONObject activity)
    {
        // Handle Actor in Activity
        JSONObject activityActor = activity.getJSONObject("actor");
        OrientVertex actor = this.findOrCreateVertex(activityActor, this.getVertexClass(activityActor));

        // Update actor reputation based on the activity verb
        double newScore = (double)actor.getProperty("score") + this.getReputationValueForActor();
        logger.info(String.format("Actor score updated to %f", newScore));
        actor.setProperty("score", newScore);

        // Handle Object in Activity
        OrientVertex object = null;


        JSONObject activityObject = this.getActivitySocialObject(activity);

        if (this.existObjectTypeInGraph(activityObject))
        {
            object = this.findOrCreateVertex(activityObject, this.getVertexClass(activityObject));

            // Check if the relation between actor and object exist or if it needs to be created.
            if (this.isVerbEdgeable()) {
                this.handleRelationActorObject(actor, object, this.getGraphLabel(activity));
            }

            if (this.isObjectReputationAffectedByVerb()) {
                double score = object.getProperty("score");
                object.setProperty("score", score + this.getReputationValueForObject());
            }
        }

        // Complete Activity
        this.acknowledgeActivity(activity, actor, object);

        logger.info("Commit info to the graph");
        // End transaction with graph database
        this.graph.commit();
    }

    private Boolean handleRelationActorObject(OrientVertex actor, OrientVertex object, String label)
    {
        // Check if its needed to create a new edge between the nodes
        Boolean createNewEdge = true;

        Iterable<Edge> edges = actor.getEdges(object, Direction.OUT, label);
        if (edges.iterator().hasNext()) {
            logger.info("The edge already exist");
            createNewEdge = false;
        }
        // Created the edge if it is necessary
        if (createNewEdge) {
            logger.info("Create new edge");
            graph.addEdge(null, actor, object, label);
        }

        return createNewEdge;
    }

    protected void acknowledgeActivity(JSONObject activity, OrientVertex actor, OrientVertex object){
        // nothing to do
    }

    protected Boolean isObjectReputationAffectedByVerb() {
        return false;
    }

    protected double getReputationValueForObject()
    {
        return 0;
    }

    protected String getVertexClass(JSONObject vertex)
    {
        return StringUtils.capitalize(vertex.get("objectType").toString());
    }

    protected String getGraphLabel(JSONObject activity)
    {
        return activity.getString("verb");
    }

    protected OrientEdge findOrCreateEdge(OrientVertex from, OrientVertex to, String verb, Direction direction)
    {
        Iterable<Edge> edges = from.getEdges(to, direction, verb);
        return edges.iterator().hasNext() ? (OrientEdge)edges.iterator().next() : graph.addEdge(null, from, to, verb);
    }

    protected OrientVertex findOrCreateVertex(JSONObject item, String targetClass)
    {
        Iterable<Vertex> vertices = graph.getVertices(targetClass, new String[] {"id"}, new Object[] {item.get("id")});
        OrientVertex vertex;

        if (vertices.iterator().hasNext()) {

            vertex = (OrientVertex)vertices.iterator().next();
        }
        else {
            vertex = graph.addVertex(String.format("class:%s", targetClass));
            vertex.setProperty("id", item.get("id"));
            vertex.setProperty("score", 0.0);
        }
        return vertex;
    }

    protected void connectTechnologies(List<OrientVertex> techs)
    {
        for(OrientVertex tech: techs)
        {
            for(int j = 1; j <= techs.size() - 1; j++)
            {
                OrientEdge link = this.findOrCreateEdge(tech, techs.get(j), "related", Direction.BOTH);
                int score = link.getProperty("score") != null ? (int) link.getProperty("score") : 0;
                link.setProperty("score", ++score);
            }
        }
    }

    protected JSONObject getActivitySocialObject(JSONObject activity)
    {
        return activity.getJSONObject("object");
    }
}
