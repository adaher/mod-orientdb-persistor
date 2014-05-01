package org.vertx.mods;

import java.util.List;

import org.vertx.java.busmods.BusModBase;
import org.vertx.java.core.Handler;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;

import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentPool;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.serializer.OJSONWriter;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;

public class OrientDBGenericLookup extends BusModBase implements Handler<Message<JsonObject>>
{
	protected String address;
	private ODatabaseDocument database;

	@Override
	public void start()
	{
		super.start();
		database = ODatabaseDocumentPool.global().acquire("remote:localhost/CBWeb", "admin", "admin");
		ODatabaseRecordThreadLocal.INSTANCE.set(database);
		address = "odbGenericLookup";

		vertx.eventBus().registerHandler("odbGenericLookup", this);

		container.logger().info("odbGenericLookup started");
	}

	@Override
	public void stop()
	{}

	@Override
	public void handle(Message<JsonObject> message)
	{
		String action = message.body().getString("action");

		switch (action)
		{
			case "fetchByQuery":
				fetchByQuery(message);

				break;
			default:
				sendError(message, "Invalid action: " + action);
		}
	}

	private void fetchByQuery(Message<JsonObject> message)
	{
		long timer = System.currentTimeMillis();
		OSQLSynchQuery<ODocument> osqlSynchQuery = new OSQLSynchQuery<ODocument>(message.body().getString("query"));
		osqlSynchQuery.setFetchPlan(message.body().getString("fetchPlan"));
		List<ODocument> query = database.query(osqlSynchQuery);
		System.out.println("After fetch  - " + (System.currentTimeMillis() - timer));
		// String listToJSON = OJSONWriter.listToJSON(query, "fetchPlan:*:-1,rid,class,type,version,attribSameRow,alwaysFetchEmbedded");
		String listToJSON = OJSONWriter.listToJSON(query, "");

		System.out.println("After listToJSON  - " + (System.currentTimeMillis() - timer));
		JsonArray message2 = new JsonArray(listToJSON);
		System.out.println("After JsonArray  - " + (System.currentTimeMillis() - timer));

		message.reply(message2);
	}

}
