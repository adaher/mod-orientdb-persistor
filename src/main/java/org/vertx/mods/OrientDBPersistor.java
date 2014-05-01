/*
 * Copyright 2011-2012 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.serializer.OJSONWriter;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;

public class OrientDBPersistor extends BusModBase implements Handler<Message<JsonObject>>
{
	protected String address;
	private ODatabaseDocument database;

	@Override
	public void start()
	{
		super.start();
		database = ODatabaseDocumentPool.global().acquire("remote:localhost/CBWeb", "admin", "admin");
		ODatabaseRecordThreadLocal.INSTANCE.set(database);
		address = "odb";// getOptionalStringConfig("address", "vertx.orientdbpersistor");
		// connect db
		// eb.registerHandler(address, this);

		vertx.eventBus().registerHandler("odb", this);

		container.logger().info("ODBP started");
	}

	@Override
	public void stop()
	{}

	@Override
	public void handle(Message<JsonObject> message)
	{

		String action = message.body().getString("action");

		if (action == null)
		{
			sendError(message, "action must be specified");
			return;
		}
		// Note actions should not be in camel case, but should use underscores
		// I have kept the version with camel case so as not to break compatibility

		switch (action)
		{
			case "findByRID":
				findByRID(message);
				break;
			case "fetchGraph":
				fetchGraph(message);
				break;
			case "newGraph":
				newGraph(message);
				break;
			case "saveGraph":
				saveGraph(message);
				break;
			case "deleteGraph":
				deleteGraph(message);
				break;
			default:
				sendError(message, "Invalid action: " + action);
		}
	}

	public void findByRID(Message<JsonObject> message)
	{
		String rid = getMandatoryString("rid", message);
		String fetchPlan = message.body().getString("fetchPlan");

		try
		{
			ODocument doc = null;
			if (fetchPlan != null)
			{
				doc = getDatabase().load(new ORecordId(rid), fetchPlan);
			}
			else
			{
				doc = getDatabase().load(new ORecordId(rid));
			}
			message.reply(new JsonObject(doc.toJSON("fetchPlan:*:-1,rid,class,type,version,attribSameRow,alwaysFetchEmbedded")));
		}
		catch (Exception e)
		{
			logger.error(e, e);
		}
	}

	public void fetchGraph(Message<JsonObject> message)
	{
		try
		{
			String className = getMandatoryString("className", message);
			String criteria = getMandatoryString("criteria", message);
			String fetchPlan = getMandatoryString("fetchPlan", message);

			getDatabase().begin();
			List<ODocument> resultset = getDatabase().query(
					new OSQLSynchQuery<ODocument>(String.format("select from %s where %s", className, criteria)).setFetchPlan(fetchPlan));
			getDatabase().commit();
			message.reply(new JsonArray(OJSONWriter.listToJSON(resultset, "fetchPlan:*:-1,rid,class,type,version,attribSameRow,alwaysFetchEmbedded")));
		}
		catch (Exception e)
		{
			logger.error(e, e);
		}
	}

	public void newGraph(Message<JsonObject> message)
	{
		try
		{
			String className = getMandatoryString("className", message);
			ODocument doc = getDatabase().newInstance();
			doc.setClassName(className);
			message.reply(new JsonObject(doc.toJSON("fetchPlan:*:-1,rid,class,type,version,attribSameRow,alwaysFetchEmbedded")));
		}
		catch (Exception e)
		{
			logger.error(e, e);
		}
	}

	public void saveGraph(Message<JsonObject> message)
	{
		try
		{
			JsonObject jsonToSave = getMandatoryObject("jsonToSave", message);

			getDatabase().begin();
			ODocument doc = new ODocument();
			doc.fromJSON(jsonToSave.toString());

			// TODO CLUSTERS
			getDatabase().save(doc, doc.getClassName());
			getDatabase().commit();
			message.body().putString("fetchPlan", "*:-1");
			message.body().putString("rid", doc.getIdentity().toString());
			findByRID(message);
		}
		catch (Exception e)
		{
			logger.error(e, e);
		}
	}

	public void deleteGraph(Message<JsonObject> message)
	{
		JsonObject jsonToDelete = getMandatoryObject("jsonToDelete", message);
		getDatabase().begin();
		ODocument doc = new ODocument();
		doc.fromJSON(jsonToDelete.toString());
		getDatabase().delete(doc.getIdentity());
		getDatabase().commit();
		newGraph(message);
	}

	private ODatabaseRecord getDatabase()
	{
		return ODatabaseRecordThreadLocal.INSTANCE.get();

	}

}
