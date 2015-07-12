package com.es.narayan

import org.elasticsearch.action.admin.indices.refresh.RefreshRequest
import org.elasticsearch.client.Client
import org.elasticsearch.node.NodeBuilder.nodeBuilder
import org.elasticsearch.index.query.QueryBuilders

object ESApiObject extends App with ESScrollApi {


  /**
   * getClient method returns local node client
   *
   * @return
   */
  def getClient(): Client = {
    val node = nodeBuilder().local(true).node()
    val client = node.client()
    insertBulkDoc(client)
    val refresh = new RefreshRequest()
    client.admin().indices().refresh(refresh)
    Thread.sleep(2000)
    client.admin().indices().refresh(refresh)
    client
  }
  val client = getClient

  val result = scrollFetch(client, QueryBuilders.matchAllQuery(), "gnip_index", "/tmp/outputJson.json")
  log.info("total number of scrolled documents is " + result)
  deleteIndex(client, "gnip_index")
  client.close()
}