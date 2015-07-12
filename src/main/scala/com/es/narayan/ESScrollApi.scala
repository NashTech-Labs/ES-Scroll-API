package com.es.narayan

import java.io.OutputStreamWriter
import org.elasticsearch.search.SearchHit
import java.io.FileOutputStream
import org.elasticsearch.action.search.SearchType
import scala.annotation.tailrec
import java.io.IOException
import org.elasticsearch.common.unit.TimeValue
import org.elasticsearch.index.query.QueryBuilder
import java.io.File
import org.elasticsearch.client.Client
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest
import scala.io.Source
import org.slf4j.LoggerFactory

/**
 * @author narayan
 *
 */
trait ESScrollApi {
  
  
  val log = LoggerFactory.getLogger(this.getClass)

  /**
   * This is scrollFetch method which provides pagination over selected data
   *
   * @param client
   * @param query
   * @param indexName
   * @param outputFileUrl
   * @param scrollSize
   * @param scrollKeepAlive
   * @return
   */
  def scrollFetch(client: Client, query: QueryBuilder, indexName: String, outputFileUrl: String,
                   scrollSize: Int = 10, scrollKeepAlive: String = "10m"): Int = {

    var scrollResp = client.prepareSearch(indexName).setSearchType(SearchType.SCAN).
      setScroll(scrollKeepAlive).setQuery(query).setSize(scrollSize).execute().actionGet()
    val totalCount = scrollResp.getHits.getTotalHits
    log.info("totalhits " + totalCount)
    var successCount = 0
    if (totalCount > 0) {
      val outputFile = new File(outputFileUrl)
      val outputFileName = outputFile.getAbsolutePath
      val outputStream = new FileOutputStream(outputFile)
      val outputWriter = new OutputStreamWriter(outputStream)
      @tailrec
      def fetch() {
        scrollResp = client.prepareSearchScroll(scrollResp.getScrollId()).setScroll(new TimeValue(60000)).execute().actionGet()
        log.info("scroll length " + scrollResp.getHits.getHits.length)
        if (scrollResp.getHits.getHits.length == 0) {
          try {
            client.prepareClearScroll().addScrollId(scrollResp.getScrollId).execute().actionGet()
            return
          } catch {
            case ex: Throwable =>
              log.error("we can't more scroll due to " + ex)
              return
          }

        } else {
          successCount = writeDataOnLocalFile(scrollResp.getHits.getHits, outputWriter, successCount, outputFileName)
          fetch()
        }
      }
      fetch()
      outputWriter.flush()
      outputWriter.close()
      successCount
    } else successCount
  }

  /**
   * writeDataOnLocalFile is private method which is used for write page data into local file
   *
   * @param searchHit
   * @param outputWriter
   * @param successCount
   * @param outputFileName
   * @return
   */
  private def writeDataOnLocalFile(searchHit: Array[SearchHit], outputWriter: OutputStreamWriter, successCount: Int, outputFileName: String): Int = {
    var successCountModified = successCount
    var badWriter = false
    for (oneHit <- searchHit) {
      if (!badWriter) {
        try {
          val docId = oneHit.getId
          val dataSource = oneHit.getSourceAsString
          val writableDoc = s"""{"_id":${docId},"_source":${dataSource}}\n"""
          outputWriter.write(writableDoc)
          successCountModified += 1
        } catch {
          case ex: IOException =>
            log.error(s"can't write to the file ${outputFileName} due to ${ex}")
            badWriter = true
        }
      }
    }

    successCountModified
  }

  /**
   * This is insertBulkDoc method which is used for read data from file and write into ES index
   *
   * @param client
   */
  def insertBulkDoc(client: Client) {
    val sourceBuffer = Source.fromFile("src/main/resources/inputJson.json")
    val bulkJson = sourceBuffer.getLines().toList
    val bulkRequest = client.prepareBulk()
    for (i <- 0 until bulkJson.size) {
      bulkRequest.add(client.prepareIndex("gnip_index", "twitter", s"${i + 1}").setSource(bulkJson(i)))
    }
    bulkRequest.execute().actionGet()
    sourceBuffer.close()
  }

  /**
   * deleteIndex method is used for delete index from ES node
   *
   * @param client
   * @param indexName
   */
  def deleteIndex(client: Client, indexName: String) {
    val deleteIndexRequest = new DeleteIndexRequest(indexName)
    val deleteResponse = client.admin().indices().delete(deleteIndexRequest).actionGet()
    if (deleteResponse.isAcknowledged())
      log.info("index is successfully deleted")
    else
      log.warn("index is not deleted ")
  }

}