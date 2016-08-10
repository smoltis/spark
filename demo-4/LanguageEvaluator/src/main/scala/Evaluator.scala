package main

import org.apache.spark.SparkContext
import org.apache.spark.mllib.feature.Word2Vec

object Evaluator {
  def getWordModel(sc: SparkContext) = {
    val wikiDocuments = HadoopWikiRDDGenerator
                          .createUsing(sc, withPath = "file:///Data/RandomishWikis.xml")
   
    val rawWikiPages = WikiCleaner.clean(wikiDocuments)
    
    val tokenizedWikiData = rawWikiPages.map(wikiText=>wikiText.split("\\W+").toSeq)
    
    val preparedWikiPageData = tokenizedWikiData.map(wikiWords =>
      wikiWords.map(wikiWord => wikiWord.replaceAll("[.|,|'|\"|?|)|(]", "").trim.toLowerCase)
               .filter(wikiWord => wikiWord.length > 2)
    ).cache
                                             
    val word2Vec = new Word2Vec
    val wordModel = word2Vec.fit(preparedWikiPageData)
    wordModel
  }
}