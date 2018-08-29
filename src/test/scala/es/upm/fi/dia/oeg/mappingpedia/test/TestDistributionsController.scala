package es.upm.fi.dia.oeg.mappingpedia.test

import es.upm.fi.dia.oeg.mappingpedia.controller.DistributionController
import org.slf4j.{Logger, LoggerFactory}

object TestDistributionsController {
  val logger: Logger = LoggerFactory.getLogger(this.getClass);
  val distributionsController = DistributionController();

  def main(args:Array[String]) : Unit = {
    this.testFindDistributionsByDatasetId("f787e167-1959-4bae-8de4-c86c119d62ab");
  }


  def testFindDistributionsByDatasetId(datasetId:String) = {
    val foundDistributions = distributionsController.findDistributionsByDatasetId(datasetId);
    logger.info(s"foundDistributions = ${foundDistributions}")
    foundDistributions
  }
}
