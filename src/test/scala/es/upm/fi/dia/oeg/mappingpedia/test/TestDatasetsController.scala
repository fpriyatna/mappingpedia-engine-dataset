package es.upm.fi.dia.oeg.mappingpedia.test

import java.io.File

import es.upm.fi.dia.oeg.mappingpedia.controller.{DatasetController, DistributionController}
import es.upm.fi.dia.oeg.mappingpedia.model.{Agent, Dataset, Distribution, UnannotatedDistribution}
import org.slf4j.{Logger, LoggerFactory}

object TestDatasetsController {
  val logger: Logger = LoggerFactory.getLogger(this.getClass);

  val organizationId = "test-mobileage-upm";
  val agent = new Agent(organizationId);
  val dataset = new Dataset(agent);
  dataset.ckanOrganizationName = "test-mobileage-upm3";

  val datasetController = DatasetController();

  def main(args:Array[String]) = {




    //this.testAddEmptyDataset(agent);
    //this.testOneDistributionDataset(agent);

    //val distribution = new UnannotatedDistribution(dataset);
    //val distributionController = DistributionController();
    //this.testAddDistribution(distributionController, distribution);

    this.testFindAll();
    println("bye");
  }

  def testFindAll() = {
    val allDatasets = datasetController.findAll();
    logger.info(s"allDatasets = ${allDatasets}")

  }

  def testAddEmptyDataset() = {
    datasetController.add(
      dataset:Dataset
      , null
      , true
      , true
    );
  }

  def testOneDistributionDataset(agent:Agent) = {
    val distribution = new UnannotatedDistribution(dataset);
    distribution.dcatDownloadURL = "https://raw.githubusercontent.com/oeg-upm/morph-rdb/master/morph-examples/examples-csv/edificio-historico.csv"

    val datasetController = DatasetController();
    datasetController.add(
      dataset:Dataset
      , null
      , true
      , true
    );
  }

  def testAddDistribution(distributionController: DistributionController
                          , distribution:Distribution) = {
    distributionController.addDistribution(
      distribution: Distribution
      , null
      , true
      , true
    )
  }


}
