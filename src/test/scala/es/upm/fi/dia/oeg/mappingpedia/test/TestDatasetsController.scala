package es.upm.fi.dia.oeg.mappingpedia.test

import java.io.File

import es.upm.fi.dia.oeg.mappingpedia.controller.{DatasetController, DistributionController}
import es.upm.fi.dia.oeg.mappingpedia.model.{Agent, Dataset, Distribution, UnannotatedDistribution}

object TestDatasetsController {
  def main(args:Array[String]) = {
    val organizationId = "test-mobileage-upm";
    val agent = new Agent(organizationId);




    //this.testAddEmptyDataset(agent);
    this.testOneDistributionDataset(agent);

    //val distribution = new UnannotatedDistribution(dataset);
    //val distributionController = DistributionController();
    //this.testAddDistribution(distributionController, distribution);
    println("bye");
  }

  def testAddEmptyDataset(agent: Agent) = {
    val dataset = new Dataset(agent);
    val datasetController = DatasetController();
    datasetController.add(
      dataset:Dataset
      , null
      , true
      , true
    );
  }

  def testOneDistributionDataset(agent: Agent) = {
    val dataset = new Dataset(agent);
    val distribution = new UnannotatedDistribution(dataset);

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
