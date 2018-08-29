package es.upm.fi.dia.oeg.mappingpedia.controller

import java.io.File
import java.net.HttpURLConnection
import java.text.SimpleDateFormat
import java.util.{Date, Properties}

import com.fasterxml.jackson.databind.ObjectMapper
import com.mashape.unirest.http.{HttpResponse, JsonNode}
import es.upm.fi.dia.oeg.mappingpedia.MappingPediaConstant
import es.upm.fi.dia.oeg.mappingpedia.model.result.{AddDatasetResult, AddDistributionResult, ListResult}
import es.upm.fi.dia.oeg.mappingpedia.model._
import es.upm.fi.dia.oeg.mappingpedia.utility._
import org.apache.http.client.methods.HttpPost
import org.apache.http.entity.mime.MultipartEntityBuilder
import org.apache.http.impl.client.HttpClientBuilder
import org.slf4j.{Logger, LoggerFactory}
import org.springframework.web.multipart.MultipartFile

import scala.io.Source

class DistributionController(
                              val ckanClient:MpcCkanUtility
                              , val githubClient:MpcGithubUtility
                              , val virtuosoClient:MpcVirtuosoUtility
                              , val properties:Properties
                            )
{

  val logger: Logger = LoggerFactory.getLogger(this.getClass);
  val mapper = new ObjectMapper();

  def createResource(distribution: Distribution, textBodyMap:Option[Map[String, String]]) = {
    logger.info("CREATING A RESOURCE ON CKAN ... ")
    this.createOrUpdateResource(MappingPediaConstant.CKAN_API_ACTION_RESOURCE_CREATE, distribution, textBodyMap);
  }

  def updateResource(distribution: Distribution, textBodyMap:Option[Map[String, String]]) = {
    logger.info("UPDATING A RESOURCE ON CKAN ... ")
    val textBodyMap2 = textBodyMap.get + ("id" -> distribution.ckanResourceId);
    this.createOrUpdateResource(MappingPediaConstant.CKAN_API_ACTION_RESOURCE_UPDATE, distribution, Some(textBodyMap2));
  }

  def createOrUpdateResource(ckanAction:String, distribution: Distribution, textBodyMap:Option[Map[String, String]]) = {
    //val dataset = distribution.dataset;

    //val packageId = distribution.dataset.dctIdentifier;
    val datasetPackageId = distribution.dataset.ckanPackageId;
    val packageId = if(datasetPackageId == null) { distribution.dataset.dctIdentifier } else { datasetPackageId }
    logger.info(s"packageId = $packageId")


    logger.info(s"distribution.dcatDownloadURL = ${distribution.dcatDownloadURL}")

    val httpClient = HttpClientBuilder.create.build
    try {


      val createOrUpdateUrl = this.ckanClient.ckanUrl + ckanAction
      logger.info(s"Hitting endpoint: $createOrUpdateUrl");

      val httpPostRequest = new HttpPost(createOrUpdateUrl)
      httpPostRequest.setHeader("Authorization", this.ckanClient.authorizationToken)
      val builder = MultipartEntityBuilder.create()
        .addTextBody(MappingPediaConstant.CKAN_FIELD_PACKAGE_ID, packageId)
        .addTextBody(MappingPediaConstant.CKAN_FIELD_URL, distribution.dcatDownloadURL)
      ;

      logger.info(s"distribution.dctTitle = ${distribution.dctTitle}")
      if(distribution.dctTitle != null) {
        builder.addTextBody(MappingPediaConstant.CKAN_FIELD_NAME, distribution.dctTitle)
      }

      logger.info(s"distribution.dctDescription = ${distribution.dctDescription}")
      if(distribution.dctDescription != null) {
        builder.addTextBody(MappingPediaConstant.CKAN_FIELD_DESCRIPTION, distribution.dctDescription)
      }

      logger.info(s"dataset.dcatMediaType = ${distribution.dcatMediaType}")
      if(distribution.dcatMediaType != null) {
        builder.addTextBody("mimetype", distribution.dcatMediaType)
      }

      logger.info(s"dataset.distributionFile = ${distribution.distributionFile}")
      if(distribution.distributionFile != null) {
        builder.addBinaryBody("upload", distribution.distributionFile)
      }

      if(distribution.dctLanguage != null) {
        builder.addTextBody("language", distribution.dctLanguage)
      }

      if(distribution.dctRights != null) {
        builder.addTextBody("rights", distribution.dctRights)
      }

      if(distribution.hash != null) {
        builder.addTextBody("hash", distribution.hash)
      }

      if(distribution.manifestDownloadURL != null) {
        builder.addTextBody(MappingPediaConstant.CKAN_RESOURCE_PROV_TRIPLES, distribution.manifestDownloadURL)
      }

      if(textBodyMap != null && textBodyMap.isDefined) {

        for((key, value) <- textBodyMap.get) {

          if(key != null && value != null) {
            builder.addTextBody(key, value)
          } else {
            logger.warn(s"textBodyMap key,value = ${key},${value}")
          }
        }
      }



      val mpEntity = builder.build();
      httpPostRequest.setEntity(mpEntity)
      val response = httpClient.execute(httpPostRequest)


      if (response.getStatusLine.getStatusCode < 200 || response.getStatusLine.getStatusCode >= 300) {
        logger.info(s"response = ${response}")
        logger.info(s"response.getEntity= ${response.getEntity}");
        logger.info(s"response.getEntity.getContent= ${response.getEntity.getContent}");
        logger.info(s"response.getEntity.getContentType= ${response.getEntity.getContentType}");
        logger.info(s"response.getProtocolVersion= ${response.getProtocolVersion}");
        logger.info(s"response.getStatusLine= ${response.getStatusLine}");
        logger.info(s"response.getStatusLine.getReasonPhrase= ${response.getStatusLine.getReasonPhrase}");

        throw new Exception("Failed to add the file to CKAN storage. Response status line from " + createOrUpdateUrl + " was: " + response.getStatusLine)
      }

      response
    } catch {
      case e: Exception => {
        e.printStackTrace()
        //HttpURLConnection.HTTP_INTERNAL_ERROR
        throw e;
      }

      // log error
    } finally {
      //if (httpClient != null) httpClient.close()
    }


  }

  def findDistributions(queryString: String)= {
    logger.info(s"queryString = $queryString");

    val qexec = this.virtuosoClient.createQueryExecution(queryString);

    var results: List[Distribution] = List.empty;
    try {
      val rs = qexec.execSelect
      while (rs.hasNext) {
        val qs = rs.nextSolution

        val datasetId = qs.get("datasetId").toString;
        val dataset = new Dataset(datasetId);

        val distributionId = qs.get("distributionId").toString;
        val distribution = new UnannotatedDistribution(dataset, distributionId);

        distribution.dcatDownloadURL = MappingPediaUtility.getStringOrElse(qs, "distributionDownloadURL", null)

        results = distribution :: results;
      }
    }
    catch {
      case e:Exception => {
        e.printStackTrace()
        logger.error(s"Error execution query: ${e.getMessage}")
      }
    }
    finally qexec.close

    val listResult = new ListResult[Distribution](results.length, results);
    listResult
  }

  def findByCKANResourceId(ckanResourceId:String) = {
    logger.info("findDistributionByCKANResourceId")
    val queryTemplateFile = "templates/findDistributionByCKANResourceId.rq";

    val mapValues: Map[String, String] = Map(
      "$graphURL" -> this.properties.getProperty(MappingPediaConstant.GRAPH_NAME)
      , "$ckanResourceId" -> ckanResourceId
    );

    val queryString: String = MpcUtility.generateStringFromTemplateFile(mapValues, queryTemplateFile)
    this.findDistributions(queryString);
  }

  def findDistributionsByDatasetId(datasetId:String) = {
    logger.info("findDistributionsByDatasetId")
    val queryTemplateFile = "templates/findDistributionsByDatasetId.rq";

    val mapValues: Map[String, String] = Map(
      "$graphURL" -> this.properties.getProperty(MappingPediaConstant.GRAPH_NAME)
      , "$datasetId" -> datasetId
    );

    val queryString: String = MpcUtility.generateStringFromTemplateFile(mapValues, queryTemplateFile)
    this.findDistributions(queryString);
  }

  def storeManifestFileOnGitHub(file:File, distribution:Distribution) : HttpResponse[JsonNode] = {
    val dataset = distribution.dataset;
    val organization = dataset.dctPublisher;


    logger.info(s"STORING MANIFEST FILE FOR DISTRIBUTION: ${distribution.dctIdentifier} - DATASET: ${dataset.dctIdentifier} ON GITHUB ...")
    val addNewManifestCommitMessage = s"Add distribution manifest file: ${distribution.dctIdentifier}"
    val manifestFileName = file.getName
    val datasetId = dataset.dctIdentifier;
    val organizationId = organization.dctIdentifier;

    val githubResponse = githubClient.encodeAndPutFile(organizationId
      , datasetId, manifestFileName, addNewManifestCommitMessage, file)
    logger.info("manifest file stored on github ...")
    githubResponse
  }

  def storeDatasetDistributionFileOnGitHub(distribution: Distribution) = {
    val dataset = distribution.dataset;
    val organization = dataset.dctPublisher;

    val (filename:String, fileContent:String) = MappingPediaUtility.getFileNameAndContent(
      distribution.distributionFile, distribution.dcatDownloadURL, distribution.encoding);
    val base64EncodedContent = GitHubUtility.encodeToBase64(fileContent)


    logger.info("STORING DISTRIBUTION FILE ON GITHUB ...")
    //val datasetFile = MappingPediaUtility.multipartFileToFile(distribution.ckanFileRef, dataset.dctIdentifier)
    val addNewDatasetCommitMessage = s"Add distribution file to dataset: ${dataset.dctIdentifier}"
    val githubResponse = githubClient.putEncodedContent(organization.dctIdentifier
      , dataset.dctIdentifier, filename, addNewDatasetCommitMessage, base64EncodedContent)

    if(githubResponse != null) {
      val responseStatus = githubResponse.getStatus;
      if (HttpURLConnection.HTTP_OK == responseStatus || HttpURLConnection.HTTP_CREATED == responseStatus) {
        logger.info("Distribution file stored on GitHub")
        if(distribution.dcatAccessURL == null) {
          distribution.dcatAccessURL =
            githubResponse.getBody.getObject.getJSONObject("content").getString("url")
          logger.info(s"distribution.dcatAccessURL = ${distribution.dcatAccessURL}")
        }
        if(distribution.dcatDownloadURL == null) {
          distribution.dcatDownloadURL =
            githubResponse.getBody.getObject.getJSONObject("content").getString("download_url")
          logger.info(s"distribution.dcatDownloadURL = ${distribution.dcatDownloadURL}")
        }
      } else {
        val errorMessage = "Error when storing dataset on GitHub: " + responseStatus
        throw new Exception(errorMessage);
      }
    }

    githubResponse;
  }

  def addModifiedDate(distribution: Distribution) = {
    val sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss")
    val now = sdf.format(new Date())
    distribution.dctModified = now;

    try {
      val mapValues: Map[String, String] = Map(
        "$graphURL" -> this.properties.getProperty(MappingPediaConstant.GRAPH_NAME)
        , "$distributionID" -> distribution.dctIdentifier
        , "$distributionModified" -> distribution.dctModified
      );

      val templateFiles = List("templates/metadata-namespaces-template.ttl"
        , "templates/addDistributionModifiedDate.ttl")
      val triplesString: String = MpcUtility.generateStringFromTemplateFiles(
        mapValues, templateFiles)
      logger.info(s"adding triples to virtuoso: ${triplesString}");

      if(triplesString != null) {
        this.virtuosoClient.storeFromString(triplesString);
      }
    } catch {
      case e:Exception => {
        e.printStackTrace()
      }
    }
  }

  def addDistribution(
                       distribution: Distribution
                       , pManifestFile:File
                       , generateManifestFile:Boolean
                       , storeToCKAN:Boolean
                     ) : AddDistributionResult = {

    //val dataset = distribution.dataset
    val organization: Agent = distribution.dataset.dctPublisher;
    var errorOccured = false;
    var collectiveErrorMessage:List[String] = Nil;


    //STORING DISTRIBUTION FILE ON GITHUB
    val addDistributionFileGitHubResponse:HttpResponse[JsonNode] = try {
      if(distribution != null &&
        (distribution.distributionFile != null || distribution.dcatDownloadURL != null)) {
        this.storeDatasetDistributionFileOnGitHub(distribution);
      } else {
        val statusMessage = "No distribution or distribution file has been provided"
        logger.info(statusMessage);
        null
      }
    } catch {
      case e: Exception => {
        errorOccured = true;
        e.printStackTrace()
        val errorMessage = "error storing dataset file on GitHub: " + e.getMessage
        logger.error(errorMessage)
        collectiveErrorMessage = errorMessage :: collectiveErrorMessage
        null
      }
    }


    val distributionAccessURL = if(addDistributionFileGitHubResponse == null) {
      null
    } else {
      this.githubClient.getAccessURL(addDistributionFileGitHubResponse)
    }
    val distributionDownloadURL = this.githubClient.getDownloadURL(distributionAccessURL);
    val addDistributionFileGitHubResponseStatus:Integer = if(addDistributionFileGitHubResponse == null) { null }
    else { addDistributionFileGitHubResponse.getStatus }
    /*
    if(addDistributionFileGitHubResponseStatus!= null && addDistributionFileGitHubResponseStatus >= 200
      && addDistributionFileGitHubResponseStatus < 300 && distributionDownloadURL != null) {
      distribution.hash = this.githubClient.getSHA(distributionAccessURL);
    }
    */
    distribution.hash = MappingPediaUtility.calculateHash(distributionDownloadURL, distribution.encoding);


    val addDatasetFileGitHubResponseStatusText = if(addDistributionFileGitHubResponse == null) { null }
    else { addDistributionFileGitHubResponse.getStatusText }


    //STORING DISTRIBUTION FILE AS RESOURCE ON CKAN
    val ckanAddResourceResponse = try {
      val ckanEnableValue = this.properties.getProperty(MappingPediaConstant.CKAN_ENABLE)
      val isCkanEnabled = MappingPediaUtility.stringToBoolean(ckanEnableValue);

      if(isCkanEnabled && storeToCKAN) {
        logger.info("STORING DISTRIBUTION FILE AS A RESOURCE ON CKAN...")

        if(distribution != null
          && (distribution.distributionFile != null || distribution.dcatDownloadURL != null)) {
          this.createResource(distribution, None);
        } else {
          null
        }
      } else {
        null
      }
    } catch {
      case e: Exception => {
        errorOccured = true;
        e.printStackTrace()
        val errorMessage = "error storing distribution file as a resource on CKAN: " + e.getMessage
        logger.error(errorMessage)
        collectiveErrorMessage = errorMessage :: collectiveErrorMessage
        null
      }
    }
    logger.info(s"ckanAddResourceResponse= ${ckanAddResourceResponse}");
    val ckanAddResourceResponseStatusCode:Integer = {
      if(ckanAddResourceResponse == null) { null }
      else { ckanAddResourceResponse.getStatusLine.getStatusCode }
    }

    if(ckanAddResourceResponseStatusCode != null && ckanAddResourceResponseStatusCode >= 200
      && ckanAddResourceResponseStatusCode <300) {
      try {
        val ckanAddResourceResult = MpcCkanUtility.getResult(ckanAddResourceResponse);
        val packageId = ckanAddResourceResult.getString("package_id")
        val resourceId = ckanAddResourceResult.getString("id")
        val resourceURL = ckanAddResourceResult.getString("url")

        distribution.ckanResourceId = resourceId;
        distribution.dcatAccessURL= s"${this.ckanClient.ckanUrl}/dataset/${packageId}/resource/${resourceId}";
        distribution.dcatDownloadURL = resourceURL;
      } catch { case e:Exception => { e.printStackTrace() } }
    }
    logger.info(s"distribution.ckanResourceId = ${distribution.ckanResourceId}");


    //MANIFEST FILE
    val manifestFile:File = try {
      if (pManifestFile != null) {//if the user provides a manifest file
        pManifestFile
      } else { // if the user does not provide any manifest file
        if(generateManifestFile) {
          //MANIFEST FILE GENERATION
          val generatedFile = DistributionController.generateManifestFile(distribution);
          generatedFile
        } else {
          null
        }
      }
    } catch {
      case e: Exception => {
        errorOccured = true;
        e.printStackTrace()
        val errorMessage = "error generating manifest file: " + e.getMessage
        logger.error(errorMessage)
        collectiveErrorMessage = errorMessage :: collectiveErrorMessage
        null
      }
    }


    //STORING MANIFEST ON GITHUB
    val addManifestFileGitHubResponse:HttpResponse[JsonNode] = try {
      this.storeManifestFileOnGitHub(manifestFile, distribution);
    } catch {
      case e: Exception => {
        errorOccured = true;
        e.printStackTrace()
        val errorMessage = "error storing manifest file on GitHub: " + e.getMessage
        logger.error(errorMessage)
        collectiveErrorMessage = errorMessage :: collectiveErrorMessage
        null
      }
    }
    distribution.manifestAccessURL = this.githubClient.getAccessURL(addManifestFileGitHubResponse)
    distribution.manifestDownloadURL = this.githubClient.getDownloadURL(distribution.manifestAccessURL);

    //STORING MANIFEST ON VIRTUOSO
    val addManifestVirtuosoResponse:String = try {
      val virtuosoEnabledValue = this.properties.getProperty(MappingPediaConstant.VIRTUOSO_ENABLED)
      val isVirtuosoEnabled = MappingPediaUtility.stringToBoolean(virtuosoEnabledValue);

      if(isVirtuosoEnabled) {
        if(manifestFile != null) {
          logger.info(s"STORING TRIPLES OF THE MANIFEST FILE FOR DISTRIBUTION ${distribution.dctIdentifier} ON VIRTUOSO...")
          this.virtuosoClient.storeFromFile(manifestFile)
          "OK"
        } else {
          "No manifest has been generated/provided";
        }
      } else {
        "Storing to Virtuoso is not enabled!";
      }
    } catch {
      case e: Exception => {
        errorOccured = true;
        e.printStackTrace()
        val errorMessage = "error storing manifest file on Virtuoso: " + e.getMessage
        logger.error(errorMessage);
        collectiveErrorMessage = errorMessage :: collectiveErrorMessage
        e.getMessage
      }
    }

    logger.debug(s"errorOccured = $errorOccured")
    logger.debug(s"collectiveErrorMessage = $collectiveErrorMessage")

    val (responseStatus, responseStatusText) = if(errorOccured) {
      (HttpURLConnection.HTTP_INTERNAL_ERROR, "Internal Error: " + collectiveErrorMessage.mkString("[", ",", "]"))
    } else {
      (HttpURLConnection.HTTP_OK, "OK")
    }





    val addManifestFileGitHubResponseStatus:Integer = if(addManifestFileGitHubResponse == null) {
      null
    } else {
      addManifestFileGitHubResponse.getStatus
    }

    val addManifestFileGitHubResponseStatusText = if(addManifestFileGitHubResponse == null) {
      null
    } else {
      addManifestFileGitHubResponse.getStatusText
    }

    /*
    val addDatasetResult:AddDatasetResult = new AddDatasetResult(
      responseStatus, responseStatusText

      , manifestAccessURL, manifestDownloadURL
      , addManifestFileGitHubResponseStatus
      , addManifestFileGitHubResponseStatusText

      , distributionAccessURL, distributionDownloadURL, distribution.sha
      , addDatasetFileGitHubResponseStatus
      , addDatasetFileGitHubResponseStatusText

      , addManifestVirtuosoResponse

      , null
      , ckanAddResourceResponseStatusCode
      , ckanResourceId

      , distribution.dataset.dctIdentifier
    )
    addDatasetResult
    */

    val addDistributionResult:AddDistributionResult = new AddDistributionResult(responseStatus, responseStatusText
      , distribution

      //, manifestAccessURL, manifestDownloadURL
      , addManifestFileGitHubResponseStatus, addManifestFileGitHubResponseStatusText

      //, distributionAccessURL, distributionDownloadURL, distribution.sha
      , addDistributionFileGitHubResponseStatus, addDatasetFileGitHubResponseStatusText

      , addManifestVirtuosoResponse

      , ckanAddResourceResponseStatusCode
    )

    try {
      val addDistributionResultAsJson = this.mapper.writeValueAsString(addDistributionResult);
      logger.info(s"addDistributionResultAsJson = ${addDistributionResultAsJson}\n\n");
    } catch {
      case e:Exception => {
        logger.error(s"addDistributionResult = ${addDistributionResult}")
      }
    }

    addDistributionResult


    /*
    val executionResult = new MappingPediaExecutionResult(manifestURL, datasetURL, null
      , null, null, responseStatusText, responseStatus, ckanResponseStatusText)
    executionResult;
    */

  }
}

object DistributionController {
  val logger: Logger = LoggerFactory.getLogger(this.getClass);

  def apply(): DistributionController = {
    val propertiesFilePath = "/" + MappingPediaConstant.DEFAULT_CONFIGURATION_FILENAME;
    val url = getClass.getResource(propertiesFilePath)
    logger.info(s"loading mappingpedia-engine-datasets configuration file from:\n ${url}")
    val properties = new Properties();
    if (url != null) {
      val source = Source.fromURL(url)
      val reader = source.bufferedReader();
      properties.load(reader)
      logger.debug(s"properties.keySet = ${properties.keySet()}")
    }

    DistributionController(properties)
  }

  def apply(properties: Properties): DistributionController = {
    val ckanUtility = new MpcCkanUtility(
      properties.getProperty(MappingPediaConstant.CKAN_URL)
      , properties.getProperty(MappingPediaConstant.CKAN_KEY)
    );

    val githubUtility = new MpcGithubUtility(
      properties.getProperty(MappingPediaConstant.GITHUB_REPOSITORY)
      , properties.getProperty(MappingPediaConstant.GITHUB_USER)
      , properties.getProperty(MappingPediaConstant.GITHUB_ACCESS_TOKEN)
    );

    val virtuosoUtility = new MpcVirtuosoUtility(
      properties.getProperty(MappingPediaConstant.VIRTUOSO_JDBC)
      , properties.getProperty(MappingPediaConstant.VIRTUOSO_USER)
      , properties.getProperty(MappingPediaConstant.VIRTUOSO_PWD)
      , properties.getProperty(MappingPediaConstant.GRAPH_NAME)
    );

    new DistributionController(ckanUtility, githubUtility, virtuosoUtility, properties);

  }

  def generateManifestFile(distribution: Distribution) = {
    logger.info("GENERATING MANIFEST FOR DISTRIBUTION ...")
    val dataset = distribution.dataset;

    try {
      val organization = dataset.dctPublisher;

      val templateFiles = List(
        "templates/metadata-namespaces-template.ttl"
        , "templates/metadata-distributions-template.ttl"
      );


      val mapValuesWithDistribution:Map[String,String] = {
        val distributionAccessURL = if(distribution.dcatAccessURL == null) { ""; }
        else { distribution.dcatAccessURL }

        logger.info(s"distributionAccessURL = ${distributionAccessURL}")

        val distributionDownloadURL = if(distribution.dcatDownloadURL == null) { ""; }
        else { distribution.dcatDownloadURL }

        logger.info(s"distributionDownloadURL = ${distributionDownloadURL}")

        Map(
          "$datasetID" -> distribution.dataset.dctIdentifier
          , "$distributionTitle" -> distribution.dctTitle
          , "$distributionDescription" -> distribution.dctDescription
          , "$distributionAccessURL" -> distributionAccessURL
          , "$distributionDownloadURL" -> distributionDownloadURL
          , "$distributionMediaType" -> distribution.dcatMediaType
          , "$distributionID" -> distribution.dctIdentifier
          , "$distributionIssued" -> distribution.dctIssued
          , "$distributionModified" -> distribution.dctModified
          , "$ckanResourceID" -> distribution.ckanResourceId
          , "$hash" -> distribution.hash
          , "$distributionLanguage" -> distribution.dctLanguage
          , "$distributionLicense" -> distribution.dctLicense
          , "$distributionRights" -> distribution.dctRights
        )
      }

      val filename = s"metadata-distribution-${distribution.dctIdentifier}.ttl"

      val manifestFile = MpcUtility.generateManifestFile(
        mapValuesWithDistribution, templateFiles, filename, dataset.dctIdentifier);
      logger.info("Manifest file generated.")
      manifestFile;
    } catch {
      case e:Exception => {
        e.printStackTrace()
        val errorMessage = "Error occured when generating manifest file: " + e.getMessage
        null;
      }
    }
  }


}
