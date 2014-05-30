#
#
# This is the code for scoring submissions to the DREAM 9 Broad Challenge.
#
# Author: brucehoff and mehmetgonen
#
###############################################################################

library(RJSONIO)
library(synapseClient)

# page size for retrieving submissions
PAGE_SIZE<-100
# batch size for uploading submission status updates
BATCH_SIZE<-500

evaluationId1<-"2468319"
evaluationId2<-"2468322"
evaluationId3<-"2482339"

readMeasuredFile<-function(id) {
  synEntity<-synGet(id)
  filePath<-getFileLocation(synEntity)
  measuredData<-parsePredictionFile(filePath)
  return (measuredData)
}

parsePredictionFile<-function(filePath) {  
  fileContent<-read.table(filePath, header=TRUE, sep="\t", skip=2, stringsAsFactors=FALSE)
  geneNames<-as.character(fileContent[,1])
  cellLineNames<-colnames(fileContent)[-2:-1]
  predictionData<-t(fileContent[,-2:-1])
  colnames(predictionData)<-geneNames
  rownames(predictionData)<-cellLineNames
  return (predictionData)
}

readPrioritizedGeneList<-function() {
  synEntity<-synGet("syn2482403")
  filePath<-getFileLocation(synEntity)
  fileContent<-read.table(filePath, header=FALSE, sep="\t", stringsAsFactors=FALSE)
  geneList<-as.character(fileContent[,1])  
  return (geneList)
}

readCopyNumberFeatureList<-function() {
  synEntity<-synGet("syn2482674")
  filePath<-getFileLocation(synEntity)
  fileContent<-read.table(filePath, header=FALSE, sep="\t", stringsAsFactors=FALSE)
  featureList<-as.character(fileContent[,1])  
  return (featureList)
}

readGeneExpressionFeatureList<-function() {
  synEntity<-synGet("syn2482675")
  filePath<-getFileLocation(synEntity)
  fileContent<-read.table(filePath, header=FALSE, sep="\t", stringsAsFactors=FALSE)
  featureList<-as.character(fileContent[,1])  
  return (featureList)
}

calculateScore<-function(measuredData, predictedData) {
  geneCount<-ncol(measuredData)
  correlationPerGene<-matrix(0, 1, geneCount)
  for (g in 1:geneCount) {
    correlationPerGene[g]<-cor(measuredData[,g], predictedData[,g], method='spearman')
  }
  score<-mean(correlationPerGene)
  return (score)
}

validate1<-function(evaluation) {
  total<-1e+10
  offset<-0
  statusesToUpdate<-list()

  while(offset<total) {
    submissionBundles<-synRestGET(sprintf("/evaluation/%s/submission/bundle/all?limit=%s&offset=%s&status=%s", evaluation$id, PAGE_SIZE, offset, "RECEIVED")) 
    total<-submissionBundles$totalNumberOfResults
    offset<-offset+PAGE_SIZE
    page<-submissionBundles$results
    if (length(page)>0) {
      measuredData<-readMeasuredFile("syn2468461")
      for (i in 1:length(page)) {
        submission<-synGetSubmission(page[[i]]$submission$id)
        filePath<-getFileLocation(submission)
        directoryPath<-dirname(filePath)
        checkSubmission<-try(
{
  stopifnot(length(list.files(directoryPath, pattern='\\.gct$')) == 1)
  predictedData<-parsePredictionFile(filePath)
  stopifnot(setequal(rownames(measuredData), rownames(predictedData)))
  stopifnot(setequal(colnames(measuredData), colnames(predictedData)))
  predictedData<-predictedData[rownames(measuredData), colnames(measuredData)]
}, silent=TRUE)
        isValid<-!inherits(checkSubmission, "try-error")
        subStatus<-page[[i]]$submissionStatus
        if (isValid) {
          newStatus<-"VALIDATED"
        } else {
          newStatus<-"INVALID"
          sendMessage(list(submission$userId), paste0("Invalid submission for ", evaluation$name), paste0("Your submission for ", evaluation$name, " with synapse id ", submission$entityId, " is invalid. Please check the submission format at https://www.synapse.org/#!Synapse:syn2384331/wiki/64275 and try again."))
        }        
        subStatus$status<-newStatus
        subStatus$annotations<-generateAnnotations(submission, NA)
        statusesToUpdate[[length(statusesToUpdate)+1]]<-subStatus
      }
    }
  }
  updateSubmissionStatusBatch(evaluation, statusesToUpdate)
}

validate2<-function(evaluation) {
  total<-1e+10
  offset<-0
  statusesToUpdate<-list()

  while(offset<total) {
    submissionBundles<-synRestGET(sprintf("/evaluation/%s/submission/bundle/all?limit=%s&offset=%s&status=%s", evaluation$id, PAGE_SIZE, offset, "RECEIVED")) 
    total<-submissionBundles$totalNumberOfResults
    offset<-offset+PAGE_SIZE
    page<-submissionBundles$results
    if (length(page)>0) {
      measuredData<-readMeasuredFile("syn2468461")
      geneList<-readPrioritizedGeneList()
      measuredData<-measuredData[,geneList]
      for (i in 1:length(page)) {
        submission<-synGetSubmission(page[[i]]$submission$id)
        filePath<-getFileLocation(submission)
        directoryPath<-dirname(filePath)
        checkSubmission<-try(
{
  stopifnot(length(list.files(directoryPath, pattern='\\.zip$')) == 1)
  extractPath<-paste0(directoryPath, '/content')
  unzip(filePath, junkpaths=T, exdir=extractPath)
  stopifnot(length(list.files(extractPath, pattern='\\.gct$')) == 1)
  predictedPath<-list.files(extractPath, pattern='\\.gct$', full.names=TRUE)[1]
  predictedData<-parsePredictionFile(predictedPath)
  stopifnot(setequal(rownames(measuredData), rownames(predictedData)))
  stopifnot(setequal(colnames(measuredData), colnames(predictedData)))
  predictedData<-predictedData[rownames(measuredData), colnames(measuredData)]
}, silent=TRUE)
isValid<-!inherits(checkSubmission, "try-error")
subStatus<-page[[i]]$submissionStatus
if (isValid) {
  newStatus<-"VALIDATED"
} else {
  newStatus<-"INVALID"
  sendMessage(list(submission$userId), paste0("Invalid submission for ", evaluation$name), paste0("Your submission for ", evaluation$name, " with synapse id ", submission$entityId, " is invalid. Please check the submission format at https://www.synapse.org/#!Synapse:syn2384331/wiki/64275 and try again."))
}        
subStatus$status<-newStatus
subStatus$annotations<-generateAnnotations(submission, NA)
statusesToUpdate[[length(statusesToUpdate)+1]]<-subStatus
      }
    }
  }
updateSubmissionStatusBatch(evaluation, statusesToUpdate)
}

validate3<-function(evaluation) {
  total<-1e+10
  offset<-0
  statusesToUpdate<-list()
  
  while(offset<total) {
    submissionBundles<-synRestGET(sprintf("/evaluation/%s/submission/bundle/all?limit=%s&offset=%s&status=%s", evaluation$id, PAGE_SIZE, offset, "RECEIVED")) 
    total<-submissionBundles$totalNumberOfResults
    offset<-offset+PAGE_SIZE
    page<-submissionBundles$results
    if (length(page)>0) {
      measuredData<-readMeasuredFile("syn2468461")
      geneList<-readPrioritizedGeneList()
      measuredData<-measuredData[,geneList]
      for (i in 1:length(page)) {
        submission<-synGetSubmission(page[[i]]$submission$id)
        filePath<-getFileLocation(submission)
        directoryPath<-dirname(filePath)
        checkSubmission<-try(
{
  stopifnot(length(list.files(directoryPath, pattern='\\.zip$')) == 1)
  extractPath<-paste0(directoryPath, '/content')
  unzip(filePath, junkpaths=T, exdir=extractPath)
  stopifnot(length(list.files(extractPath, pattern='\\.gct$')) == 1)
  predictedPath<-list.files(extractPath, pattern='\\.gct$', full.names=TRUE)[1]
  predictedData<-parsePredictionFile(predictedPath)
  stopifnot(setequal(rownames(measuredData), rownames(predictedData)))
  stopifnot(setequal(colnames(measuredData), colnames(predictedData)))
  predictedData<-predictedData[rownames(measuredData), colnames(measuredData)]
}, silent=TRUE)
isValid<-!inherits(checkSubmission, "try-error")
subStatus<-page[[i]]$submissionStatus
if (isValid) {
  newStatus<-"VALIDATED"
} else {
  newStatus<-"INVALID"
  sendMessage(list(submission$userId), paste0("Invalid submission for ", evaluation$name), paste0("Your submission for ", evaluation$name, " with synapse id ", submission$entityId, " is invalid. Please check the submission format at https://www.synapse.org/#!Synapse:syn2384331/wiki/64275 and try again."))
}        
subStatus$status<-newStatus
subStatus$annotations<-generateAnnotations(submission, NA)
statusesToUpdate[[length(statusesToUpdate)+1]]<-subStatus
      }
    }
  }
updateSubmissionStatusBatch(evaluation, statusesToUpdate)
}

BATCH_UPLOAD_RETRY_COUNT<-3

updateSubmissionStatusBatch<-function(evaluation, statusesToUpdate) {
  for (retry in 1:BATCH_UPLOAD_RETRY_COUNT) {
    tryCatch(
{
  batchToken<-NULL
  offset<-0
  while (offset<length(statusesToUpdate)) {
    batch<-statusesToUpdate[(offset+1):min(offset+BATCH_SIZE, length(statusesToUpdate))]
    updateBatch<-list(
      statuses=batch, 
      isFirstBatch=(offset==0), 
      isLastBatch=(offset+BATCH_SIZE>=length(statusesToUpdate)),
      batchToken=batchToken
    )
    response<-synRestPUT(sprintf("/evaluation/%s/statusBatch",evaluation$id), updateBatch)
    batchToken<-response$nextUploadToken
    offset<-offset+BATCH_SIZE
  } # end while offset loop
  break
}, 
error=function(e){
  # on 412 ConflictingUpdateException we want to retry
  if (regexpr("412", e, fixed=TRUE)>0) {
    # will retry
  } else {
    stop(e)
  }
}
    )
if (retry<BATCH_UPLOAD_RETRY_COUNT) message("Encountered 412 error, will retry batch upload.")
  }
}

score1<-function(evaluation, submissionStateToFilter) {
  total<-1e+10
  offset<-0
  statusesToUpdate<-list()

  while(offset<total) {
    submissionBundles<-synRestGET(sprintf("/evaluation/%s/submission/bundle/all?limit=%s&offset=%s&status=%s", evaluation$id, PAGE_SIZE, offset, submissionStateToFilter)) 
    total<-submissionBundles$totalNumberOfResults
    offset<-offset+PAGE_SIZE
    page<-submissionBundles$results
    if (length(page)>0) {
      measuredData<-readMeasuredFile("syn2468461")
      for (i in 1:length(page)) {
        submission<-synGetSubmission(page[[i]]$submission$id)
        filePath<-getFileLocation(submission)
        predictedData<-parsePredictionFile(filePath)
        predictedData<-predictedData[rownames(measuredData), colnames(measuredData)]
        score<-calculateScore(measuredData, predictedData)
        subStatus<-page[[i]]$submissionStatus
        subStatus$status<-"SCORED"
        subStatus$annotations<-generateAnnotations(submission, score)
        statusesToUpdate[[length(statusesToUpdate)+1]]<-subStatus
      }
    }
  }
  updateSubmissionStatusBatch(evaluation, statusesToUpdate)
  message(sprintf("Retrieved and scored %s submission(s) for %s.", length(statusesToUpdate), evaluation$name))
}

score2<-function(evaluation, submissionStateToFilter) {
  total<-1e+10
  offset<-0
  statusesToUpdate<-list()
  
  while(offset<total) {
    submissionBundles<-synRestGET(sprintf("/evaluation/%s/submission/bundle/all?limit=%s&offset=%s&status=%s", evaluation$id, PAGE_SIZE, offset, submissionStateToFilter)) 
    total<-submissionBundles$totalNumberOfResults
    offset<-offset+PAGE_SIZE
    page<-submissionBundles$results
    if (length(page)>0) {
      measuredData<-readMeasuredFile("syn2468461")  
      geneList<-readPrioritizedGeneList()
      measuredData<-measuredData[,geneList]
      for (i in 1:length(page)) {
        submission<-synGetSubmission(page[[i]]$submission$id)
        filePath<-getFileLocation(submission)
        directoryPath<-paste0(dirname(filePath), '/content')
        predictedPath<-list.files(directoryPath, pattern='\\.gct$', full.names=TRUE)[1]
        predictedData<-parsePredictionFile(predictedPath)
        predictedData<-predictedData[rownames(measuredData), colnames(measuredData)]
        score<-calculateScore(measuredData, predictedData)
        subStatus<-page[[i]]$submissionStatus
        subStatus$status<-"SCORED"
        subStatus$annotations<-generateAnnotations(submission, score)
        statusesToUpdate[[length(statusesToUpdate)+1]]<-subStatus
      }
    }
  }
  updateSubmissionStatusBatch(evaluation, statusesToUpdate)
  message(sprintf("Retrieved and scored %s submission(s) for %s.", length(statusesToUpdate), evaluation$name))
}

score3<-function(evaluation, submissionStateToFilter) {
  total<-1e+10
  offset<-0
  statusesToUpdate<-list()
  
  while(offset<total) {
    submissionBundles<-synRestGET(sprintf("/evaluation/%s/submission/bundle/all?limit=%s&offset=%s&status=%s", evaluation$id, PAGE_SIZE, offset, submissionStateToFilter)) 
    total<-submissionBundles$totalNumberOfResults
    offset<-offset+PAGE_SIZE
    page<-submissionBundles$results
    if (length(page)>0) {
      measuredData<-readMeasuredFile("syn2468461")  
      geneList<-readPrioritizedGeneList()
      measuredData<-measuredData[,geneList]
      
      for (i in 1:length(page)) {
        submission<-synGetSubmission(page[[i]]$submission$id)
        filePath<-getFileLocation(submission)
        directoryPath<-paste0(dirname(filePath), '/content')
        predictedPath<-list.files(directoryPath, pattern='\\.gct$', full.names=TRUE)[1]
        predictedData<-parsePredictionFile(predictedPath)
        predictedData<-predictedData[rownames(measuredData), colnames(measuredData)]
        score<-calculateScore(measuredData, predictedData)
        subStatus<-page[[i]]$submissionStatus
        subStatus$status<-"SCORED"
        subStatus$annotations<-generateAnnotations(submission, score)
        statusesToUpdate[[length(statusesToUpdate)+1]]<-subStatus
      }
    }
  }
  updateSubmissionStatusBatch(evaluation, statusesToUpdate)
  message(sprintf("Retrieved and scored %s submission(s) for %s.", length(statusesToUpdate), evaluation$name))
}

generateAnnotations<-function(submission, score) {
  list(
    stringAnnos=list(
      list(key="SubmissionName", value=submission$name, isPrivate=FALSE),
      list(key="Team", value=submission$submitterAlias, isPrivate=FALSE),
      list(key="userIdPublic", value=submission$userId, isPrivate=FALSE),
      list(key="createdOnPublic", value=submission$createdOn, isPrivate=FALSE)
    ),
    doubleAnnos=list(
      list(key="score", value=score, isPrivate=FALSE)
    )
  )
}

scoringApplication<-function() {
  synapseLogin()
  
  evaluation1<-synGetEvaluation(evaluationId1)  
  validate1(evaluation1)
  score1(evaluation1, "VALIDATED")
  
  evaluation2<-synGetEvaluation(evaluationId2)
  validate2(evaluation2)
  score2(evaluation2, "VALIDATED")
  
  evaluation3<-synGetEvaluation(evaluationId3)
  validate3(evaluation3)
  score3(evaluation3, "VALIDATED")
}

scoringApplication()
