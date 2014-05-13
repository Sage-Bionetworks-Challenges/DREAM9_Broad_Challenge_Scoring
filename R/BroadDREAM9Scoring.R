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
PAGE_SIZE <- 100
# batch size for uploading submission status updates
BATCH_SIZE <- 500

evaluationId1<-"2468319"
evaluationId2<-"2468322"

readPredictionFile<-function(id) {
  synEntity <- synGet(id, downloadFile = TRUE)
  filePath <- getFileLocation(synEntity)
  content <- read.table(filePath, header = TRUE, sep = "\t", skip = 2, stringsAsFactors = FALSE)
  gene_names <- as.character(content[,1])
  cell_line_names <- colnames(content)[-2:-1]
  content_data <- t(content[,-2:-1])
  colnames(content_data) <- gene_names
  rownames(content_data) <- cell_line_names
  return (content_data)
}

validate1<-function(evaluation) {
  total<-1e+10
  offset<-0
  statusesToUpdate<-list()
  
  measured_data <- readPredictionFile(id = "syn2468461")

  while(offset<total) {
    submissionBundles<-synRestGET(sprintf("/evaluation/%s/submission/bundle/all?limit=%s&offset=%s&status=%s", evaluation$id, PAGE_SIZE, offset, "RECEIVED")) 
    total<-submissionBundles$totalNumberOfResults
    offset<-offset+PAGE_SIZE
    page<-submissionBundles$results
    if (length(page)>0) {
      for (i in 1:length(page)) {
        submission<-synGetSubmission(page[[i]]$submission$id)
        checkSubmission <- try(
{
  predicted_data<-readPredictionFile(id = submission$entityId)
  predicted_data <- predicted_data[rownames(measured_data), colnames(measured_data)]
}, silent = TRUE)
        isValid<-!inherits(checkSubmission, "try-error")
        subStatus<-page[[i]]$submissionStatus
        if (isValid) {
          newStatus<-"VALIDATED"
        } else {
          newStatus<-"INVALID"
          sendMessage(list(submission$userId), "Submission Acknowledgment", paste0("Your submission with synapse id ", submission$entityId," is invalid. Please try again."))
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
      for (i in 1:length(page)) {
        # need to download the file
        submission<-synGetSubmission(page[[i]]$submission$id)
        filePath<-getFileLocation(submission)
        # challenge-specific validation of the downloaded file goes here
        isValid<-TRUE
        if (isValid) {
          newStatus<-"VALIDATED"
        } else {
          newStatus<-"INVALID"
          sendMessage(list(), "Submission Acknowledgment", "Your submission is invalid. Please try again.")
        }
        subStatus<-page[[i]]$submissionStatus
        subStatus$status<-newStatus
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
  
  measured_data <- readPredictionFile(id = "syn2468461")
  
  while(offset<total) {
    submissionBundles<-synRestGET(sprintf("/evaluation/%s/submission/bundle/all?limit=%s&offset=%s&status=%s", evaluation$id, PAGE_SIZE, offset, submissionStateToFilter)) 
    total<-submissionBundles$totalNumberOfResults
    offset<-offset+PAGE_SIZE
    page<-submissionBundles$results
    if (length(page)>0) {
      for (i in 1:length(page)) {
        submission<-synGetSubmission(page[[i]]$submission$id)
        predicted_data<-readPredictionFile(id = submission$entityId)
        predicted_data <- predicted_data[rownames(measured_data), colnames(measured_data)]
        gene_count <- ncol(measured_data)
        correlation_per_gene <- matrix(0, 1, gene_count)
        for (gene_index in 1:gene_count) {
          correlation_per_gene[gene_index] <- cor(measured_data[,gene_index], predicted_data[,gene_index], method = 'spearman')
        }
        score <- mean(correlation_per_gene)
        subStatus<-page[[i]]$submissionStatus
        subStatus$status<-"SCORED"
        subStatus$annotations<-generateAnnotations(submission, score)
        statusesToUpdate[[length(statusesToUpdate)+1]]<-subStatus
      }
    }
  }
  updateSubmissionStatusBatch(evaluation, statusesToUpdate)
  message(sprintf("Retrieved and scored %s submissions.", length(statusesToUpdate)))
}

score2<-function(evaluation, submissionStateToFilter) {
  total<-1e+10
  offset<-0
  statusesToUpdate<-list()
  while(offset<total) {
    if (FALSE) {
      # get ALL the submissions in the Evaluation
      submissionBundles<-synRestGET(sprintf("/evaluation/%s/submission/bundle/all?limit=%s&offset=%s", evaluation$id, PAGE_SIZE, offset)) 
    } else {
      # alternatively just get the unscored submissions in the Evaluation
      # here we get the ones that the 'validation' step (above) marked as validated
      submissionBundles<-synRestGET(sprintf("/evaluation/%s/submission/bundle/all?limit=%s&offset=%s&status=%s", evaluation$id, PAGE_SIZE, offset, submissionStateToFilter)) 
    }
    total<-submissionBundles$totalNumberOfResults
    offset<-offset+PAGE_SIZE
    page<-submissionBundles$results
    if (length(page)>0) {
      for (i in 1:length(page)) {
        # download the file
        submission<-synGetSubmission(page[[i]]$submission$id)
        filePath<-getFileLocation(submission)
        score <- runif(1)
        
        subStatus<-page[[i]]$submissionStatus
        subStatus$status<-"SCORED"
        # add the score and any other information as submission annotations:
        subStatus$annotations<-generateAnnotations(submission, score)
        statusesToUpdate[[length(statusesToUpdate)+1]]<-subStatus
      }
    }
  }
  updateSubmissionStatusBatch(evaluation, statusesToUpdate)
  message(sprintf("Retrieved and scored %s submissions.", length(statusesToUpdate)))
}

generateAnnotations<-function(submission, score) {
  list(
    stringAnnos=list(
      list(key="SubmissionName", value=submission$name, isPrivate=FALSE),
      list(key="Team", value=submission$submitterAlias, isPrivate=FALSE),
      list(key="userId", value=submission$userId, isPrivate=FALSE),
      list(key="createdOn", value=submission$createdOn, isPrivate=FALSE)
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
  
  #evaluation2<-synGetEvaluation(evaluationId2)
  #validate2(evaluation2)
  #score2(evaluation2, "VALIDATED")
}

scoringApplication()