/**
 * README
 * This extension is used by Mashup
 *
 * Name : EXT045
 * Description : allows to plan and submit the schedule job EXT040 for all customers
 * Date         Changed By   Description
 * 20231215     RENARN       COMX02 - Cadencier
 * 20240806     FLEBARS     Evolution 20, 52, 56
 */

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

public class EXT945 extends ExtendM3Batch {
  private final LoggerAPI logger
  private final DatabaseAPI database
  private final ProgramAPI program
  private final BatchAPI batch
  private final MICallerAPI miCaller
  private final TextFilesAPI textFiles
  private final UtilityAPI utility
  private Integer currentCompany
  private String rawData
  private int rawDataLength
  private int beginIndex
  private int endIndex
  private String logFileName
  private boolean in60
  private String jobNumber
  private Integer currentDate
  private String newCDNN
  private String customer
  private String assortment
  private String globalOffer
  private String calendar
  private String allContacts
  private String schedule
  private String listAlreadySentCustomers
  private Integer counter

  public EXT945(LoggerAPI logger, DatabaseAPI database, ProgramAPI program, BatchAPI batch, MICallerAPI miCaller, TextFilesAPI textFiles, UtilityAPI utility) {
    this.logger = logger
    this.database = database
    this.program = program
    this.batch = batch
    this.miCaller = miCaller
    this.textFiles = textFiles
    this.utility = utility
  }

  public void main() {
    // Get job number
    LocalDateTime timeOfCreation = LocalDateTime.now()
    jobNumber = program.getJobNumber() + timeOfCreation.format(DateTimeFormatter.ofPattern("yyMMdd")) + timeOfCreation.format(DateTimeFormatter.ofPattern("HHmmss"))

    logger.debug("Début" + program.getProgramName())
    //logger.debug("referenceId = " + batch.getReferenceId().get())
    if (batch.getReferenceId().isPresent()) {
      Optional<String> data = getJobData(batch.getReferenceId().get())
      logger.debug("data = " + data)
      performActualJob(data)
    } else {
      // No job data found
      logger.debug("Job data for job ${batch.getJobId()} is missing")
    }
  }
  // Get job data
  private Optional<String> getJobData(String referenceId) {
    def query = database.table("EXTJOB").index("00").selection("EXDATA").build()
    def container = query.createContainer()
    container.set("EXRFID", referenceId)
    if (query.read(container)) {
      logger.debug("EXDATA = " + container.getString("EXDATA"))
      return Optional.of(container.getString("EXDATA"))
    } else {
      logger.debug("EXTJOB not found")
    }
    return Optional.empty()
  }
  // Perform actual job
  private performActualJob(Optional<String> data) {
    if (!data.isPresent()) {
      logger.debug("Job reference Id ${batch.getReferenceId().get()} is passed but data was not found")
      return
    }
    rawData = data.get()
    logger.debug("Début performActualJob")

    currentCompany = (Integer) program.getLDAZD().CONO

    LocalDateTime timeOfCreation = LocalDateTime.now()
    currentDate = timeOfCreation.format(DateTimeFormatter.ofPattern("yyyyMMdd")) as Integer

    // Perform Job
    listAlreadySentCustomers = ""
    counter = 0

    ExpressionFactory ocusmaExpression = database.getExpressionFactory("OCUSMA")
    ocusmaExpression = ocusmaExpression.eq("OKSTAT", "20")
    DBAction ocusmaQuery = database.table("OCUSMA").index("01").matching(ocusmaExpression).selection("OKCUNO").build()
    DBContainer ocusmaRequest = ocusmaQuery.getContainer()
    ocusmaRequest.set("OKCONO", currentCompany)
    ocusmaRequest.set("OKCUTP", 0)
    //no limit for readall we have to compute all normal customers cutp = 0
    if (!ocusmaQuery.readAll(ocusmaRequest, 2, ocusmaReader)) {
    }
  }

  Closure<?> ocusmaReader = { DBContainer ocusmaResult ->
    String cuno = ocusmaResult.getString("OKCUNO").trim()
    String[] tbcuno = ["90150", "90153", "90154", "90156", "90158", "97041"]// todo test remove

    if (cuno in tbcuno) {// todo test remove
      String tAssortment = cuno.trim() + "0"
      DBAction cugex1Query = database.table("CUGEX1").index("00").selection("F1A230").build()
      DBContainer cugex1Request = cugex1Query.getContainer()
      cugex1Request.set("F1CONO", currentCompany)
      cugex1Request.set("F1FILE", "OCUSMA")
      cugex1Request.set("F1PK01", ocusmaResult.get("OKCUNO") as String)
      cugex1Request.set("F1PK02", "")
      cugex1Request.set("F1PK03", "")
      cugex1Request.set("F1PK04", "")
      cugex1Request.set("F1PK05", "")
      cugex1Request.set("F1PK06", "")
      cugex1Request.set("F1PK07", "")
      cugex1Request.set("F1PK08", "")
      if (cugex1Query.read(cugex1Request)) {
        String ta230 = cugex1Request.get("F1A230") as String
        if (ta230.trim().length() > 0 && ta230.indexOf(cuno) > -1) {
          tAssortment = ta230.trim()
        }
      }

      newCDNN = ""
      executeEXT040MIRtvNextCalendar(currentCompany as String, cuno)
      calendar = newCDNN

      LocalDateTime timeOfCreation = LocalDateTime.now()
      DBAction query = database.table("EXT042").index("00").build()
      DBContainer EXT042 = query.getContainer()
      EXT042.set("EXCONO", currentCompany)
      EXT042.set("EXCUNO", cuno)
      EXT042.set("EXCDNN", calendar)
      EXT042.set("EXASCD", tAssortment)
      if (!query.read(EXT042)) {
        EXT042.setInt("EXRGDT", timeOfCreation.format(DateTimeFormatter.ofPattern("yyyyMMdd")) as Integer)
        EXT042.setInt("EXRGTM", timeOfCreation.format(DateTimeFormatter.ofPattern("HHmmss")) as Integer)
        EXT042.setInt("EXLMDT", timeOfCreation.format(DateTimeFormatter.ofPattern("yyyyMMdd")) as Integer)
        EXT042.setInt("EXCHNO", 1)
        EXT042.set("EXCHID", program.getUser())
        query.insert(EXT042)
      }

      allContacts = "1"
      schedule = "1"

      logger.debug("assortment = " + tAssortment)
      logger.debug("executeEXT820MISubmitBatch :")
      logger.debug("customer = " + customer)
      logger.debug("calendar = " + calendar)
      logger.debug("globalOffer = " + globalOffer)
      logger.debug("allContacts = " + allContacts)
      logger.debug("schedule = " + schedule)

      //if(formatTXT.trim() != "" || formatCSV.trim() != "" || formatXLSX.trim() != ""){
      //executeEXT820MISubmitBatch(currentCompany as String, "EXT040", customer, calendar, globalOffer, formatCSV, formatTXT, formatXLSX, allContacts, schedule, "")
      executeEXT820MISubmitBatch(currentCompany as String, "EXT040", cuno, calendar, allContacts, schedule, "", "", "", "", "")
    }// todo test remove

  }

  /**
   * Call EXT040MI.RtvNextCalendar
   * @param CONO
   * @param CUNO
   * @return
   */
  private executeEXT040MIRtvNextCalendar(String CONO, String CUNO) {
    def parameters = ["CONO": CONO, "CUNO": CUNO]
    Closure<?> handler = { Map<String, String> response ->
      if (response.error != null) {
      } else {
        newCDNN = response.CDNN.trim()
      }
    }
    miCaller.call("EXT040MI", "RtvNextCalendar", parameters, handler)
  }
  // Execute EXT820MI.SubmitBatch
  //private executeEXT820MISubmitBatch(String CONO, String JOID, String P001, String P002, String P003, String P004, String P005, String P006, String P007, String P008, String P009){
  private executeEXT820MISubmitBatch(String CONO, String JOID, String P001, String P002, String P003, String P004, String P005, String P006, String P007, String P008, String P009) {
    def parameters = ["CONO": CONO, "JOID": JOID, "P001": P001, "P002": P002, "P003": P003, "P004": P004, "P005": P005, "P006": P006, "P007": P007, "P008": P008, "P009": P009]
    Closure<?> handler = { Map<String, String> response ->
      if (response.error != null) {
      } else {
      }
    }
    miCaller.call("EXT820MI", "SubmitBatch", parameters, handler)
  }
  // Get first parameter
  private String getFirstParameter() {
    logger.debug("rawData = " + rawData)
    rawDataLength = rawData.length()
    beginIndex = 0
    endIndex = rawData.indexOf(";")
    // Get parameter
    String parameter = rawData.substring(beginIndex, endIndex)
    logger.debug("parameter = " + parameter)
    return parameter
  }
  // Get next parameter
  private String getNextParameter() {
    beginIndex = endIndex + 1
    endIndex = rawDataLength - rawData.indexOf(";") - 1
    rawData = rawData.substring(beginIndex, rawDataLength)
    rawDataLength = rawData.length()
    beginIndex = 0
    endIndex = rawData.indexOf(";")
    // Get parameter
    String parameter = rawData.substring(beginIndex, endIndex)
    logger.debug("parameter = " + parameter)
    return parameter
  }
  // Delete records related to the current job from EXTJOB table
  public void deleteEXTJOB() {
    LocalDateTime timeOfCreation = LocalDateTime.now()
    DBAction query = database.table("EXTJOB").index("00").build()
    DBContainer EXTJOB = query.getContainer()
    EXTJOB.set("EXRFID", batch.getReferenceId().get())
    if (!query.readAllLock(EXTJOB, 1, updateCallBack_EXTJOB)) {
    }
  }
  // Delete EXTJOB
  Closure<?> updateCallBack_EXTJOB = { LockedResult lockedResult ->
    lockedResult.delete()
  }
  // Log message
  void logMessage(String header, String message) {
    textFiles.open("FileImport")
    logFileName = "MSG_" + program.getProgramName() + "." + "batch" + "." + jobNumber + ".csv"
    if (!textFiles.exists(logFileName)) {
      log(header)
      log(message)
    }
  }
  // Log
  void log(String message) {
    in60 = true
    //logger.debug(message)
    message = LocalDateTime.now().toString() + ";" + message
    Closure<?> consumer = { PrintWriter printWriter ->
      printWriter.println(message)
    }
    textFiles.write(logFileName, "UTF-8", true, consumer)
  }
}
