/****************************************************************************************
 Extension Name: EXT045
 Type: ExtendM3Transaction
 Script Author: RENARN
 Date: 2023-12-15
 Description:
 * allows to plan and submit the schedule job EXT040 for all customers

 Revision History:
 Name                    Date         Version   Description of Changes
 RENARN                  2023-12-15   1.0       COMX02 - Cadencier
 FLEBARS                 2024-08-06   1.1       Evolution 20, 52, 56
 FLEBARS                 2025-04-18   1.2       Mise en conformité du code pour validation
 RENARN                  2025-05-12   1.3       Taking into account INFOR standards
 ******************************************************************************************/


import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

public class EXT045 extends ExtendM3Batch {
  private final LoggerAPI logger
  private final DatabaseAPI database
  private final ProgramAPI program
  private final BatchAPI batch
  private final MICallerAPI miCaller
  private final TextFilesAPI textFiles
  private final UtilityAPI utility

  //Logging management
  private List<String> LOGLEVELS = ["DEBUG", "INFO", "WARN", "ERROR"]
  private List<String> logmessages
  private String loglevel
  private String logfile

  private Integer currentCompany
  private String rawData
  private int rawDataLength
  private int beginIndex
  private int endIndex
  private String logFileName
  private boolean in60
  private String jobNumber
  private Integer currentDate
  private String newCdnn
  private String calendar
  private String allContacts
  private String schedule
  private String listAlreadySentCustomers
  private Integer counter

  public EXT045(LoggerAPI logger, DatabaseAPI database, ProgramAPI program, BatchAPI batch, MICallerAPI miCaller, TextFilesAPI textFiles, UtilityAPI utility) {
    this.logger = logger
    this.database = database
    this.program = program
    this.batch = batch
    this.miCaller = miCaller
    this.textFiles = textFiles
    this.utility = utility
  }

  public void main() {
    currentCompany = (Integer) program.getLDAZD().CONO
    // Get job number
    LocalDateTime timeOfCreation = LocalDateTime.now()
    jobNumber = program.getJobNumber() + timeOfCreation.format(DateTimeFormatter.ofPattern("yyMMdd")) + timeOfCreation.format(DateTimeFormatter.ofPattern("HHmmss"))

    //log management
    initializeLogManagement()

    //logger.debug("referenceId = " + batch.getReferenceId().get())
    if (batch.getReferenceId().isPresent()) {
      Optional<String> data = getJobData(batch.getReferenceId().get())
      logger.debug("data = " + data)
      performActualJob(data)
    } else {
      // No job data found
      logMessage("ERROR", "Job data for job ${batch.getJobId()} is missing")
    }
    logMessages()
  }
  // Get job data
  private Optional<String> getJobData(String referenceId) {
    DBAction query = database.table("EXTJOB").index("00").selection("EXDATA").build()
    DBContainer container = query.createContainer()
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
      logMessage("ERROR", "No job data found")
      return
    }
    rawData = data.get()
    logger.debug("Début performActualJob")

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
    if (!ocusmaQuery.readAll(ocusmaRequest, 2, 10000, ocusmaReader)) {
    }
  }

  /**
   * Read OCUSMA
   * @param ocusmaResult
   */
  Closure<?> ocusmaReader = { DBContainer ocusmaResult ->
    String cuno = ocusmaResult.getString("OKCUNO").trim()

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

    newCdnn = ""
    executeEXT040MIRtvNextCalendar(currentCompany as String, cuno)
    calendar = newCdnn

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

    logMessage("INFO", "Customer ${cuno} - Calendar ${calendar} - Assortment ${tAssortment} - All contacts ${allContacts} - Schedule ${schedule}")
    executeEXT820MISubmitBatch(currentCompany as String, "EXT040", cuno, calendar, allContacts, schedule, "", "", "", "", "")

  }

  /**
   * Call EXT040MI.RtvNextCalendar
   * @param CONO
   * @param CUNO
   * @return
   */
  private executeEXT040MIRtvNextCalendar(String CONO, String CUNO) {
    Map<String, String> parameters = ["CONO": CONO, "CUNO": CUNO]
    Closure<?> handler = { Map<String, String> response ->
      if (response.error != null) {
      } else {
        newCdnn = response.CDNN.trim()
      }
    }
    miCaller.call("EXT040MI", "RtvNextCalendar", parameters, handler)
  }
  // Execute EXT820MI.SubmitBatch
  private executeEXT820MISubmitBatch(String CONO, String JOID, String P001, String P002, String P003, String P004, String P005, String P006, String P007, String P008, String P009) {
    Map<String, String> parameters = ["CONO": CONO, "JOID": JOID, "P001": P001, "P002": P002, "P003": P003, "P004": P004, "P005": P005, "P006": P006, "P007": P007, "P008": P008, "P009": P009]
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

  // Log
  void log(String message) {
    in60 = true
    message = LocalDateTime.now().toString() + ";" + message
    Closure<?> consumer = { PrintWriter printWriter ->
      printWriter.println(message)
    }
    textFiles.write(logFileName, "UTF-8", true, consumer)
  }
  /**
   * Initialize log management
   */
  private void initializeLogManagement() {
    logfile = program.getProgramName() + "." + "batch" + "." + jobNumber + ".log"
    logmessages = new LinkedList<String>()
    loglevel = getCRS881("", "EXTENC", "1", "ExtendM3", "I", program.getProgramName(), "LOGLEVEL", "", "")
    if (!LOGLEVELS.contains(loglevel)) {
      String message = "Niveau de log incorrect ${loglevel}"
      loglevel = "ERROR"
      logMessage("ERROR", message)
    }
  }

  /**
   * Add Log message in list
   * @param level
   * @param message
   */
  private void logMessage(String level, String message) {
    int lvl = LOGLEVELS.indexOf(level)
    int lvg = LOGLEVELS.indexOf(loglevel)
    if (lvl >= lvg) {
      message = LocalDateTime.now().toString() + ": ${level} ${message}"
      logmessages.add(message)
    }
  }

  /**
   * Write Log messages in File
   */
  private void logMessages() {
    if (logmessages.isEmpty()) {
      return
    }
    textFiles.open("log")
    String message = String.join("\r\n", logmessages)
    Closure<?> consumer = { PrintWriter printWriter ->
      printWriter.println(message)
    }
    textFiles.write(logfile, "UTF-8", true, consumer)
  }

  /**
   * Get the value in CRS881/CRS882
   * @parameter division
   * @parameter mstd
   * @parameter mvrs
   * @parameter bmsg
   * @parameter ibob
   * @parameter elmp
   * @parameter elmd
   * @parameter elmc
   * @parameter mbmc
   * @return
   */
  private String getCRS881(String division, String mstd, String mvrs, String bmsg, String ibob, String elmp, String elmd, String elmc, String mbmc) {
    String mvxd = ""
    DBAction queryMbmtrn = database.table("MBMTRN").index("00").selection("TRIDTR").build()
    DBContainer requestMbmtrn = queryMbmtrn.getContainer()
    requestMbmtrn.set("TRTRQF", "0")
    requestMbmtrn.set("TRMSTD", mstd)
    requestMbmtrn.set("TRMVRS", mvrs)
    requestMbmtrn.set("TRBMSG", bmsg)
    requestMbmtrn.set("TRIBOB", ibob)
    requestMbmtrn.set("TRELMP", elmp)
    requestMbmtrn.set("TRELMD", elmd)
    requestMbmtrn.set("TRELMC", elmc)
    requestMbmtrn.set("TRMBMC", mbmc)
    if (queryMbmtrn.read(requestMbmtrn)) {
      DBAction queryMbmtrd = database.table("MBMTRD").index("00").selection("TDMVXD", "TDTX15").build()
      DBContainer requestMbmtrd = queryMbmtrd.getContainer()
      requestMbmtrd.set("TDCONO", currentCompany)
      requestMbmtrd.set("TDDIVI", division)
      requestMbmtrd.set("TDIDTR", requestMbmtrn.get("TRIDTR"))
      // Retrieve MBTRND
      Closure<?> readerMbmtrd = { DBContainer resultMbmtrd ->
        mvxd = resultMbmtrd.get("TDTX15") as String
        mvxd = mvxd.trim()
      }
      if (queryMbmtrd.readAll(requestMbmtrd, 3, 1, readerMbmtrd)) {
      }
      return mvxd
    }
  }
}
