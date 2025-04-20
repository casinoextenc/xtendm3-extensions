import java.time.LocalDate
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

/**
 * Name : EXT020
 * COMX01 Gestion des assortiments clients
 * Description :
 * Call EXT024 for each customer from EXT010
 * Date         Changed By   Description
 * 20250409     PBEAUDOUIN   COMX01 - Submit call EXT024
 * 20250409     PBEAUDOUIN   COMX01 - Check for approval
 */

public class EXT025 extends ExtendM3Batch {
  private final DatabaseAPI database
  private final LoggerAPI logger
  private final MICallerAPI miCaller
  private final ProgramAPI program
  private final UtilityAPI utility
  private final BatchAPI batch
  private final TextFilesAPI textFiles

  private int currentCompany
  private String currentDivision
  private String cuno = ""
  private String svcuno = ""
  private String rawData
  private String jobNumber
  private String referenceId

  //Logging management
  private List<String> LOGLEVELS = ["DEBUG", "INFO", "WARN", "ERROR"]
  private List<String> logmessages
  private String loglevel
  private String logfile


  public EXT025(LoggerAPI logger, DatabaseAPI database, UtilityAPI utility, ProgramAPI program, BatchAPI batch, MICallerAPI miCaller, TextFilesAPI textFiles) {
    this.logger = logger
    this.database = database
    this.program = program
    this.batch = batch
    this.miCaller = miCaller
    this.utility = utility
    this.textFiles = textFiles
  }

  public void main() {
    // Get job number
    LocalDateTime timeOfCreation = LocalDateTime.now()
    jobNumber = program.getJobNumber() + timeOfCreation.format(DateTimeFormatter.ofPattern("yyMMdd")) + timeOfCreation.format(DateTimeFormatter.ofPattern("HHmmss"))

    //log management
    initializeLogManagement()


    if (batch.getReferenceId().isPresent()) {
      referenceId = batch.getReferenceId()
      Optional<String> data = getJobData(batch.getReferenceId().get())
      if (!data.isEmpty())
        performActualJob(data)
    } else {
      logMessage("ERROR", "No job data found")
    }
    logMessages()
  }

  // Get job data
  private Optional<String> getJobData(String referenceId) {
    DBAction extjobQuery = database.table("EXTJOB").index("00").selection("EXDATA").build()
    DBContainer extjobRequest = extjobQuery.createContainer()
    extjobRequest.set("EXRFID", referenceId)
    if (extjobQuery.read(extjobRequest)) {
      return Optional.of(extjobRequest.getString("EXDATA"))
    } else {
    }
    return Optional.empty()
  }

  /**
   * @param data
   * @return
   */
  private performActualJob(Optional<String> data) {
    if (!data.isPresent()) {
      logMessage("ERROR", "No job data found")
      return
    }
    logMessage("INFO", "Start job EXT025")
    rawData = data.get()
    svcuno = ""
    currentCompany = (Integer) program.getLDAZD().CONO
    currentDivision = program.getLDAZD().DIVI
    LocalDateTime timeOfCreation = LocalDateTime.now()

    //Read EXT010
    String tcuno = svcuno
    boolean doloop = true
    // loop until we have read all the customers having ext010's records
    while (doloop) {
      ExpressionFactory ext010Expression = database.getExpressionFactory("EXT010")
      ext010Expression = ext010Expression.ge("EXLMDT", "19700101")

      if (!svcuno.isEmpty())
        ext010Expression = ext010Expression.and(ext010Expression.gt("EXCUNO", svcuno))

      DBAction ext010Query = database.table("EXT010").index("02").matching(ext010Expression).selection("EXCUNO").build()
      DBContainer ext010Request = ext010Query.getContainer()
      ext010Request.set("EXCONO", currentCompany)

      if (!ext010Query.readAll(ext010Request, 1, 1, ext010Reader)) {
      }
      if (tcuno.equals(svcuno)) {
        doloop = false
      }
      tcuno = svcuno
    }
  }

  // Read EXT010
  Closure<?> ext010Reader = { DBContainer ext010Result ->
    cuno = ext010Result.getString("EXCUNO").trim()
    if (!cuno.equals(svcuno)) {
      executeEXT820MISubmitBatch(currentCompany as String, "EXT024", cuno, "","", "", "2", "", "", "")
    }
    logMessage("DEBUG", "ext010Reader cuno:${cuno}")
    svcuno = cuno.trim()
  }


  /** Execute EXT820MI.SubmitBatch
   * @param CONO
   * @param JOID
   * @param P001
   * @param P002
   * @param P003
   * @param P004
   * @param P005
   * @param P006
   * @param P007
   * @param P008
   * @return
   */
  private executeEXT820MISubmitBatch(String CONO, String JOID, String P001, String P002, String P003, String P004, String P005, String P006, String P007, String P008) {
    Map<String, String> parameters = ["CONO": CONO, "JOID": JOID, "P001": P001, "P002": P002, "P003": P003, "P004": P004, "P005": P005, "P006":
      P006, "P007"                          : P007, "P008": P008]
    Closure<?> handler = { Map<String, String> response ->
      if (response.error != null) {
      } else {
      }
    }
    logMessage("INFO", "Start processing EXT024 for cuno:${cuno}")
    miCaller.call("EXT820MI", "SubmitBatch", parameters, handler)
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
