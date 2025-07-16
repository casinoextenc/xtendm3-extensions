import java.time.LocalDate
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

/**
 * Name : EXT020
 * COMX01 Gestion des assortiments clients
 * Description :
 * Call EXT022 for each customer from EXT010
 * Date         Changed By   Description
 * 20240118     YYOU         COMX01 - Submit calc EXT022
 * 20240919     PBEAUDOUIN   COMX01 - Create OASCUS and EXT020 -EXT021 if not exist
 * 20250119     YJANNIN      COMX01 - Full refresh
 * 20250409     PBEAUDOUIN   COMX01 - Check for approval
 */

public class EXT020 extends ExtendM3Batch {
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
  private String fvdt = ""
  private String fdat = ""
  private String todaydat = ""
  private String ascd = ""
  private String rawData
  private int rawDataLength
  private int beginIndex
  private int endIndex
  private String jobNumber
  private String referenceId

  //Logging management
  private List<String> LOGLEVELS = ["DEBUG", "INFO", "WARN", "ERROR"]
  private List<String> logmessages
  private String loglevel
  private String logfile

  public EXT020(LoggerAPI logger, DatabaseAPI database, UtilityAPI utility, ProgramAPI program, BatchAPI batch, MICallerAPI miCaller, TextFilesAPI textFiles) {
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
    rawData = data.get()
    String inNBDAYS = getFirstParameter()
    logMessage("DEBUG", "nombre de jours demandés inNBDAYS:${inNBDAYS}")
    svcuno = ""

    currentCompany = (Integer) program.getLDAZD().CONO
    currentDivision = program.getLDAZD().DIVI
    LocalDateTime timeOfCreation = LocalDateTime.now()

    if (inNBDAYS != null && !inNBDAYS.trim().isBlank()) {
      LocalDate currentDate = LocalDate.now()
      if (inNBDAYS.startsWith("-")) {
        fdat == "19700101"
      } else {
        LocalDate dateMinus7Days = currentDate.minusDays(inNBDAYS as int)
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMdd")
        fdat = dateMinus7Days.format(formatter)
        todaydat = currentDate.format(formatter)
        if (!utility.call("DateUtil", "isDateValid", fdat, "yyyyMMdd")) {
          return
        }
      }
    } else {
      fdat = timeOfCreation.format(DateTimeFormatter.ofPattern("yyyyMMdd"))
      todaydat = timeOfCreation.format(DateTimeFormatter.ofPattern("yyyyMMdd"))
    }

    //Read EXT010 modified since fdat
    String tcuno = svcuno
    boolean doloop = true
    while (doloop) {
      ExpressionFactory ext010Expression = database.getExpressionFactory("EXT010")
      ext010Expression = ext010Expression.ge("EXLMDT", fdat)

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
      ExpressionFactory oascusExpression = database.getExpressionFactory("OASCUS")
      oascusExpression = oascusExpression.le("OCFDAT", todaydat)
      oascusExpression = oascusExpression.and(oascusExpression.ge("OCTDAT", todaydat))
      DBAction oascusQuery = database.table("OASCUS").index("20").matching(oascusExpression).selection("OCCONO", "OCASCD", "OCCUNO", "OCFDAT").build()
      DBContainer oascusRequest = oascusQuery.getContainer()
      oascusRequest.set("OCCONO", currentCompany)
      ascd = cuno + "0"
      oascusRequest.set("OCCUNO", cuno)
      if (!oascusQuery.readAll(oascusRequest, 2, 500, oascusReader)) {
        //Creation of OASCUS
        addCompletAssort()
        logMessage("INFO", "launch EXT022 for ASCD:${ascd} CUNO:${cuno} FDAT:${todaydat}")
        executeEXT820MISubmitBatch(currentCompany as String, "EXT022", ascd, cuno, todaydat, "", "2", "", "", "")
      }
      svcuno = cuno.trim()
    }
  }

  // Read OASCUS for each record we post
  Closure<?> oascusReader = { DBContainer oascusResult ->
    // Add selected items in the assortment
    fvdt = oascusResult.get("OCFDAT") as String
    ascd = oascusResult.get("OCASCD")
    cuno = oascusResult.get("OCCUNO")
    logMessage("INFO", "launch EXT022 for ASCD:${ascd} CUNO:${cuno} FDAT:${fvdt}")
    executeEXT820MISubmitBatch(currentCompany as String, "EXT022", ascd, cuno, fvdt, "", "2", "", "", "")
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
    miCaller.call("EXT820MI", "SubmitBatch", parameters, handler)
  }

  /**
   * Add complete assortment
   * @return
   */
  private addCompletAssort() {
    logMessage("INFO", "Début ajout assortiment")
    String tx40 = ascd + " Assortiment Complet"
    String tx15 = tx40.substring(0, 15)
    executeCRS105MIAddAssmHead(currentCompany as String, ascd, tx40, tx15)
    executeCRS105MIAddAssmCust(currentCompany as String, ascd, cuno, todaydat)
    executeEXT020MIAddAssortCriter(currentCompany as String, ascd, cuno, todaydat)
    executeEXT021MIAddAssortHist(currentCompany as String, ascd, cuno, todaydat, "GOLD", "GOLD")
    logMessage("INFO", "Fin ajout assortiment")
  }

  /** CRS105MI AddAssmHead
   *
   * @param CONO
   * @param ASCD
   * @param TX40
   * @param TX15
   * @return
   */
  private executeCRS105MIAddAssmHead(String CONO, String ASCD, String TX40, String TX15) {
    Map parameters = ["CONO": CONO, "ASCD": ASCD, "TX40": TX40, "TX15": TX15]
    Closure<?> handler = { Map<String, String> response ->
      if (response.error != null) {
        String header = "MSG"
        String message = "Failed CRS105MI.AddAssmHead: " + response.errorMessage
        logMessage(header, message)
        return
      } else {
      }
    }
    miCaller.call("CRS105MI", "AddAssmHead", parameters, handler)
  }

  /** Execute CRS105MI.AddAssmCust
   * @param CONO
   * @param ASCD
   * @param CUNO
   * @param FDAT
   * @return
   */
  private executeCRS105MIAddAssmCust(String CONO, String ASCD, String CUNO, String FDAT) {
    Map parameters = ["CONO": CONO, "ASCD": ASCD, "CUNO": CUNO, "FDAT": FDAT]
    Closure<?> handler = { Map<String, String> response ->
      if (response.error != null) {
        String header = "MSG"
        String message = "Failed CRS105MI.AddAssmCust: " + response.errorMessage
        logMessage(header, message)
        return
      } else {
      }
    }
    miCaller.call("CRS105MI", "AddAssmCust", parameters, handler)
  }

  /** Execute EXT020MI.AddAssortCriter
   * @param CONO
   * @param ASCD
   * @param CUNO
   * @param FDAT
   * @return
   */
  private executeEXT020MIAddAssortCriter(String CONO, String ASCD, String CUNO, String FDAT) {
    Map parameters = ["CONO": CONO, "ASCD": ASCD, "CUNO": CUNO, "FDAT": FDAT]
    Closure<?> handler = { Map<String, String> response ->
      if (response.error != null) {
        String header = "MSG"
        String message = "Failed EXT020MI.AddAssortCriter: " + response.errorMessage
        logMessage(header, message)
        return
      } else {
      }
    }
    miCaller.call("EXT020MI", "AddAssortCriter", parameters, handler)
  }

  /** Execute EXT021MI.AddAssortHist
   * @param CONO
   * @param ASCD
   * @param CUNO
   * @param FDAT
   * @param TYPE
   * @param DATA
   * @return
   */
  private executeEXT021MIAddAssortHist(String CONO, String ASCD, String CUNO, String FDAT, String TYPE, String DATA) {
    Map parameters = ["CONO": CONO, "ASCD": ASCD, "CUNO": CUNO, "FDAT": FDAT, "TYPE": TYPE, "DATA": DATA]
    Closure<?> handler = { Map<String, String> response ->
      if (response.error != null) {
        String header = "MSG"
        String message = "Failed EXT021MI.AddAssortHist: " + response.errorMessage
        logMessage(header, message)
        return
      } else {
      }
    }
    miCaller.call("EXT021MI", "AddAssortHist", parameters, handler)
  }

  /** Get first parameter
   * @return
   */
  private String getFirstParameter() {
    rawDataLength = rawData.length()
    beginIndex = 0
    endIndex = rawData.indexOf(";")
    // Get parameter
    String parameter = rawData.substring(beginIndex, endIndex)
    return parameter
  }

  /** Get next parameter
   * @return
   */
  private String getNextParameter() {
    beginIndex = endIndex + 1
    endIndex = rawDataLength - rawData.indexOf(";") - 1
    rawData = rawData.substring(beginIndex, rawDataLength)
    rawDataLength = rawData.length()
    beginIndex = 0
    endIndex = rawData.indexOf(";")
    // Get parameter
    String parameter = rawData.substring(beginIndex, endIndex)
    return parameter
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
      DBAction queryMbmtrd = database.table("MBMTRD").index("00").selection("TDMVXD").build()
      DBContainer requestMbmtrd = queryMbmtrd.getContainer()
      requestMbmtrd.set("TDCONO", currentCompany)
      requestMbmtrd.set("TDDIVI", division)
      requestMbmtrd.set("TDIDTR", requestMbmtrn.get("TRIDTR"))
      // Retrieve MBTRND
      Closure<?> readerMbmtrd = { DBContainer resultMbmtrd ->
        mvxd = resultMbmtrd.get("TDMVXD") as String
        mvxd = mvxd.trim()
      }
      if (queryMbmtrd.readAll(requestMbmtrd, 3, 1, readerMbmtrd)) {
      }
      return mvxd
    }
  }
}
