/**
 * README
 * This extension is used by Mashup
 *
 * Name : EXT024
 * Description : Delete assortement from EXT010, OIQ072, CRS105 and OIS017
 * Date         Changed By   Description
 * 20250114     YJANNIN      COMX01- Del assortment
 * 20250409     PBEAUDOUIN   Check for approval
 */

import java.time.LocalDateTime
import java.time.ZoneOffset
import java.time.format.DateTimeFormatter
import mvx.db.common.PositionKey
import mvx.db.common.PositionEmpty

public class EXT024 extends ExtendM3Batch {
  private final DatabaseAPI database
  private final LoggerAPI logger
  private final MICallerAPI miCaller
  private final ProgramAPI program
  private final UtilityAPI utility
  private final BatchAPI batch
  private final TextFilesAPI textFiles

  //Logging management
  private List<String> LOGLEVELS = ["DEBUG", "INFO", "WARN", "ERROR"]
  private List<String> logmessages
  private String loglevel
  private String logfile


  private int currentCompany
  private String cuno
  private String cucd
  private String currentDate

  private String rawData
  private int rawDataLength
  private int beginIndex
  private int endIndex
  private String jobNumber
  List<String> oascusList


  public EXT024(LoggerAPI logger, DatabaseAPI database, UtilityAPI utility, ProgramAPI program, BatchAPI batch, MICallerAPI miCaller, TextFilesAPI textFiles) {
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
    currentCompany = (Integer) program.getLDAZD().CONO
    LocalDateTime timeOfCreation = LocalDateTime.now()
    currentDate = timeOfCreation.format(DateTimeFormatter.ofPattern("yyyyMMdd"))
    jobNumber = program.getJobNumber() + timeOfCreation.format(DateTimeFormatter.ofPattern("yyMMdd")) + timeOfCreation.format(DateTimeFormatter.ofPattern("HHmmss"))

    //log management
    initializeLogManagement()

    if (batch.getReferenceId().isPresent()) {
      Optional<String> data = getJobData(batch.getReferenceId().get())
      performActualJob(data)
    } else {
      logMessage("ERROR", "No job data found")
    }
    logMessages()
  }

  // Perform actual job
  private performActualJob(Optional<String> data) {
    if (!data.isPresent()) {
      logMessage("ERROR", "No job data found")
      return
    }
    rawData = data.get()
    String inCuno = getFirstParameter()
    oascusList = new LinkedList<String>()
    cuno = inCuno
    cucd = 'EUR'

    logMessage("INFO", "début suppression assortiments client:${cuno}")
    // 1rst step Delete all assortment from EXT010 for the customer
    deleteAssortmentFromExt010()

    // 2nd step Delete all assortment from EXT072 for the customer
    deleteAssortmentFromOis072()

    // 3rd step Delete all prices from oprbas opricl
    deleteAssortmentFromOis017()

    // Delete file EXTJOB
    deleteEXTJOB()
  }

  /**
   * Delete assortment from EXT010
   */
  private void deleteAssortmentFromExt010() {
    int count = 0
    DBAction ext010Query = database.table("EXT010")
      .index("02")
      .build()

    DBContainer ext010Request = ext010Query.getContainer()
    ext010Request.set("EXCONO", currentCompany)
    ext010Request.set("EXCUNO", cuno)

    // Delete EXT010
    Closure<?> ext010Reader = { DBContainer ext010Result ->
      Closure<?> ext010Updater = { LockedResult ext010LockedResult ->
        ext010LockedResult.delete()
        count++
      }
      ext010Query.readLock(ext010Result, ext010Updater)
    }
    boolean doLoop = true
    int nbIteration = 0
    while (doLoop) {
      if (nbIteration > 0) {
        PositionKey position = ext010Query.getPositionKey()
        if (position instanceof PositionEmpty) {
          doLoop = false
          break
        } else {
          ext010Query = database.table("EXT010")
            .index("02")
            .position(position)
            .build()
        }
      }
      if (!ext010Query.readAll(ext010Request, 2, 10000, ext010Reader)) {
      }
      nbIteration++
      if (nbIteration > 5) {//max 50 0000 records
        logMessage("ERROR", "Nombre d'itération trop important cuno:${cuno}")
        break
      }
    }
    logMessage('INFO', "client:${cuno} ${count} enregistrements EXT010 supprimés")
  }

  /**
   * Delete assortment from OIS072
   */
  private void deleteAssortmentFromOis072() {
    int count = 0
    // Load oascus records for the customer in list
    DBAction queryOascus = database.table("OASCUS")
      .index("10")
      .selection(
        "OCASCD", "OCFDAT"
      )
      .build()
    DBContainer requestOascus = queryOascus.getContainer()
    requestOascus.set("OCCONO", currentCompany)
    requestOascus.set("OCCUNO", cuno)
    Closure<?> oascusReader = { DBContainer recordOascus ->
      String ascd = recordOascus.get("OCASCD")
      String fdat = recordOascus.get("OCFDAT") as String
      // Check selection header
      DBAction queryCsytab = database.table("CSYTAB")
        .index("00")
        .selection("CTSTKY","CTTX15","CTTX40")
        .build()

      DBContainer requestCsytab = queryCsytab.getContainer()
      requestCsytab.set("CTCONO", currentCompany)
      requestCsytab.set("CTDIVI", "")
      requestCsytab.set("CTSTCO", "ASCD")
      requestCsytab.set("CTSTKY", ascd)
      Closure<?> outDataCsytab = { DBContainer resultCsytab ->
        String rAscd = resultCsytab.get("CTSTKY") as String
        String rTx15 = resultCsytab.get("CTTX15") as String
        String rTx40 = resultCsytab.get("CTTX40")
        logMessage("DEBUG", "start suppression assortiment ${rAscd} ${rTx40} ${rTx15} ${cuno} ${fdat}")
        // Delete selected items in the assortment
        executeCRS105MIDltAssmHead(rAscd.trim())
        // Create Assortment
        executeCRS105MIAddAssmHead(rAscd.trim(),rTx40.trim(),rTx15.trim())
        // Create Assortment
        executeCRS105MIAddAssmCust(rAscd.trim(),cuno,fdat.trim())
        logMessage("DEBUG", "end suppression assortiment ${rAscd} ${rTx40} ${rTx15} ${cuno} ${fdat}")
      }
      queryCsytab.readAll(requestCsytab, 4, 1,outDataCsytab)
      count++
    }
    queryOascus.readAll(requestOascus, 2, 1, oascusReader)
    logMessage('INFO', "client:${cuno} ${count} enregistrements OIS071 supprimés puis recrées")
    // no assortment for the customer
    if (oascusList.isEmpty()) {
      return
    }
  }

  /**
   * Delete prices from OIS017
   */
  private void deleteAssortmentFromOis017() {
    int count = 0
    String prrf = getPriceLstMatrix()

    DBAction oprichQuery = database.table("OPRICH").index("00")
      .selection(
        "OJFVDT",
        "OJLVDT",
        "OJTX40",
        "OJTX15",
        "OJCRTP"
      )
      .build()
    DBContainer oprichRequest = oprichQuery.getContainer()
    oprichRequest.set("OJCONO", currentCompany)
    oprichRequest.set("OJPRRF", prrf)
    oprichRequest.set("OJCUCD", cucd)
    oprichRequest.set("OJCUNO", cuno)
    Closure<?> oprichReader = { DBContainer oprichRecord ->
      executeOIS017MIDelPriceList(prrf,
        oprichRecord.get("OJFVDT") as String)

      count++
    }
    oprichQuery.readAll(oprichRequest, 4, 10000, oprichReader)
    logMessage('INFO', "client:${cuno} ${count} enregistrements OPRICH supprimés puis recrées")
  }

  /**
   * Get price list matrix for customer
   * @return prrf
   */
  private String getPriceLstMatrix() {
    String oPrrf = ""
    // Delete Assortment selection
    DBAction oprmtxQuery = database.table("OPRMTX")
      .index("00")
      .selection("DXPRRF")
      .build()
    DBContainer oprmtxRequest = oprmtxQuery.getContainer()
    oprmtxRequest.set("DXCONO", currentCompany)
    oprmtxRequest.set("DXPLTB", "CSN")
    oprmtxRequest.set("DXPREX", " 5")
    oprmtxRequest.set("DXOBV1", cuno.trim())
    if (oprmtxQuery.read(oprmtxRequest)) {
      oPrrf = oprmtxRequest.get("DXPRRF") as String
    }
    return oPrrf
  }

  /**
   * Create EXT011 record
   * @parameter DBContainer
   * @parameter flag
   * */
  private void createExt011Record(DBContainer recordExt010, String flag) {
    DBAction ext011Query = database.table("EXT011")
      .index("00")
      .selection(
        "EXCONO",
        "EXASGD",
        "EXCUNO",
        "EXITNO",
        "EXCDAT",
        "EXRGDT",
        "EXRGTM",
        "EXCHNO",
        "EXCHID"
      )
      .build()

    DBContainer ext011Request = ext011Query.getContainer()
    ext011Request.set("EXCONO", currentCompany)
    ext011Request.set("EXASGD", recordExt010.get("EXASGD"))
    ext011Request.set("EXCUNO", recordExt010.get("EXCUNO"))
    ext011Request.set("EXITNO", recordExt010.get("EXITNO"))
    ext011Request.set("EXSIG6", recordExt010.get("EXSIG6"))
    ext011Request.set("EXSAPR", recordExt010.get("EXSAPR"))
    ext011Request.set("EXSULE", recordExt010.get("EXSULE"))
    ext011Request.set("EXSULD", recordExt010.get("EXSULD"))
    ext011Request.set("EXFUDS", recordExt010.get("EXFUDS"))
    ext011Request.set("EXCDAT", recordExt010.get("EXCDAT"))
    ext011Request.set("EXRSCL", recordExt010.get("EXRSCL"))
    ext011Request.set("EXCMDE", recordExt010.get("EXCMDE"))
    ext011Request.set("EXFVDT", recordExt010.get("EXFVDT"))
    ext011Request.set("EXLVDT", recordExt010.get("EXLVDT"))
    ext011Request.set("EXTVDT", recordExt010.get("EXTVDT"))
    ext011Request.set("EXRGDT", recordExt010.get("EXRGDT"))
    ext011Request.set("EXRGTM", recordExt010.get("EXRGTM"))
    ext011Request.set("EXCHNO", recordExt010.get("EXCHNO"))
    ext011Request.set("EXCHID", recordExt010.get("EXCHID"))
    ext011Request.set("EXFLAG", flag)
    ext011Query.insert(ext011Request)
  }

  // Execute CRS105MI DltAssmHead
  private executeCRS105MIDltAssmHead(String pAscd) {
    Map<String, String> parameters = ["ASCD": pAscd]
    Closure<?> handler = { Map<String, String> response ->
      if (response.error != null) {
        String message = "Failed CRS105MI.DltAssmHead: " + response.errorMessage
        logMessage("ERROR", message)
        return
      } else {
      }
    }
    miCaller.call("CRS105MI", "DltAssmHead", parameters, handler)
  }

  // Execute CRS105MI AddAssmHead
  private executeCRS105MIAddAssmHead(String pAscd, String pTx40, String pTx15) {
    Map<String, String> parameters = [
      "ASCD": pAscd,
      "TX40":pTx40,
      "TX15":pTx15
    ]
    Closure<?> handler = { Map<String, String> response ->
      if (response.error != null) {
        String message = "Failed CRS105MI.AddAssmHead: " + response.errorMessage
        logMessage("ERROR", message)
        return
      } else {
      }
    }
    miCaller.call("CRS105MI", "AddAssmHead", parameters, handler)
  }
  // Execute CRS105MI AddAssmHead
  private executeCRS105MIAddAssmCust(String pAscd, String pCuno, String pFdat) {
    Map<String, String> parameters = [
      "ASCD": pAscd,
      "CUNO": pCuno,
      "FDAT": pFdat
    ]
    Closure<?> handler = { Map<String, String> response ->
      if (response.error != null) {
        String message = "Failed CRS105MI.AddAssmCust: " + response.errorMessage
        logMessage("ERROR", message)
        return
      } else {
      }
    }
    miCaller.call("CRS105MI", "AddAssmCust", parameters, handler)
  }
  /**
   * Execute OIS017MI DelPriceList
   * @param prrf
   * @param fvdt
   */
  private void executeOIS017MIDelPriceList(String prrf, String fvdt) {
    Map<String, String> parameters = [
      "PRRF": prrf,
      "CUNO": cuno,
      "CUCD": cucd,
      "FVDT": fvdt,
    ]
    Closure<?> handler = { Map<String, String> response ->
      if (response.error != null) {
        String message = "Failed OIS017MI.DelPriceList: " + response.errorMessage
        logMessage("ERROR", message)
        return
      } else {
      }
    }
    miCaller.call("OIS017MI", "DelPriceList", parameters, handler)
  }

  // Get job data
  private Optional<String> getJobData(String referenceId) {
    def query = database.table("EXTJOB").index("00").selection("EXDATA").build()
    def container = query.createContainer()
    container.set("EXRFID", referenceId)
    if (query.read(container)) {
      return Optional.of(container.getString("EXDATA"))
    } else {
    }
    return Optional.empty()
  }

  // Get first parameter
  private String getFirstParameter() {
    rawDataLength = rawData.length()
    beginIndex = 0
    endIndex = rawData.indexOf(";")
    // Get parameter
    String parameter = rawData.substring(beginIndex, endIndex)
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
    return parameter
  }

  // Delete records related to the current job from EXTJOB table
  public void deleteEXTJOB() {
    LocalDateTime timeOfCreation = LocalDateTime.now()
    DBAction query = database.table("EXTJOB").index("00").build()
    DBContainer EXTJOB = query.getContainer()
    EXTJOB.set("EXRFID", batch.getReferenceId().get())
    if (!query.readAllLock(EXTJOB, 1, updateCallBackExtJob)) {
    }
  }

  // Delete EXTJOB
  Closure<?> updateCallBackExtJob = { LockedResult lockedResult ->
    lockedResult.delete()
  }

  /**
   * Get the value in CRS881/CRS882
   * @param division
   * @param mstd
   * @param mvrs
   * @param bmsg
   * @param ibob
   * @param elmp
   * @param elmd
   * @param elmc
   * @param mbmc
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

  /**
   *
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
    textFiles.open("log")
    if (logmessages.isEmpty()) {
      return
    }
    String message = String.join("\r\n", logmessages)
    Closure<?> consumer = { PrintWriter printWriter ->
      printWriter.println(message)
    }
    textFiles.write(logfile, "UTF-8", true, consumer)
  }

}
