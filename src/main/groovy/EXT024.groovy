/**
 * README
 * This extension is used by Mashup
 *
 * Name : EXT024
 * Description : Select the items based on the criteria contained in EXT021 table and add records to the EXT022 table (EXT022MI.SelAssortItems conversion)
 * Date         Changed By   Description
 * 20250114     YJANNIN      COMX01- Del assortment
 */

import java.time.LocalDateTime
import java.time.ZoneOffset
import java.time.format.DateTimeFormatter

public class EXT024 extends ExtendM3Batch {

  private final MIAPI mi
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
  private String rawData
  private int rawDataLength
  private int beginIndex
  private int endIndex
  private String logFileName
  private String jobNumber
  private boolean IN60 = false
  private String currentDate = ""
  private Integer nbMaxRecord = 10000

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
    LocalDateTime timeOfCreation = LocalDateTime.now()
    jobNumber = program.getJobNumber() + timeOfCreation.format(DateTimeFormatter.ofPattern("yyMMdd")) + timeOfCreation.format(DateTimeFormatter.ofPattern("HHmmss"))

    //logger.debug("Début EXT024")
    if (batch.getReferenceId().isPresent()) {
      Optional<String> data = getJobData(batch.getReferenceId().get())
      performActualJob(data)
    } else {
      // No job data found
      //logger.debug("Job data for job ${batch.getJobId()} is missing")
    }
  }
  // Get job data
  private Optional<String> getJobData(String referenceId) {
    def query = database.table("EXTJOB").index("00").selection("EXDATA").build()
    def container = query.createContainer()
    container.set("EXRFID", referenceId)
    if (query.read(container)) {
      //logger.debug("EXDATA = " + container.getString("EXDATA"))
      return Optional.of(container.getString("EXDATA"))
    } else {
      //logger.debug("EXTJOB not found")
    }
    return Optional.empty()
  }
  // Perform actual job
  private performActualJob(Optional<String> data) {
    if (!data.isPresent()) {
      //logger.debug("Job reference Id ${batch.getReferenceId().get()} is passed but data was not found")
      return
    }
    rawData = data.get()
    logger.debug("Début performActualJob")
    String inCUNO = getFirstParameter()

    logger.debug("#YJ inCUNO = " + inCUNO)

    currentCompany = (Integer) program.getLDAZD().CONO
    currentDivision = program.getLDAZD().DIVI
    LocalDateTime timeOfCreation = LocalDateTime.now()

    logger.debug("#YJ current Company = " + currentCompany)

    cuno = inCUNO

    DBAction queryOCUSMA = database.table("OCUSMA")
      .index("00")
      .selection(
        "OKCUNO"
      )
      .build()
    DBContainer OCUSMA = queryOCUSMA.getContainer()
    OCUSMA.set("OKCONO", currentCompany)
    OCUSMA.set("OKCUNO", cuno)
    if (queryOCUSMA.read(OCUSMA)) {

      // Retrive assortment
      DBAction queryOASCUS = database.table("OASCUS")
        .index("10")
        .selection(
          "OCASCD"
        )
        .build()
      DBContainer OASCUS = queryOASCUS.getContainer()
      OASCUS.set("OCCONO", currentCompany)
      OASCUS.set("OCCUNO", cuno)
      if (!queryOASCUS.readAll(OASCUS, 2, nbMaxRecord, outDataOASCUS)) {
      }

      // Delete Assortment selection
      DBAction ext010Query = database.table("EXT010")
        .index("02")
        .selection(
          "EXCONO",
          "EXASGD",
          "EXCUNO",
          "EXITNO",
          "EXCDAT",
          "EXSIG6",
          "EXSAPR",
          "EXSULE",
          "EXSULD",
          "EXFUDS",
          "EXRSCL",
          "EXCMDE",
          "EXFVDT",
          "EXLVDT",
          "EXTVDT",
          "EXRGDT",
          "EXRGTM",
          "EXLMDT",
          "EXCHNO",
          "EXCHID"
        )
        .build();

      DBContainer ext010Request = ext010Query.getContainer()
      ext010Request.set("EXCONO", currentCompany)
      ext010Request.set("EXCUNO", cuno)

      Closure<?> ext010Updater = { LockedResult ext010LockedResult ->
        createEXT011Record(ext010LockedResult, "D")
        ext010LockedResult.delete()
      }

      ext010Query.readAllLock(ext010Request, 2, ext010Updater)


    }

    // Delete file EXTJOB
    deleteEXTJOB()
  }

  /**
   * Create EXT011 record
   * @parameter DBContainer
   * */
  private void createEXT011Record(DBContainer ext010Request, String flag) {
    LocalDateTime timeOfCreation = LocalDateTime.now()
    Long lmts = timeOfCreation.toInstant(ZoneOffset.UTC).toEpochMilli()
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
    ext011Request.set("EXASGD", ext010Request.get("EXASGD"))
    ext011Request.set("EXCUNO", ext010Request.get("EXCUNO"))
    ext011Request.set("EXITNO", ext010Request.get("EXITNO"))
    ext011Request.set("EXSIG6", ext010Request.get("EXSIG6"))
    ext011Request.set("EXSAPR", ext010Request.get("EXSAPR"))
    ext011Request.set("EXSULE", ext010Request.get("EXSULE"))
    ext011Request.set("EXSULD", ext010Request.get("EXSULD"))
    ext011Request.set("EXFUDS", ext010Request.get("EXFUDS"))
    ext011Request.set("EXCDAT", ext010Request.get("EXCDAT"))
    ext011Request.set("EXRSCL", ext010Request.get("EXRSCL"))
    ext011Request.set("EXCMDE", ext010Request.get("EXCMDE"))
    ext011Request.set("EXFVDT", ext010Request.get("EXFVDT"))
    ext011Request.set("EXLVDT", ext010Request.get("EXLVDT"))
    ext011Request.set("EXTVDT", ext010Request.get("EXTVDT"))
    ext011Request.set("EXRGDT", ext010Request.get("EXRGDT"))
    ext011Request.set("EXRGTM", ext010Request.get("EXRGTM"))
    ext011Request.set("EXCHNO", ext010Request.get("EXCHNO"))
    ext011Request.set("EXCHID", ext010Request.get("EXCHID"))
    ext011Request.set("EXLMTS", lmts)
    ext011Request.set("EXFLAG", flag)
    ext011Query.insert(ext011Request)
  }

  // Retrieve EXT021
  Closure<?> outDataOASCUS = { DBContainer OASCUS ->
    String ascd = OASCUS.get("OCASCD")
    logger.debug("#YJ ASCD = " + ascd)

    // Check selection header
    DBAction queryOASITN = database.table("OASITN")
      .index("00")
      .selection(
        "OIASCD",
        "OIITNO",
        "OIFDAT"
      )
      .build()
    DBContainer OASITN = queryOASITN.getContainer()
    OASITN.set("OICONO", currentCompany)
    OASITN.set("OIASCD", ascd)
    Closure<?> outDataOASITN = { DBContainer resultOASITN ->
      String rAscd = resultOASITN.get("OIASCD")
      String rItno = resultOASITN.get("OIITNO")
      String rFdat = resultOASITN.get("OIFDAT")
      logger.debug("#YJ ASCD = " + rAscd)
      logger.debug("#YJ ITNO = " + rItno)
      logger.debug("#YJ FDAT = " + rFdat)

      // Delete selected items in the assortment
      executeCRS105MIDltAssmItem(rAscd, rItno, rFdat)

    }
    if (!queryOASITN.readAll(OASITN, 2, outDataOASITN)) {
    }

    // Delete Assortment selection
    DBAction queryEXT022 = database.table("EXT022").index("00").build()
    DBContainer EXT022 = queryEXT022.getContainer()
    EXT022.set("EXCONO", currentCompany)
    EXT022.set("EXASCD", ascd)
    EXT022.set("EXCUNO", cuno)
    Closure<?> outDataEXT022 = { LockedResult lockedResultEXT022 ->
      lockedResultEXT022.delete()
    }
    if (!queryEXT022.readAllLock(EXT022, 3, outDataEXT022)) {
    }

  }

  // Execute CRS105MI DltAssmItem
  private executeCRS105MIDltAssmItem(String ASCD, String ITNO, String FDAT) {
    Map<String, String> parameters = ["ASCD": ASCD, "ITNO": ITNO, "FDAT": FDAT]
    Closure<?> handler = { Map<String, String> response ->
      if (response.error != null) {
        String header = "MSG"
        String message = "Failed CRS105MI.DltAssmItem: " + response.errorMessage
        logMessage(header, message)
        return
      } else {
      }
    }
    miCaller.call("CRS105MI", "DltAssmItem", parameters, handler)
  }

  // Get first parameter
  private String getFirstParameter() {
    //logger.debug("rawData = " + rawData)
    rawDataLength = rawData.length()
    beginIndex = 0
    endIndex = rawData.indexOf(";")
    // Get parameter
    String parameter = rawData.substring(beginIndex, endIndex)
    //logger.debug("parameter = " + parameter)
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
    //logger.debug("parameter = " + parameter)
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
    IN60 = true
    ////logger.debug(message)
    message = LocalDateTime.now().toString() + ";" + message
    Closure<?> consumer = { PrintWriter printWriter ->
      printWriter.println(message)
    }
    textFiles.write(logFileName, "UTF-8", true, consumer)
  }
}
