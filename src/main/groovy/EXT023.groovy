/**
 * README
 * This extension is used by batch EXT022
 *
 * Name : EXT023
 * Description : Read EXT022 table and call "CRS105MI/AddAssmItem" for each item (EXT023MI.AddAssortItems conversion)
 * Date         Changed By   Description
 * 20220112     YBLUTEAU     COMX01- Add assortment
 * 20240206		YVOYOU		 COMX01 - Exclude item management
 */
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

public class EXT023 extends ExtendM3Batch {
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
  private boolean IN60
  private String jobNumber
  private String ascd = ""
  private String cuno = ""
  private String fdat = ""
  private String itno = ""
  private boolean exclu
  private Integer nbMaxRecord = 10000

  public EXT023(LoggerAPI logger, DatabaseAPI database, ProgramAPI program, BatchAPI batch, MICallerAPI miCaller, TextFilesAPI textFiles, UtilityAPI utility) {
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
    logger.debug("referenceId = " + batch.getReferenceId().get())
    if(batch.getReferenceId().isPresent()){
      Optional<String> data = getJobData(batch.getReferenceId().get())
      logger.debug("data = " + data)
      performActualJob(data)
    } else {
      // No job data found
      logger.error("Job data for job ${batch.getJobId()} is missing")
    }
  }
  // Optional
  private Optional<String> getJobData(String referenceId){
    DBAction query = database.table("EXTJOB").index("00").selection("EXDATA").build()
    DBContainer container = query.createContainer()
    container.set("EXRFID", referenceId)
    if (query.read(container)){
      logger.debug("EXDATA = " + container.getString("EXDATA"))
      return Optional.of(container.getString("EXDATA"))
    } else {
      logger.error("EXTJOB not found")
    }
    return Optional.empty()
  }
  // Perform actual job
  private performActualJob(Optional<String> data){
    if(!data.isPresent()){
      logger.error("Job reference Id ${batch.getReferenceId().get()} is passed but data was not found")
      return
    }
    rawData = data.get()
    logger.debug("Début performActualJob")
    String inASCD = getFirstParameter()
    String inCUNO = getNextParameter()
    String inFDAT = getNextParameter()

    currentCompany = (Integer)program.getLDAZD().CONO

    // Perform Job
    ascd = inASCD
    cuno = inCUNO

    fdat =""
    if (inFDAT == null){
      String header = "MSG"
      String message = "Date de début est obligatoire"
      logMessage(header, message)
      return
    } else {
      fdat = inFDAT
      if (!utility.call("DateUtil", "isDateValid", fdat, "yyyyMMdd")) {
        String header = "MSG"
        String message = "Date de début est invalide"
        logMessage(header, message)
        return
      }
    }
    logger.debug("EXT023 fdat = " + fdat)

    // Check selection header
    DBAction queryEXT020 = database.table("EXT020").index("00").build()
    DBContainer EXT020 = queryEXT020.getContainer()
    EXT020.set("EXCONO", currentCompany)
    EXT020.set("EXASCD", ascd)
    EXT020.set("EXCUNO", cuno)
    EXT020.setInt("EXFDAT", fdat as Integer)
    if(!queryEXT020.readAll(EXT020, 4, nbMaxRecord, outDataEXT020)){
      String header = "MSG"
      String message = "Entête sélection n'existe pas"
      logMessage(header, message)
      return
    }

    DBAction queryEXT022 = database.table("EXT022").index("00").selection("EXITNO").build()
    DBContainer EXT022 = queryEXT022.getContainer()
    EXT022.set("EXCONO", currentCompany)
    EXT022.set("EXASCD", ascd)
    EXT022.set("EXCUNO", cuno)
    EXT022.set("EXFDAT", fdat as Integer)
    if (!queryEXT022.readAll(EXT022, 4, nbMaxRecord, outDataEXT022)) {
    }


    // Delete file EXTJOB
    deleteEXTJOB()
  }
  // Get first parameter
  private String getFirstParameter(){
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
  private String getNextParameter(){
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
  /**
   * Delete records related to the current job from EXTJOB table
   */
  public void deleteEXTJOB(){
    LocalDateTime timeOfCreation = LocalDateTime.now()
    DBAction query = database.table("EXTJOB").index("00").build()
    DBContainer EXTJOB = query.getContainer()
    EXTJOB.set("EXRFID", batch.getReferenceId().get())
    if(!query.readAllLock(EXTJOB, 1, updateCallBackEXTJOB)){
    }
  }
  // Delete EXTJOB
  Closure<?> updateCallBackEXTJOB = { LockedResult lockedResult ->
    lockedResult.delete()
  }
  // Log message
  void logMessage(String header, String message) {
    textFiles.open("FileImport")
    logFileName = "MSG_" + program.getProgramName() + "." + "batch" + "." + jobNumber + ".csv"
    if(!textFiles.exists(logFileName)) {
      log(header)
      log(message)
    }
  }
  // Log
  void log(String message) {
    IN60 = true
    logger.debug(message)
    message = LocalDateTime.now().toString() + "" + message
    Closure<?> consumer = { PrintWriter printWriter ->
      printWriter.println(message)
    }
    textFiles.write(logFileName, "UTF-8", true, consumer)
  }
  // Retrieve EXT020
  Closure<?> outDataEXT020 = { DBContainer EXT020 ->
  }
  // Retrieve EXT022
  Closure<?> outDataEXT022 = { DBContainer EXT022 ->
    itno = EXT022.get("EXITNO")
    logger.debug("executeCRS105MIAddAssmItem : ascd = " + ascd)
    logger.debug("executeCRS105MIAddAssmItem : itno = " + itno)
    logger.debug("executeCRS105MIAddAssmItem : fdat = " + fdat)
    executeCRS105MIAddAssmItem(ascd, itno, fdat)
  }
  // Execute CRS105MI.AddAssmItem
  private executeCRS105MIAddAssmItem(String ASCD, String ITNO, String FDAT){
    Map<String, String> parameters = ["ASCD": ASCD, "ITNO": ITNO, "FDAT": FDAT]
    Closure<?> handler = { Map<String, String> response ->
      if (response.error != null) {
      } else {
      }
    }
    //Search Item exclusion
    exclu = false
    ExpressionFactory expressionEXT025 = database.getExpressionFactory("EXT025")
    expressionEXT025 = expressionEXT025.le("EXFDAT", FDAT)

    DBAction queryEXT025 = database.table("EXT025").index("00").matching(expressionEXT025).selection("EXCONO", "EXITNO", "EXCUNO", "EXFDAT", "EXRGDT", "EXRGTM", "EXLMDT", "EXCHNO", "EXCHID").build()
    DBContainer EXT025 = queryEXT025.getContainer()
    EXT025.set("EXCONO", currentCompany)
    EXT025.set("EXCUNO", cuno)
    EXT025.set("EXITNO", ITNO)
    if(!queryEXT025.readAll(EXT025, 3, nbMaxRecord, outDataEXT025)){
    }
    logger.debug("Exclu : " + ITNO + "-" + cuno +"-"+FDAT+"-"+exclu)
    if (!exclu) {
      miCaller.call("CRS105MI", "AddAssmItem", parameters, handler)
    }
  }
  // Retrieve EXT075
  Closure<?> outDataEXT025 = { DBContainer EXT025 ->
    exclu = true
  }
}
