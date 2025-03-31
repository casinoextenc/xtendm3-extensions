import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

/**
 * Name : EXT022
 * COMX01 Gestion des assortiments clients
 * Description : Select the items based on the criteria contained in EXT021 table and add records to the EXT022 table (EXT022MI.SelAssortItems conversion)
 * Date         Changed By   Description
 * 20220112     YBLUTEAU     COMX01- Add assortment
 * 20230303     ARENARD      Constraints handling has been added and adjustments have been made
 * 20230717     ARENARD      csno handling has been fixed
 * 20240620     FLEBARS       COMX01 - Controle code pour validation Infor
 */

public class EXT022 extends ExtendM3Batch {
  private final MIAPI mi
  private final DatabaseAPI database
  private final LoggerAPI logger
  private final MICallerAPI miCaller
  private final ProgramAPI program
  private final UtilityAPI utility
  private final BatchAPI batch
  private final TextFilesAPI textFiles
  private int currentCompany

  //Logging management
  private List<String> LOGLEVELS = ["DEBUG", "INFO", "WARN", "ERROR"]
  private List<String> logmessages
  private String loglevel
  private String logfile
  private String logFileName

  private String ascd = ""
  private String cuno = ""
  private String fdat = ""
  private String fvdt = ""
  private String stat = ""
  private Integer iSUNO = 0
  private String suno = ""
  private Integer iPROD = 0
  private String prod = ""
  private Integer iHIE1 = 0
  private String hie1 = ""
  private Integer iHIE2 = 0
  private String hie2 = ""
  private Integer iHIE3 = 0
  private String hie3 = ""
  private Integer iHIE4 = 0
  private String hie4 = ""
  private Integer iHIE5 = 0
  private String hie5 = ""
  private Integer iBUAR = 0
  private String buar = ""
  private Integer iCFI1 = 0
  private String cfi1 = ""
  private Integer iCSCD = 0
  private String cscd = ""
  private Integer iCSNO = 0
  private String csno = ""
  private Integer iCFI2 = 0
  private String cfi2 = ""
  private Integer iITNO = 0
  private String itno = ""
  private String lastItno = ""
  private Integer iPOPN = 0
  private String popn = ""
  private Integer iULTY = 0
  private String ulty = ""
  private Integer iSLDY = 0
  private String sldy = ""
  private Integer iCPFX = 0
  private String cpfx = ""
  private Integer iCMDE = 0
  private String cmde = ""
  private Integer iGOLD = 0
  private String spe1 = ""
  private String hazi
  private String cfi4
  private Integer zali
  private String constraintCSCD
  private Integer zalc
  private Integer zsan
  private String zcap
  private String zca1
  private String zca2
  private String zca3
  private String zca4
  private String zca5
  private String zca6
  private String zca7
  private String zca8
  private Integer zagr
  private String znag
  private Integer zori
  private Integer zphy
  private String sule
  private String suld
  private String data
  private Integer zohf
  private Integer ext010Fvdt
  private Integer ext010Lvdt
  private double price

  private boolean constraintIsOK = false
  private boolean criteriaFound = false
  private Integer itemsInOASITN = 0
  private Integer countExt010Records = 0
  private boolean in60 = false
  private String rawData
  private int rawDataLength
  private int beginIndex
  private int endIndex
  private String logfile
  private String jobNumber
  private String referenceId
  private Integer nbMaxRecord = 10000

  public EXT022(LoggerAPI logger, DatabaseAPI database, UtilityAPI utility, ProgramAPI program, BatchAPI batch, MICallerAPI miCaller, TextFilesAPI textFiles) {
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
    jobNumber = program.getJobNumber() + timeOfCreation.format(DateTimeFormatter.ofPattern("yyMMdd")) + timeOfCreation.format(DateTimeFormatter.ofPattern("HHmmss"))

    //log management
    initializeLogManagement()

    //initialize log file
    textFiles.open("log")
    logfile = program.getProgramName() + "." + "batch" + "." + jobNumber + ".log"
    logmessages = new LinkedList<String>()


    String nbr = getCRS881("","EXTENC", "1", "ExtendM3", "I", "EXT022", "nbMaxRecord", "", "")
    nbMaxRecord = nbr as Integer

    if (batch.getReferenceId().isPresent()) {
      referenceId = "test"
      Optional<String> data = getJobData(batch.getReferenceId().get())
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
  // Perform actual job
  private performActualJob(Optional<String> data) {
    if (!data.isPresent()) {
      logMessage("ERROR", "No job data found")
      return
    }
    rawData = data.get()
    String inASCD = getFirstParameter()
    String inCUNO = getNextParameter()
    String inFDAT = getNextParameter()
    String inITNO = getNextParameter()
    String inOPT2 = getNextParameter()
    String inFVDT = getNextParameter()
    String inLEVL = getNextParameter()
    logMessage("DEBUG", "start job EXT022 ascd:${ascd} cuno:${cuno} fdat:${fdat} itno:${itno} opt2:${inOPT2} fvdt:${fvdt} loglevel:${inLEVL}")

    loglevel = inLEVL.length() == 0 ? "WARN" : inLEVL.trim()
    if (!LOGLEVELS.contains(loglevel)) {
      String message = "Niveau de log incorrect ${loglevel}"
      loglevel = "WARN"
      logMessage("ERROR", message)
    }

    LocalDateTime timeOfCreation = LocalDateTime.now()

    ascd = inASCD
    cuno = inCUNO

    if (inFDAT != null && !inFDAT.trim().isBlank()) {
      fdat = inFDAT
      if (!utility.call("DateUtil", "isDateValid", fdat, "yyyyMMdd")) {
        String message = "Date de début est incorrecte " + ";" + fdat
        logMessage("ERROR", message)
        return
      }
    } else {
      fdat = "0"
    }

    // Check selection header
    DBAction ext020Query = database.table("EXT020").index("00").build()
    DBContainer ext020Request = ext020Query.getContainer()
    ext020Request.set("EXCONO", currentCompany)
    ext020Request.set("EXASCD", ascd)
    ext020Request.set("EXCUNO", cuno)
    ext020Request.setInt("EXFDAT", fdat as Integer)
    if (!ext020Query.read(ext020Request)) {
      String message = "Entête sélection n'existe pas ${ascd} ${cuno} ${fdat}"
      logMessage("ERROR", message)
      return
    }

    // Check option
    if (inOPT2 == null && !inOPT2.trim().isBlank()) {
      String message = "Option est obligatoire"
      logMessage("ERROR", message)
      return
    }

    if (inOPT2 != "1" && inOPT2 != "2") {
      String opt2 = inOPT2
      String header = "MSG"
      String message = "Option ${opt2} est invalide"
      logMessage("ERROR", message)
      return
    }

    if (inFVDT != null && !inFVDT.trim().isBlank()) {
      fvdt = inFVDT
      if (!utility.call("DateUtil", "isDateValid", fvdt, "yyyyMMdd")) {
        String header = "MSG"
        String message = "Date de début de validité ${fvdt} est incorrecte"
        logMessage("ERROR", message)
        return
      }
    } else {
      fvdt = "0"
    }

    constraintCSCD = ""
    DBAction ocusmaQuery = database.table("OCUSMA").index("00").selection("OKCSCD").build()
    DBContainer ocusmaRequest = ocusmaQuery.getContainer()
    ocusmaRequest.set("OKCONO", currentCompany)
    ocusmaRequest.set("OKCUNO", cuno)
    if (ocusmaQuery.read(ocusmaRequest)) {
      constraintCSCD = ocusmaRequest.getString("OKCSCD")
    }
    // Check criteria used in the selection
    checkUsedCriteria()

    if (criteriaFound) {
      updateCUGEX1("10", "" + itemsInOASITN)

      // Delete file EXT022
      deleteEXT022()

      ExpressionFactory ext010Expression = database.getExpressionFactory("EXT010")
      ext010Expression = ext010Expression.le("EXFVDT", timeOfCreation.format(DateTimeFormatter.ofPattern("yyyyMMdd")))
      ext010Expression = ext010Expression.and(ext010Expression.ge("EXLVDT", timeOfCreation.format(DateTimeFormatter.ofPattern("yyyyMMdd"))))
      if (!inITNO.trim().isEmpty()){
        ext010Expression = ext010Expression.and(ext010Expression.ge("EXITNO", inITNO.trim()))
      }

      DBAction ext010Query = database.table("EXT010").index("02").matching(ext010Expression).selection("EXCUNO", "EXITNO", "EXCMDE", "EXFVDT", "EXLVDT", "EXSULE", "EXSULD").build()
      DBContainer ext010Request = ext010Query.getContainer()
      ext010Request.set("EXCONO", currentCompany)
      ext010Request.set("EXCUNO", cuno)
      //no limitation in readAll we have to read all ext010 records for the customer
      if (!ext010Query.readAll(ext010Request, 2, nbMaxRecord, ext010Reader)) {
      }
      updateCUGEX1("90", "" + itemsInOASITN)
    } else {
      logMessage("ERROR", "Pas de critères trouvés pour la selection ascd:${ascd} cuno:${cuno} fdat:${fdat}")
    }
    // Delete file EXTJOB
    deleteEXTJOB()

    //launch new job
    if (countExt010Records == nbMaxRecord) {
      executeEXT820MISubmitBatch(currentCompany as String, "EXT022", inASCD, inCUNO, inFDAT, lastItno, inOPT2, inFVDT, inLEVL, "")
    }
  }
  /**
   * Check used criteria contained in EXT021 table
   */
  public void checkUsedCriteria() {
    criteriaFound = false
    DBAction ext021Query = database.table("EXT021").index("00").selection("EXCHB1").build()
    DBContainer ext021Request = ext021Query.getContainer()
    ext021Request.set("EXCONO", currentCompany)
    ext021Request.set("EXASCD", ascd)
    ext021Request.set("EXCUNO", cuno)
    ext021Request.set("EXFDAT", fdat as Integer)
    // Initialization of a boolean for each criteria: 0 = not used, 1 = inclusion, 2 = exclusion
    if (!ext021Query.readAll(ext021Request, 4, 10000, outDataEXT021)) {
    }
  }
  // Retrieve MITMAS
  Closure<?> outDataMITMAS = { DBContainer MITMAS ->
    // Get item criteria value
    itno = MITMAS.get("MMITNO")
    // Get item type value
    stat = MITMAS.get("MMSTAT")
    prod = MITMAS.get("MMPROD")
    hie1 = MITMAS.get("MMHIE1")
    hie2 = MITMAS.get("MMHIE2")
    hie3 = MITMAS.get("MMHIE3")
    hie4 = MITMAS.get("MMHIE4")
    hie5 = MITMAS.get("MMHIE5")
    buar = MITMAS.get("MMBUAR")
    cfi1 = MITMAS.get("MMCFI1")
    cfi2 = MITMAS.get("MMCFI2")
    spe1 = MITMAS.get("MMSPE1")
    if (cfi2.trim() != "") {
    }
    sldy = 0
    if (spe1.trim() != "") {
      sldy = spe1.trim() as Integer
    }
    hazi = MITMAS.get("MMHAZI")
    cfi4 = MITMAS.get("MMCFI4")
    suno = MITMAS.get("MMSUNO")

    zali = 0
    if (MITMAS.get("MMITGR") == "ALIM") {
      zali = 1
    }

    // Get the value for the other criteria used in the selection
    logger.debug("#FLB - getCriteriaValue ${itno}")
    getCriteriaValue()

    logger.debug("#FLB - itemSelectionOK  ${itno}")
    boolean itemOk = itemSelectionOK()
    logMessage("DEBUG", "Item GO selection : ITNO:${itno}")
    if (!itemOk) {
      logMessage("DEBUG", "ItemKO selection : ITNO:${itno} ok:${itemOk}")
    }

    // Check if the item matches the selection
    logger.debug("#FLB - itemSelectionOK  ${itno}")
    if ((itemOk)) {
      boolean contOK = constraintsOK()
      if (!contOK) {
        logMessage("DEBUG", "ItemKO qualité : ITNO:${itno} ok:${contOK}")
      }
      if (contOK) {
        LocalDateTime timeOfCreation = LocalDateTime.now()
        DBAction query = database.table("EXT022").index("00").build()
        DBContainer EXT022 = query.getContainer()
        EXT022.set("EXCONO", currentCompany)
        EXT022.set("EXASCD", ascd)
        EXT022.set("EXCUNO", cuno)
        EXT022.set("EXFDAT", fdat as Integer)
        EXT022.set("EXITNO", itno)
        if (!query.read(EXT022)) {
          EXT022.setInt("EXRGDT", timeOfCreation.format(DateTimeFormatter.ofPattern("yyyyMMdd")) as Integer)
          EXT022.setInt("EXRGTM", timeOfCreation.format(DateTimeFormatter.ofPattern("HHmmss")) as Integer)
          EXT022.setInt("EXLMDT", timeOfCreation.format(DateTimeFormatter.ofPattern("yyyyMMdd")) as Integer)
          EXT022.setInt("EXCHNO", 1)
          EXT022.set("EXCHID", program.getUser())
          query.insert(EXT022)
          DBAction queryOasitn = database.table("OASITN").index("00").build()
          DBContainer containerOasitn = queryOasitn.getContainer()
          containerOasitn.set("OICONO", currentCompany)
          containerOasitn.set("OIASCD", ascd)
          containerOasitn.set("OIITNO", itno)
          containerOasitn.set("OIFDAT", fdat as Integer)
          if (!queryOasitn.read(containerOasitn)) {
            executeCRS105MIAddAssmItem(ascd, itno, fdat)
          }
          itemsInOASITN++
        }
      }
    }
  }


  // Execute CRS105MI.AddAssmItem
  private executeCRS105MIAddAssmItem(String pAscd, String pItno, String pFdat){
    Map<String, String> parameters = ["ASCD": pAscd, "ITNO": pItno, "FDAT": pFdat]
    Closure<?> handler = { Map<String, String> response ->
      if (response.error != null) {
        logger.debug("#PB CRS105MI.AddAssmItem error :" + response.errorMessage)
        String header = "MSG"
        String message = "Failed CRS105MI.AddAssmItem: " + response.errorMessage
        logMessage(header, message)
        return
      } else {
      }
    }
    //Search Item exclusion
    boolean exclu = false
    ExpressionFactory expressionExt025 = database.getExpressionFactory("EXT025")
    expressionExt025 = expressionExt025.le("EXFDAT", pFdat)

    DBAction ext025Query = database.table("EXT025").index("00").matching(expressionExt025).selection("EXCONO", "EXITNO", "EXCUNO", "EXFDAT", "EXRGDT", "EXRGTM", "EXLMDT", "EXCHNO", "EXCHID").build()
    DBContainer ext025Request = ext025Query.getContainer()
    ext025Request.set("EXCONO", currentCompany)
    ext025Request.set("EXCUNO", cuno)
    ext025Request.set("EXITNO", pItno)

    Closure<?> ext025Reader = { DBContainer ext025Result ->
      exclu = true
    }

    if(!ext025Query.readAll(ext025Request, 3, 1, ext025Reader)){
    }
    if (!exclu) {
      miCaller.call("CRS105MI", "AddAssmItem", parameters, handler)
    }
  }

  /**
   * Delete records related to the assortment from EXT022 table
   */
  public void deleteEXT022() {
    DBAction ext022Query = database.table("EXT022").index("00").build()
    DBContainer ext022Request = ext022Query.getContainer()
    ext022Request.set("EXCONO", currentCompany)
    ext022Request.set("EXASCD", ascd)
    ext022Request.set("EXCUNO", cuno)
    ext022Request.set("EXFDAT", fdat as Integer)
    // Delete EXTJOB
    Closure<?> ext022Updater = { LockedResult ext022LockedResult ->
      ext022LockedResult.delete()
    }
    if (!ext022Query.readAllLock(ext022Request, 4, ext022Updater)) {
    }
  }
  /**
   * Retrieve criterias values
   */
  public void getCriteriaValue() {
    ulty = ""
    cpfx = ""
    if (iULTY != 0 || iCPFX != 0) {
      ExpressionFactory expressionCUGEX1 = database.getExpressionFactory("CUGEX1")
      DBAction queryCUGEX1 = database.table("CUGEX1").index("00").matching(expressionCUGEX1).selection("F1A030", "F1A830").build()
      DBContainer CUGEX1 = queryCUGEX1.getContainer()
      CUGEX1.set("F1CONO", currentCompany)
      CUGEX1.set("F1FILE", "MITMAS")
      CUGEX1.set("F1PK01", itno)
      if (!queryCUGEX1.readAll(CUGEX1, 3, nbMaxRecord, outDataCUGEX1)) {
      }
    }
    cscd = ""
    csno = ""
    //if(CSCD != 0 || CSNO != 0) {
    LocalDateTime timeOfCreation = LocalDateTime.now()
    DBAction query = database.table("MITFAC").index("00").selection("M9ORCO", "M9CSNO").build()
    DBContainer MITFAC = query.getContainer()
    MITFAC.set("M9CONO", currentCompany)
    MITFAC.set("M9FACI", "E10")
    MITFAC.set("M9ITNO", itno)
    if (query.read(MITFAC)) {
      cscd = MITFAC.get("M9ORCO")
      csno = MITFAC.get("M9CSNO")
      csno = csno.trim()
    }
    //}
    popn = ""
    //if(POPN != 0){
    ExpressionFactory expressionMITPOP = database.getExpressionFactory("MITPOP")
    expressionMITPOP = expressionMITPOP.eq("MPREMK", "SIGMA6")
    DBAction queryMITPOP = database.table("MITPOP").index("00").matching(expressionMITPOP).selection("MPPOPN").build()
    DBContainer MITPOP = queryMITPOP.getContainer()
    MITPOP.set("MPCONO", currentCompany)
    MITPOP.set("MPALWT", 1)
    MITPOP.set("MPALWQ", "")
    MITPOP.set("MPITNO", itno)
    if (!queryMITPOP.readAll(MITPOP, 4, 1, outDataMITPOP)) {
    }
    //}
    zalc = 0
    zsan = 0
    zcap = ""
    zca1 = ""
    zca2 = ""
    zca3 = ""
    zca4 = ""
    zca5 = ""
    zca6 = ""
    zca7 = ""
    zca8 = ""
    zori = 0
    zphy = 0
    zagr = 0
    zohf = 0
    DBAction queryEXT032 = database.table("EXT032").index("00").selection("EXZALC", "EXZSAN", "EXZCA1", "EXZCA2", "EXZCA3", "EXZCA4", "EXZCA5", "EXZCA6", "EXZCA7", "EXZCA8", "EXZORI", "EXZPHY", "EXZAGR").build()
    DBContainer EXT032 = queryEXT032.getContainer()
    EXT032.set("EXCONO", currentCompany)
    EXT032.set("EXPOPN", popn)
    EXT032.set("EXSUNO", suno)
    EXT032.set("EXORCO", cscd)
    if (queryEXT032.read(EXT032)) {
      zalc = EXT032.get("EXZALC")
      zsan = EXT032.get("EXZSAN")
      zcap = EXT032.get("EXZCA1")
      zca1 = EXT032.get("EXZCA1")
      zca2 = EXT032.get("EXZCA2")
      zca3 = EXT032.get("EXZCA3")
      zca4 = EXT032.get("EXZCA4")
      zca5 = EXT032.get("EXZCA5")
      zca6 = EXT032.get("EXZCA6")
      zca7 = EXT032.get("EXZCA7")
      zca8 = EXT032.get("EXZCA8")
      zori = EXT032.get("EXZORI")
      zphy = EXT032.get("EXZPHY")
      zagr = EXT032.get("EXZAGR")
    }
    znag = ""
    DBAction cugex1CidmasQuery = database.table("CUGEX1").index("00").selection("F1A030").build()
    DBContainer cugex1Cidmas = cugex1CidmasQuery.getContainer()
    cugex1Cidmas.set("F1CONO", currentCompany)
    cugex1Cidmas.set("F1FILE", "CIDMAS")
    if (prod.trim() != "") {
      cugex1Cidmas.set("F1PK01", prod)
    } else {
      cugex1Cidmas.set("F1PK01", suno)
    }
    cugex1Cidmas.set("F1PK02", "")
    cugex1Cidmas.set("F1PK03", "")
    cugex1Cidmas.set("F1PK04", "")
    cugex1Cidmas.set("F1PK05", "")
    cugex1Cidmas.set("F1PK06", "")
    cugex1Cidmas.set("F1PK07", "")
    cugex1Cidmas.set("F1PK08", "")
    if (cugex1CidmasQuery.read(cugex1Cidmas)) {
      znag = cugex1Cidmas.get("F1A030")
    }
  }
  /**
   * Return true if the item matches the selection criteria and not if it does not
   */
  public boolean itemSelectionOK() {
    // Status must be greater than or equal to 80
    if (stat >= "80")
      return false

    // Check double
    if(!checkDouble(cuno, itno, ext010Fvdt as String, ext010Lvdt as String)){
      return false
    }

    //chech sule / suld
    if (sule.length() == 0 && suld.length() == 0){
      return false
    }

    //chech suno / itno
    if(sule.length()>0){
      if(!checkSunoItno(sule, itno)){
        return false
      }
    } else {
      if(!checkSunoItno(suld, itno)){
        return false
      }
    }

    //check price
    price = 0
    if(sule.length()>0){
      executePPS106MIGetPrice(itno, sule)
    } else {
      executePPS106MIGetPrice(itno, suld)
    }
    if(price==0){
      return false
    }

    // If global assortment is selected, selection is ok for all the items
    if (iGOLD == 1)
      return true

    DBAction queryEXT021 = database.table("EXT021").index("00").selection("EXCHB1").build()
    DBContainer EXT021 = queryEXT021.getContainer()
    EXT021.set("EXCONO", currentCompany)
    EXT021.set("EXASCD", ascd)
    EXT021.set("EXCUNO", cuno)
    EXT021.set("EXFDAT", fdat as Integer)

    // cfi2 must be greater or equal than the criteria
    if (iCFI2 != 0) {
      data = ""
      EXT021.set("EXTYPE", "CFI2")
      if (!queryEXT021.readAll(EXT021, 5, 1, outDataEXT0212)) {
      }

      if ((cfi2.trim() as double) >= (data.trim() as double)) {
        if (iCFI2 == 2) return false
      } else {
        if (iCFI2 == 1) return false
      }
    }

    if (iITNO != 0) {
      EXT021.set("EXTYPE", "ITNO")
      EXT021.set("EXDATA", itno)
      if (queryEXT021.read(EXT021)) {
        if (iITNO == 2) return false
      } else {
        if (iITNO == 1) return false
      }
    }
    if (iSUNO != 0) {
      if (iSUNO == 2 && sule.trim() == "")
        return false

      EXT021.set("EXTYPE", "SUNO")
      EXT021.set("EXDATA", sule)
      if (queryEXT021.read(EXT021)) {
        if (iSUNO == 2) return false
      } else {
        if (iSUNO == 1) return false
      }
    }
    if (iPROD != 0) {
      if (iPROD == 2 && suno.trim() == "")
        return false

      EXT021.set("EXTYPE", "PROD")
      EXT021.set("EXDATA", suno)
      if (queryEXT021.read(EXT021)) {
        if (iPROD == 2) return false
      } else {
        if (iPROD == 1) return false
      }
    }
    if (iHIE1 != 0) {
      EXT021.set("EXTYPE", "HIE1")
      EXT021.set("EXDATA", hie1)
      if (queryEXT021.read(EXT021)) {
        if (iHIE1 == 2) return false
      } else {
        if (iHIE1 == 1) return false
      }
    }
    if (iHIE2 != 0) {
      EXT021.set("EXTYPE", "HIE2")
      EXT021.set("EXDATA", hie2)
      if (queryEXT021.read(EXT021)) {
        if (iHIE2 == 2) return false
      } else {
        if (iHIE2 == 1) return false
      }
    }
    if (iHIE3 != 0) {
      EXT021.set("EXTYPE", "HIE3")
      EXT021.set("EXDATA", hie3)
      if (queryEXT021.read(EXT021)) {
        if (iHIE3 == 2) return false
      } else {
        if (iHIE3 == 1) return false
      }
    }
    if (iHIE4 != 0) {
      EXT021.set("EXTYPE", "HIE4")
      EXT021.set("EXDATA", hie4)
      if (queryEXT021.read(EXT021)) {
        if (iHIE4 == 2) return false
      } else {
        if (iHIE4 == 1) return false
      }
    }
    if (iHIE5 != 0) {
      EXT021.set("EXTYPE", "HIE5")
      EXT021.set("EXDATA", hie5)
      if (queryEXT021.read(EXT021)) {
        if (iHIE5 == 2) return false
      } else {
        if (iHIE5 == 1) return false
      }
    }
    if (iBUAR != 0) {
      EXT021.set("EXTYPE", "BUAR")
      EXT021.set("EXDATA", buar)
      if (queryEXT021.read(EXT021)) {
        if (iBUAR == 2) return false
      } else {
        if (iBUAR == 1) return false
      }
    }
    if (iCFI1 != 0) {
      EXT021.set("EXTYPE", "CFI1")
      EXT021.set("EXDATA", cfi1)
      if (queryEXT021.read(EXT021)) {
        if (iCFI1 == 2) return false
      } else {
        if (iCFI1 == 1) return false
      }
    }
    if (iCSCD != 0) {
      EXT021.set("EXTYPE", "CSCD")
      EXT021.set("EXDATA", cscd)
      if (queryEXT021.read(EXT021)) {
        if (iCSCD == 2) return false
      } else {
        if (iCSCD == 1) return false
      }
    }
    if (iCSNO != 0) {
      EXT021.set("EXTYPE", "CSNO")
      EXT021.set("EXDATA", csno)
      if (queryEXT021.read(EXT021)) {
        if (iCSNO == 2) return false
      } else {
        if (iCSNO == 1) return false
      }
    }
    if (iPOPN != 0) {
      EXT021.set("EXTYPE", "POPN")
      EXT021.set("EXDATA", popn)
      if (queryEXT021.read(EXT021)) {
        if (iPOPN == 2) return false
      } else {
        if (iPOPN == 1) return false
      }
    }
    if (iULTY != 0) {
      EXT021.set("EXTYPE", "ULTY")
      EXT021.set("EXDATA", ulty)
      if (queryEXT021.read(EXT021)) {
        if (iULTY == 2) return false
      } else {
        if (iULTY == 1) return false
      }
    }
    // sldy must be greater or equal than the criteria
    if (iSLDY != 0) {
      data = ""
      EXT021.set("EXTYPE", "SLDY")
      if (!queryEXT021.readAll(EXT021, 5, 1, outDataEXT0212)) {
      }

      if ((sldy.trim() as double) >= (data.trim() as double)) {
        if (iSLDY == 2) return false
      } else {
        if (iSLDY == 1) return false
      }
    }
    if (iCPFX != 0) {
      EXT021.set("EXTYPE", "CPFX")
      EXT021.set("EXDATA", cpfx)
      if (queryEXT021.read(EXT021)) {
        if (iCPFX == 2) return false
      } else {
        if (iCPFX == 1) return false
      }
    }
    if (iCMDE != 0) {
      EXT021.set("EXTYPE", "CMDE")
      EXT021.set("EXDATA", cmde)
      if (queryEXT021.read(EXT021)) {
        if (iCMDE == 2) return false
      } else {
        if (iCMDE == 1) return false
      }
    }
    return true
  }

  /**
   * Read information from DB EXT010
   * Double check
   *
   * @parameter Customer, Item, From date, To date
   * @return true if ok false otherwise
   * */
  private boolean checkDouble(String cuno, String itno, String fvdt, String lvdt){
    boolean errorIndicator = false
    ExpressionFactory ext010Expression1 = database.getExpressionFactory("EXT010")
    ext010Expression1 = (ext010Expression1.ge("EXFVDT", fvdt)).and(ext010Expression1.le("EXFVDT", lvdt))
    DBAction ext010Double1 = database.table("EXT010")
      .index("02")
      .matching(ext010Expression1)
      .selection("EXFVDT", "EXLVDT")
      .build()
    DBContainer containerExt0101 = ext010Double1.getContainer()
    containerExt0101.set("EXCONO", currentCompany)
    containerExt0101.set("EXCUNO", cuno)
    containerExt0101.set("EXITNO", itno)
    Closure<?> outEXT0101 = { DBContainer EXT0101result ->
      errorIndicator = true
    }
    if (!ext010Double1.readAll(containerExt0101, 3, 1, outEXT0101)) {
    }
    if(errorIndicator){
      return false
    }
    ExpressionFactory ext010Expression2 = database.getExpressionFactory("EXT010")
    ext010Expression2 = (ext010Expression2.ge("EXLVDT", fvdt)).and(ext010Expression2.le("EXLVDT", lvdt))
    DBAction ext010Double2 = database.table("EXT010")
      .index("02")
      .matching(ext010Expression2)
      .selection("EXFVDT", "EXLVDT")
      .build()
    DBContainer containerExt0102 = ext010Double2.getContainer()
    containerExt0102.set("EXCONO", currentCompany)
    containerExt0102.set("EXCUNO", cuno)
    containerExt0102.set("EXITNO", itno)
    Closure<?> outEXT0102 = { DBContainer EXT0101result ->
      errorIndicator = true
    }
    if (!ext010Double2.readAll(containerExt0102, 3, 1, outEXT0102)) {
    }
    if(errorIndicator){
      return false
    }
    return true
  }

  /**
   * Read information from DB EXT010
   * Double check
   *
   * @parameter Customer, Item, From date, To date
   * @return true if ok false otherwise
   * */
  private boolean checkSunoItno(String suno, String itno){
    boolean found = false
    DBAction mitvenQuery = database.table("MITVEN")
      .index("10")
      .selection("IFSITE")
      .build()
    DBContainer containerMitven = mitvenQuery.getContainer()
    containerMitven.set("IFCONO", currentCompany)
    containerMitven.set("IFSUNO", suno)
    containerMitven.set("IFITNO", itno)
    Closure<?> outMitven = { DBContainer mitvenResult ->
      found = true
    }
    if (!mitvenQuery.readAll(containerMitven, 3, 1, outMitven)) {
    }
    if(found){
      return true
    }
    return false
  }

  /**
   * Execute PPS106MI.GetPrice
   *
   * @parameter Item, supplier
   * @return price
   * */
  private executePPS106MIGetPrice(String ITNO, String SUNO) {
    Map<String, String> parameters = ["ITNO": ITNO, "SUNO": SUNO, "ORQA": "1"]
    Closure<?> handler = { Map<String, String> response ->
      if (response.error != null) {
        logger.debug("#PB PPS106MI.GetPrice error :" + response.errorMessage)
        String header = "MSG"
        String message = "Failed PPS106MI.GetPrice: " + response.errorMessage
        logMessage(header, message)
        return
      } else {
      }
      if (response.PUPR != null)
        price = response.PUPR as double
    }
    miCaller.call("PPS106MI", "GetPrice", parameters, handler)
  }

  /**
   * Return true if no blocking constraint is found for the item
   */
  public boolean constraintsOK() {
    constraintIsOK = true
    ExpressionFactory expressionEXT030 = database.getExpressionFactory("EXT030")
    expressionEXT030 = (expressionEXT030.eq("EXCUNO", cuno)).or(expressionEXT030.eq("EXCUNO", ""))
    if (cuno == "") {
      expressionEXT030 = (expressionEXT030.eq("EXCSCD", constraintCSCD)).or(expressionEXT030.eq("EXCSCD", ""))
    } else {
      expressionEXT030 = expressionEXT030.and((expressionEXT030.eq("EXCSCD", constraintCSCD)).or(expressionEXT030.eq("EXCSCD", "")))
    }
    if (cuno == "" && constraintCSCD == "") {
      expressionEXT030 = (expressionEXT030.eq("EXHAZI", hazi as String)).or(expressionEXT030.eq("EXHAZI", "2"))
    } else {
      expressionEXT030 = expressionEXT030.and((expressionEXT030.eq("EXHAZI", hazi as String)).or(expressionEXT030.eq("EXHAZI", "2")))
    }
    if (hie5 != "") {
      if (cuno == "" && constraintCSCD == "" && hazi == 0) {
        expressionEXT030 = (expressionEXT030.eq("EXHIE0", hie5)).or(expressionEXT030.eq("EXHIE0", hie5.substring(0, 2) + "*")).or(expressionEXT030.eq("EXHIE0", hie5.substring(0, 4) + "*")).or(expressionEXT030.eq("EXHIE0", hie5.substring(0, 7) + "*")).or(expressionEXT030.eq("EXHIE0", hie5.substring(0, 9) + "*")).or(expressionEXT030.eq("EXHIE0", hie5.substring(0, 11) + "*")).or(expressionEXT030.eq("EXHIE0", ""))
      } else {
        expressionEXT030 = expressionEXT030.and((expressionEXT030.eq("EXHIE0", hie5)).or(expressionEXT030.eq("EXHIE0", hie5.substring(0, 2) + "*")).or(expressionEXT030.eq("EXHIE0", hie5.substring(0, 4) + "*")).or(expressionEXT030.eq("EXHIE0", hie5.substring(0, 7) + "*")).or(expressionEXT030.eq("EXHIE0", hie5.substring(0, 9) + "*")).or(expressionEXT030.eq("EXHIE0", hie5.substring(0, 11) + "*")).or(expressionEXT030.eq("EXHIE0", "")))
      }
    } else {
      if (cuno == "" && constraintCSCD == "" && hazi == 0) {
        expressionEXT030 = (expressionEXT030.eq("EXHIE0", hie5)).or(expressionEXT030.eq("EXHIE0", ""))
      } else {
        expressionEXT030 = expressionEXT030.and((expressionEXT030.eq("EXHIE0", hie5)).or(expressionEXT030.eq("EXHIE0", "")))
      }
    }
    if (cuno == "" && constraintCSCD == "" && hazi == 0 && hie5 == "") {
      expressionEXT030 = (expressionEXT030.eq("EXCFI4", cfi4)).or(expressionEXT030.eq("EXCFI4", ""))
    } else {
      expressionEXT030 = expressionEXT030.and((expressionEXT030.eq("EXCFI4", cfi4)).or(expressionEXT030.eq("EXCFI4", "")))
    }
    if (cuno == "" && constraintCSCD == "" && hazi == 0 && hie5 == "" && cfi4 == "") {
      expressionEXT030 = (expressionEXT030.eq("EXPOPN", popn)).or(expressionEXT030.eq("EXPOPN", ""))
    } else {
      expressionEXT030 = expressionEXT030.and((expressionEXT030.eq("EXPOPN", popn)).or(expressionEXT030.eq("EXPOPN", "")))
    }
    if (csno != "") {
      if (cuno == "" && constraintCSCD == "" && hazi == 0 && hie5 == "" && cfi4 == "" && popn == "") {
        if (csno.toString().length() == 16)
          expressionEXT030 = (expressionEXT030.eq("EXCSNO", csno)).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 1) + "*")).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 2) + "*")).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 3) + "*")).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 4) + "*")).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 5) + "*")).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 6) + "*")).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 7) + "*")).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 8) + "*")).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 9) + "*")).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 10) + "*")).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 11) + "*")).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 12) + "*")).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 13) + "*")).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 14) + "*")).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 15) + "*")).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 16) + "*")).or(expressionEXT030.eq("EXCSNO", ""))
        if (csno.toString().length() == 15)
          expressionEXT030 = (expressionEXT030.eq("EXCSNO", csno)).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 1) + "*")).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 2) + "*")).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 3) + "*")).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 4) + "*")).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 5) + "*")).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 6) + "*")).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 7) + "*")).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 8) + "*")).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 9) + "*")).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 10) + "*")).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 11) + "*")).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 12) + "*")).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 13) + "*")).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 14) + "*")).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 15) + "*")).or(expressionEXT030.eq("EXCSNO", ""))
        if (csno.toString().length() == 14)
          expressionEXT030 = (expressionEXT030.eq("EXCSNO", csno)).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 1) + "*")).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 2) + "*")).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 3) + "*")).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 4) + "*")).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 5) + "*")).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 6) + "*")).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 7) + "*")).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 8) + "*")).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 9) + "*")).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 10) + "*")).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 11) + "*")).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 12) + "*")).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 13) + "*")).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 14) + "*")).or(expressionEXT030.eq("EXCSNO", ""))
        if (csno.toString().length() == 13)
          expressionEXT030 = (expressionEXT030.eq("EXCSNO", csno)).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 1) + "*")).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 2) + "*")).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 3) + "*")).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 4) + "*")).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 5) + "*")).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 6) + "*")).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 7) + "*")).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 8) + "*")).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 9) + "*")).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 10) + "*")).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 11) + "*")).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 12) + "*")).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 13) + "*")).or(expressionEXT030.eq("EXCSNO", ""))
        if (csno.toString().length() == 12)
          expressionEXT030 = (expressionEXT030.eq("EXCSNO", csno)).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 1) + "*")).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 2) + "*")).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 3) + "*")).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 4) + "*")).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 5) + "*")).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 6) + "*")).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 7) + "*")).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 8) + "*")).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 9) + "*")).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 10) + "*")).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 11) + "*")).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 12) + "*")).or(expressionEXT030.eq("EXCSNO", ""))
        if (csno.toString().length() == 11)
          expressionEXT030 = (expressionEXT030.eq("EXCSNO", csno)).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 1) + "*")).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 2) + "*")).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 3) + "*")).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 4) + "*")).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 5) + "*")).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 6) + "*")).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 7) + "*")).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 8) + "*")).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 9) + "*")).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 10) + "*")).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 11) + "*")).or(expressionEXT030.eq("EXCSNO", ""))
        if (csno.toString().length() == 10)
          expressionEXT030 = (expressionEXT030.eq("EXCSNO", csno)).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 1) + "*")).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 2) + "*")).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 3) + "*")).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 4) + "*")).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 5) + "*")).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 6) + "*")).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 7) + "*")).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 8) + "*")).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 9) + "*")).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 10) + "*")).or(expressionEXT030.eq("EXCSNO", ""))
        if (csno.toString().length() == 9)
          expressionEXT030 = (expressionEXT030.eq("EXCSNO", csno)).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 1) + "*")).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 2) + "*")).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 3) + "*")).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 4) + "*")).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 5) + "*")).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 6) + "*")).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 7) + "*")).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 8) + "*")).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 9) + "*")).or(expressionEXT030.eq("EXCSNO", ""))
        if (csno.toString().length() == 8)
          expressionEXT030 = (expressionEXT030.eq("EXCSNO", csno)).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 1) + "*")).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 2) + "*")).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 3) + "*")).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 4) + "*")).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 5) + "*")).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 6) + "*")).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 7) + "*")).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 8) + "*")).or(expressionEXT030.eq("EXCSNO", ""))
        if (csno.toString().length() == 7)
          expressionEXT030 = (expressionEXT030.eq("EXCSNO", csno)).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 1) + "*")).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 2) + "*")).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 3) + "*")).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 4) + "*")).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 5) + "*")).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 6) + "*")).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 7) + "*")).or(expressionEXT030.eq("EXCSNO", ""))
        if (csno.toString().length() == 6)
          expressionEXT030 = (expressionEXT030.eq("EXCSNO", csno)).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 1) + "*")).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 2) + "*")).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 3) + "*")).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 4) + "*")).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 5) + "*")).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 6) + "*")).or(expressionEXT030.eq("EXCSNO", ""))
        if (csno.toString().length() == 5)
          expressionEXT030 = (expressionEXT030.eq("EXCSNO", csno)).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 1) + "*")).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 2) + "*")).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 3) + "*")).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 4) + "*")).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 5) + "*")).or(expressionEXT030.eq("EXCSNO", ""))
        if (csno.toString().length() == 4)
          expressionEXT030 = (expressionEXT030.eq("EXCSNO", csno)).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 1) + "*")).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 2) + "*")).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 3) + "*")).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 4) + "*")).or(expressionEXT030.eq("EXCSNO", ""))
        if (csno.toString().length() == 3)
          expressionEXT030 = (expressionEXT030.eq("EXCSNO", csno)).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 1) + "*")).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 2) + "*")).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 3) + "*")).or(expressionEXT030.eq("EXCSNO", ""))
        if (csno.toString().length() == 2)
          expressionEXT030 = (expressionEXT030.eq("EXCSNO", csno)).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 1) + "*")).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 2) + "*")).or(expressionEXT030.eq("EXCSNO", ""))
        if (csno.toString().length() == 1)
          expressionEXT030 = (expressionEXT030.eq("EXCSNO", csno)).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 1) + "*")).or(expressionEXT030.eq("EXCSNO", ""))
      } else {
        if (csno.toString().length() == 16)
          expressionEXT030 = expressionEXT030.and((expressionEXT030.eq("EXCSNO", csno)).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 1) + "*")).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 2) + "*")).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 3) + "*")).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 4) + "*")).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 5) + "*")).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 6) + "*")).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 7) + "*")).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 8) + "*")).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 9) + "*")).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 10) + "*")).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 11) + "*")).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 12) + "*")).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 13) + "*")).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 14) + "*")).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 15) + "*")).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 16) + "*")).or(expressionEXT030.eq("EXCSNO", "")))
        if (csno.toString().length() == 15)
          expressionEXT030 = expressionEXT030.and((expressionEXT030.eq("EXCSNO", csno)).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 1) + "*")).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 2) + "*")).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 3) + "*")).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 4) + "*")).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 5) + "*")).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 6) + "*")).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 7) + "*")).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 8) + "*")).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 9) + "*")).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 10) + "*")).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 11) + "*")).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 12) + "*")).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 13) + "*")).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 14) + "*")).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 15) + "*")).or(expressionEXT030.eq("EXCSNO", "")))
        if (csno.toString().length() == 14)
          expressionEXT030 = expressionEXT030.and((expressionEXT030.eq("EXCSNO", csno)).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 1) + "*")).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 2) + "*")).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 3) + "*")).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 4) + "*")).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 5) + "*")).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 6) + "*")).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 7) + "*")).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 8) + "*")).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 9) + "*")).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 10) + "*")).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 11) + "*")).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 12) + "*")).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 13) + "*")).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 14) + "*")).or(expressionEXT030.eq("EXCSNO", "")))
        if (csno.toString().length() == 13)
          expressionEXT030 = expressionEXT030.and((expressionEXT030.eq("EXCSNO", csno)).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 1) + "*")).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 2) + "*")).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 3) + "*")).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 4) + "*")).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 5) + "*")).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 6) + "*")).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 7) + "*")).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 8) + "*")).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 9) + "*")).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 10) + "*")).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 11) + "*")).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 12) + "*")).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 13) + "*")).or(expressionEXT030.eq("EXCSNO", "")))
        if (csno.toString().length() == 12)
          expressionEXT030 = expressionEXT030.and((expressionEXT030.eq("EXCSNO", csno)).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 1) + "*")).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 2) + "*")).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 3) + "*")).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 4) + "*")).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 5) + "*")).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 6) + "*")).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 7) + "*")).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 8) + "*")).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 9) + "*")).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 10) + "*")).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 11) + "*")).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 12) + "*")).or(expressionEXT030.eq("EXCSNO", "")))
        if (csno.toString().length() == 11)
          expressionEXT030 = expressionEXT030.and((expressionEXT030.eq("EXCSNO", csno)).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 1) + "*")).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 2) + "*")).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 3) + "*")).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 4) + "*")).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 5) + "*")).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 6) + "*")).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 7) + "*")).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 8) + "*")).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 9) + "*")).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 10) + "*")).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 11) + "*")).or(expressionEXT030.eq("EXCSNO", "")))
        if (csno.toString().length() == 10)
          expressionEXT030 = expressionEXT030.and((expressionEXT030.eq("EXCSNO", csno)).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 1) + "*")).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 2) + "*")).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 3) + "*")).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 4) + "*")).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 5) + "*")).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 6) + "*")).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 7) + "*")).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 8) + "*")).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 9) + "*")).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 10) + "*")).or(expressionEXT030.eq("EXCSNO", "")))
        if (csno.toString().length() == 9)
          expressionEXT030 = expressionEXT030.and((expressionEXT030.eq("EXCSNO", csno)).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 1) + "*")).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 2) + "*")).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 3) + "*")).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 4) + "*")).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 5) + "*")).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 6) + "*")).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 7) + "*")).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 8) + "*")).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 9) + "*")).or(expressionEXT030.eq("EXCSNO", "")))
        if (csno.toString().length() == 8)
          expressionEXT030 = expressionEXT030.and((expressionEXT030.eq("EXCSNO", csno)).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 1) + "*")).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 2) + "*")).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 3) + "*")).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 4) + "*")).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 5) + "*")).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 6) + "*")).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 7) + "*")).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 8) + "*")).or(expressionEXT030.eq("EXCSNO", "")))
        if (csno.toString().length() == 7)
          expressionEXT030 = expressionEXT030.and((expressionEXT030.eq("EXCSNO", csno)).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 1) + "*")).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 2) + "*")).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 3) + "*")).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 4) + "*")).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 5) + "*")).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 6) + "*")).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 7) + "*")).or(expressionEXT030.eq("EXCSNO", "")))
        if (csno.toString().length() == 6)
          expressionEXT030 = expressionEXT030.and((expressionEXT030.eq("EXCSNO", csno)).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 1) + "*")).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 2) + "*")).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 3) + "*")).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 4) + "*")).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 5) + "*")).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 6) + "*")).or(expressionEXT030.eq("EXCSNO", "")))
        if (csno.toString().length() == 5)
          expressionEXT030 = expressionEXT030.and((expressionEXT030.eq("EXCSNO", csno)).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 1) + "*")).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 2) + "*")).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 3) + "*")).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 4) + "*")).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 5) + "*")).or(expressionEXT030.eq("EXCSNO", "")))
        if (csno.toString().length() == 4)
          expressionEXT030 = expressionEXT030.and((expressionEXT030.eq("EXCSNO", csno)).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 1) + "*")).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 2) + "*")).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 3) + "*")).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 4) + "*")).or(expressionEXT030.eq("EXCSNO", "")))
        if (csno.toString().length() == 3)
          expressionEXT030 = expressionEXT030.and((expressionEXT030.eq("EXCSNO", csno)).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 1) + "*")).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 2) + "*")).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 3) + "*")).or(expressionEXT030.eq("EXCSNO", "")))
        if (csno.toString().length() == 2)
          expressionEXT030 = expressionEXT030.and((expressionEXT030.eq("EXCSNO", csno)).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 1) + "*")).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 2) + "*")).or(expressionEXT030.eq("EXCSNO", "")))
        if (csno.toString().length() == 1)
          expressionEXT030 = expressionEXT030.and((expressionEXT030.eq("EXCSNO", csno)).or(expressionEXT030.eq("EXCSNO", csno.substring(0, 1) + "*")).or(expressionEXT030.eq("EXCSNO", "")))
      }
    } else {
      if (cuno == "" && constraintCSCD == "" && hazi == 0 && hie5 == "" && cfi4 == "" && popn == "") {
        expressionEXT030 = (expressionEXT030.eq("EXCSNO", csno)).or(expressionEXT030.eq("EXCSNO", ""))
      } else {
        expressionEXT030 = expressionEXT030.and((expressionEXT030.eq("EXCSNO", csno)).or(expressionEXT030.eq("EXCSNO", "")))
      }
    }
    if (cuno == "" && constraintCSCD == "" && hazi == 0 && hie5 == "" && cfi4 == "" && popn == "" && csno == "") {
      expressionEXT030 = (expressionEXT030.eq("EXORCO", cscd)).or(expressionEXT030.eq("EXORCO", ""))
    } else {
      expressionEXT030 = expressionEXT030.and((expressionEXT030.eq("EXORCO", cscd)).or(expressionEXT030.eq("EXORCO", "")))
    }
    if (cuno == "" && constraintCSCD == "" && hazi == 0 && hie5 == "" && cfi4 == "" && popn == "" && csno == "" && cscd == "") {
      expressionEXT030 = (expressionEXT030.eq("EXZALC", zalc as String)).or(expressionEXT030.eq("EXZALC", "2"))
    } else {
      expressionEXT030 = expressionEXT030.and((expressionEXT030.eq("EXZALC", zalc as String)).or(expressionEXT030.eq("EXZALC", "2")))
    }
    if (cuno == "" && constraintCSCD == "" && hazi == 0 && hie5 == "" && cfi4 == "" && popn == "" && csno == "" && cscd == "" && zalc == 0) {
      expressionEXT030 = (expressionEXT030.eq("EXZSAN", zsan as String)).or(expressionEXT030.eq("EXZSAN", "2"))
    } else {
      expressionEXT030 = expressionEXT030.and((expressionEXT030.eq("EXZSAN", zsan as String)).or(expressionEXT030.eq("EXZSAN", "2")))
    }
    if (cuno == "" && constraintCSCD == "" && hazi == 0 && hie5 == "" && cfi4 == "" && popn == "" && csno == "" && cscd == "" && zalc == 0 && zsan == 0) {
      expressionEXT030 = ((expressionEXT030.eq("EXZCAP", zca1)).or(expressionEXT030.eq("EXZCAP", "")))
    } else {
      expressionEXT030 = expressionEXT030.and((expressionEXT030.eq("EXZCAP", zca1)).or(expressionEXT030.eq("EXZCAP", "")))
    }
    if (cuno == "" && constraintCSCD == "" && hazi == 0 && hie5 == "" && cfi4 == "" && popn == "" && csno == "" && cscd == "" && zalc == 0 && zsan == 0) {
      expressionEXT030 = ((expressionEXT030.eq("EXZCAS", zca1)).or(expressionEXT030.eq("EXZCAS", zca2)).or(expressionEXT030.eq("EXZCAS", zca3)).or(expressionEXT030.eq("EXZCAS", zca4)).or(expressionEXT030.eq("EXZCAS", zca5)).or(expressionEXT030.eq("EXZCAS", zca6)).or(expressionEXT030.eq("EXZCAS", zca7)).or(expressionEXT030.eq("EXZCAS", zca8)).or(expressionEXT030.eq("EXZCAS", ""))).or(expressionEXT030.eq("EXZCAS", ""))
    } else {
      expressionEXT030 = expressionEXT030.and((expressionEXT030.eq("EXZCAS", zca1)).or(expressionEXT030.eq("EXZCAS", zca2)).or(expressionEXT030.eq("EXZCAS", zca3)).or(expressionEXT030.eq("EXZCAS", zca4)).or(expressionEXT030.eq("EXZCAS", zca5)).or(expressionEXT030.eq("EXZCAS", zca6)).or(expressionEXT030.eq("EXZCAS", zca7)).or(expressionEXT030.eq("EXZCAS", zca8)).or(expressionEXT030.eq("EXZCAS", "")))
    }
    if (znag != "") {
      if (cuno == "" && constraintCSCD == "" && hazi == 0 && hie5 == "" && cfi4 == "" && popn == "" && csno == "" && cscd == "" && zalc == 0 && zsan == 0 && zca1 == "" && zca2 == "" && zca3 == "" && zca4 == "" && zca5 == "" && zca6 == "" && zca7 == "" && zca8 == "") {
        expressionEXT030 = (expressionEXT030.eq("EXZNAG", znag)).or(expressionEXT030.eq("EXZNAG", znag.substring(0, 4) + "*")).or(expressionEXT030.eq("EXZNAG", ""))
      } else {
        expressionEXT030 = expressionEXT030.and((expressionEXT030.eq("EXZNAG", znag)).or(expressionEXT030.eq("EXZNAG", znag.substring(0, 4) + "*")).or(expressionEXT030.eq("EXZNAG", "")))
      }
    } else {
      if (cuno == "" && constraintCSCD == "" && hazi == 0 && hie5 == "" && cfi4 == "" && popn == "" && csno == "" && cscd == "" && zalc == 0 && zsan == 0 && zca1 == "" && zca2 == "" && zca3 == "" && zca4 == "" && zca5 == "" && zca6 == "" && zca7 == "" && zca8 == "") {
        expressionEXT030 = (expressionEXT030.eq("EXZNAG", znag)).or(expressionEXT030.eq("EXZNAG", ""))
      } else {
        expressionEXT030 = expressionEXT030.and((expressionEXT030.eq("EXZNAG", znag)).or(expressionEXT030.eq("EXZNAG", "")))
      }
    }
    if (cuno == "" && constraintCSCD == "" && hazi == 0 && hie5 == "" && cfi4 == "" && popn == "" && csno == "" && cscd == "" && zalc == 0 && zsan == 0 && zca1 == "" && zca2 == "" && zca3 == "" && zca4 == "" && zca5 == "" && zca6 == "" && zca7 == "" && zca8 == "" && znag == "") {
      expressionEXT030 = (expressionEXT030.eq("EXZALI", zali as String)).or(expressionEXT030.eq("EXZALI", "2"))
    } else {
      expressionEXT030 = expressionEXT030.and((expressionEXT030.eq("EXZALI", zali as String)).or(expressionEXT030.eq("EXZALI", "2")))
    }
    if (cuno == "" && constraintCSCD == "" && hazi == 0 && hie5 == "" && cfi4 == "" && popn == "" && csno == "" && cscd == "" && zalc == 0 && zsan == 0 && zca1 == "" && zca2 == "" && zca3 == "" && zca4 == "" && zca5 == "" && zca6 == "" && zca7 == "" && zca8 == "" && znag == "" && zali == 0) {
      expressionEXT030 = (expressionEXT030.eq("EXZORI", zori as String)).or(expressionEXT030.eq("EXZORI", "2"))
    } else {
      expressionEXT030 = expressionEXT030.and((expressionEXT030.eq("EXZORI", zori as String)).or(expressionEXT030.eq("EXZORI", "2")))
    }
    if (cuno == "" && constraintCSCD == "" && hazi == 0 && hie5 == "" && cfi4 == "" && popn == "" && csno == "" && cscd == "" && zalc == 0 && zsan == 0 && zca1 == "" && zca2 == "" && zca3 == "" && zca4 == "" && zca5 == "" && zca6 == "" && zca7 == "" && zca8 == "" && znag == "" && zali == 0 && zori == 0) {
      expressionEXT030 = (expressionEXT030.eq("EXZPHY", zphy as String)).or(expressionEXT030.eq("EXZPHY", "2"))
    } else {
      expressionEXT030 = expressionEXT030.and((expressionEXT030.eq("EXZPHY", zphy as String)).or(expressionEXT030.eq("EXZPHY", "2")))
    }
    if (cuno == "" && constraintCSCD == "" && hazi == 0 && hie5 == "" && cfi4 == "" && popn == "" && csno == "" && cscd == "" && zalc == 0 && zsan == 0 && zca1 == "" && zca2 == "" && zca3 == "" && zca4 == "" && zca5 == "" && zca6 == "" && zca7 == "" && zca8 == "" && znag == "" && zali == 0 && zori == 0 && zphy == 0) {
      expressionEXT030 = (expressionEXT030.eq("EXZOHF", zohf as String)).or(expressionEXT030.eq("EXZOHF", "2"))
    } else {
      expressionEXT030 = expressionEXT030.and((expressionEXT030.eq("EXZOHF", zohf as String)).or(expressionEXT030.eq("EXZOHF", "2")))
    }
    DBAction queryEXT030 = database.table("EXT030").index("10").matching(expressionEXT030).selection("EXZCID", "EXZCOD", "EXZBLO").build()
    DBContainer EXT030 = queryEXT030.getContainer()
    EXT030.set("EXCONO", currentCompany)
    EXT030.set("EXZBLO", 1)
    EXT030.set("EXSTAT", "20")
    if (!queryEXT030.readAll(EXT030, 3, nbMaxRecord, outDataEXT030)) {
    }
    return constraintIsOK
  }
  // Retrieve EXT021
  Closure<?> outDataEXT0212 = { DBContainer EXT021 ->
    data = EXT021.get("EXDATA")
  }
  // Retrieve EXT030
  Closure<?> outDataEXT030 = { DBContainer EXT030 ->
    constraintIsOK = false
  }
  // Retrieve EXT010
  Closure<?> ext010Reader = { DBContainer ext010Result ->
    itno = ext010Result.get("EXITNO")
    cmde = ext010Result.get("EXCMDE")
    sule = ext010Result.get("EXSULE")
    suld = ext010Result.get("EXSULD")
    ext010Fvdt = ext010Result.get("EXFVDT") as Integer
    ext010Lvdt = ext010Result.get("EXLVDT") as Integer
    countExt010Records++
    lastItno = itno

    // Read items et insert in EXT022 the selected items
    if (itno != "") {
      DBAction mitmasQuery = database.table("MITMAS").index("00").selection("MMSTAT", "MMPROD", "MMHIE1", "MMHIE2", "MMHIE3", "MMHIE4", "MMHIE5", "MMBUAR", "MMCFI1", "MMCFI2", "MMSPE1", "MMHAZI", "MMCFI4", "MMSUNO").build()
      DBContainer mitmasRequest = mitmasQuery.getContainer()
      mitmasRequest.set("MMCONO", currentCompany)
      mitmasRequest.set("MMITNO", itno)
      if (!mitmasQuery.readAll(mitmasRequest, 2, 1, outDataMITMAS)) {
      }
    }
  }
  // Retrieve EXT021
  Closure<?> outDataEXT021 = { DBContainer EXT021 ->
    criteriaFound = true
    // Initialization of a boolean for each criteria: 0 = not used, 1 = inclusion, 2 = exclusion
    if (EXT021.get("EXTYPE") == "SUNO" && EXT021.get("EXCHB1") == 0) iSUNO = 1
    if (EXT021.get("EXTYPE") == "iSUNO" && EXT021.get("EXCHB1") == 1) iSUNO = 2
    if (EXT021.get("EXTYPE") == "PROD" && EXT021.get("EXCHB1") == 0) iPROD = 1
    if (EXT021.get("EXTYPE") == "PROD" && EXT021.get("EXCHB1") == 1) iPROD = 2
    if (EXT021.get("EXTYPE") == "HIE1" && EXT021.get("EXCHB1") == 0) iHIE1 = 1
    if (EXT021.get("EXTYPE") == "HIE1" && EXT021.get("EXCHB1") == 1) iHIE1 = 2
    if (EXT021.get("EXTYPE") == "HIE2" && EXT021.get("EXCHB1") == 0) iHIE2 = 1
    if (EXT021.get("EXTYPE") == "HIE2" && EXT021.get("EXCHB1") == 1) iHIE2 = 2
    if (EXT021.get("EXTYPE") == "HIE3" && EXT021.get("EXCHB1") == 0) iHIE3 = 1
    if (EXT021.get("EXTYPE") == "HIE3" && EXT021.get("EXCHB1") == 1) iHIE3 = 2
    if (EXT021.get("EXTYPE") == "HIE4" && EXT021.get("EXCHB1") == 0) iHIE4 = 1
    if (EXT021.get("EXTYPE") == "HIE4" && EXT021.get("EXCHB1") == 1) iHIE4 = 2
    if (EXT021.get("EXTYPE") == "HIE5" && EXT021.get("EXCHB1") == 0) iHIE5 = 1
    if (EXT021.get("EXTYPE") == "HIE5" && EXT021.get("EXCHB1") == 1) iHIE5 = 2
    if (EXT021.get("EXTYPE") == "BUAR" && EXT021.get("EXCHB1") == 0) iBUAR = 1
    if (EXT021.get("EXTYPE") == "BUAR" && EXT021.get("EXCHB1") == 1) iBUAR = 2
    if (EXT021.get("EXTYPE") == "CFI1" && EXT021.get("EXCHB1") == 0) iCFI1 = 1
    if (EXT021.get("EXTYPE") == "CFI1" && EXT021.get("EXCHB1") == 1) iCFI1 = 2
    if (EXT021.get("EXTYPE") == "CSCD" && EXT021.get("EXCHB1") == 0) iCSCD = 1
    if (EXT021.get("EXTYPE") == "CSCD" && EXT021.get("EXCHB1") == 1) iCSCD = 2
    if (EXT021.get("EXTYPE") == "CSNO" && EXT021.get("EXCHB1") == 0) iCSNO = 1
    if (EXT021.get("EXTYPE") == "CSNO" && EXT021.get("EXCHB1") == 1) iCSNO = 2
    if (EXT021.get("EXTYPE") == "CFI2" && EXT021.get("EXCHB1") == 0) iCFI2 = 1
    if (EXT021.get("EXTYPE") == "CFI2" && EXT021.get("EXCHB1") == 1) iCFI2 = 2
    if (EXT021.get("EXTYPE") == "ITNO" && EXT021.get("EXCHB1") == 0) iITNO = 1
    if (EXT021.get("EXTYPE") == "ITNO" && EXT021.get("EXCHB1") == 1) iITNO = 2
    if (EXT021.get("EXTYPE") == "POPN" && EXT021.get("EXCHB1") == 0) iPOPN = 1
    if (EXT021.get("EXTYPE") == "POPN" && EXT021.get("EXCHB1") == 1) iPOPN = 2
    if (EXT021.get("EXTYPE") == "ULTY" && EXT021.get("EXCHB1") == 0) iULTY = 1
    if (EXT021.get("EXTYPE") == "ULTY" && EXT021.get("EXCHB1") == 1) iULTY = 2
    if (EXT021.get("EXTYPE") == "SLDY" && EXT021.get("EXCHB1") == 0) iSLDY = 1
    if (EXT021.get("EXTYPE") == "SLDY" && EXT021.get("EXCHB1") == 1) iSLDY = 2
    if (EXT021.get("EXTYPE") == "CPFX" && EXT021.get("EXCHB1") == 0) iCPFX = 1
    if (EXT021.get("EXTYPE") == "CPFX" && EXT021.get("EXCHB1") == 1) iCPFX = 2
    if (EXT021.get("EXTYPE") == "CMDE" && EXT021.get("EXCHB1") == 0) iCMDE = 1
    if (EXT021.get("EXTYPE") == "CMDE" && EXT021.get("EXCHB1") == 1) iCMDE = 2
    if (EXT021.get("EXTYPE") == "GOLD" && EXT021.get("EXCHB1") == 0) iGOLD = 1
    if (EXT021.get("EXTYPE") == "GOLD" && EXT021.get("EXCHB1") == 1) iGOLD = 2
  }
  // Retrieve CUGEX1
  Closure<?> outDataCUGEX1 = { DBContainer CUGEX1 ->
    ulty = CUGEX1.get("F1A030")
    cpfx = CUGEX1.get("F1A830")
  }
  // Retrieve MITPOP
  Closure<?> outDataMITPOP = { DBContainer MITPOP ->
    popn = MITPOP.get("MPPOPN")
  }
  // Exceute EXT023MI AddAssortItems
  private executeEXT023MIAddAssortItems(String ASCD, String CUNO, String FDAT) {
    Map<String, String> parameters = ["ASCD": ASCD, "CUNO": CUNO, "FDAT": FDAT]
    Closure<?> handler = { Map<String, String> response ->
      if (response.error != null) {
        String header = "MSG"
        String message = "Failed EXT023MI.AddAssortItems: " + response.errorMessage
        logMessage("ERROR", message)
        return
      } else {
      }
    }
    miCaller.call("EXT023MI", "AddAssortItems", parameters, handler)
  }
  // Execute EXT820MI.SubmitBatch
  private executeEXT820MISubmitBatch(String CONO, String JOID, String P001, String P002, String P003, String P004, String P005, String P006, String P007, String P008) {
    Map<String, String> parameters = ["CONO": CONO, "JOID": JOID, "P001": P001, "P002": P002, "P003": P003, "P004": P004, "P005": P005, "P006": P006, "P007": P007, "P008": P008]
    Closure<?> handler = { Map<String, String> response ->
      if (response.error != null) {
        logger.debug("#PB EXT820MI.SubmitBatch error :" + response.errorMessage)
        String header = "MSG"
        String message = "Failed EXT820MI.SubmitBatch: " + response.errorMessage
        logMessage(header, message)
        return
      } else {
      }
    }
    miCaller.call("EXT820MI", "SubmitBatch", parameters, handler)
  }
  // Execute EXT023MI UpdAssortItems
  private executeEXT023MIUpdAssortItems(String ASCD, String CUNO, String FDAT) {
    Map<String, String> parameters = ["ASCD": ASCD, "CUNO": CUNO, "FDAT": FDAT]
    Closure<?> handler = { Map<String, String> response ->
      if (response.error != null) {
        String header = "MSG"
        String message = "Failed EXT023MI.UpdAssortItems: " + response.errorMessage
        logMessage("ERROR", message)
        return
      } else {
      }
    }
    miCaller.call("EXT023MI", "UpdAssortItems", parameters, handler)
  }
  /**
   * Update CUGEX1 for OASITN
   * @param status
   * @param count
   */
  public void updateCUGEX1(String status, String count) {
    DBAction cugex1Query = database.table("CUGEX1").index("00").build()
    DBContainer cugex1Request = cugex1Query.getContainer()
    cugex1Request.set("F1CONO", currentCompany)
    cugex1Request.set("F1FILE", "OASCUS")
    cugex1Request.set("F1PK01", ascd)
    cugex1Request.set("F1PK02", cuno)
    cugex1Request.set("F1PK03", fdat)
    if (!cugex1Query.read(cugex1Request)) {
      executeCUSEXTMIAddFieldValue("OASCUS", ascd, cuno, fdat, "", "", "", "", "", status, count)
    } else {
      executeCUSEXTMIChgFieldValue("OASCUS", ascd, cuno, fdat, "", "", "", "", "", status, count)
    }
  }

  // Execute CUSEXTMI AddFieldValue
  private executeCUSEXTMIAddFieldValue(String FILE, String PK01, String PK02, String PK03, String PK04, String PK05, String PK06, String PK07, String PK08, String A030, String N096) {
    Map<String, String> parameters = ["FILE": FILE, "PK01": PK01, "PK02": PK02, "PK03": PK03, "PK04": PK04, "PK05": PK05, "PK06": PK06, "PK07": PK07, "PK08": PK08, "A030": A030, "N096": N096]
    Closure<?> handler = { Map<String, String> response ->
      if (response.error != null) {
        String message = "Failed CUSEXTMI.AddFieldValue: " + response.errorMessage
        logMessage("ERROR", message)
        return
      } else {
      }
    }
    miCaller.call("CUSEXTMI", "AddFieldValue", parameters, handler)
  }
  // Exceute CUSEXTMI ChgFieldValue
  private executeCUSEXTMIChgFieldValue(String FILE, String PK01, String PK02, String PK03, String PK04, String PK05, String PK06, String PK07, String PK08, String A030, String N096) {
    Map<String, String> parameters = ["FILE": FILE, "PK01": PK01, "PK02": PK02, "PK03": PK03, "PK04": PK04, "PK05": PK05, "PK06": PK06, "PK07": PK07, "PK08": PK08, "A030": A030, "N096": N096]
    Closure<?> handler = { Map<String, String> response ->
      if (response.error != null) {
        String message = "Failed CUSEXTMI.ChgFieldValue: " + response.errorMessage
        logMessage("ERROR", message)
        return
      } else {
      }
    }
    miCaller.call("CUSEXTMI", "ChgFieldValue", parameters, handler)
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
    if (!query.readAllLock(EXTJOB, 1, updateCallBackEXTJOB)) {
    }
  }
  // Delete EXTJOB
  Closure<?> updateCallBackEXTJOB = { LockedResult lockedResult ->
    lockedResult.delete()
  }
  /**
   * Initialize log management
   */
  private void initializeLogManagement() {
    textFiles.open("log")
    logfile = program.getProgramName() + "." + "batch" + "." + jobNumber + ".log"
    logmessages = new LinkedList<String>()
    loglevel = getCRS881("", "EXTENC", "1", "ExtendM3", "I", program.getProgramName(), "LOGLEVEL", "", "")
    logger.debug("loglevel = " + loglevel)
    if (!LOGLEVELS.contains(loglevel)) {
      String message = "Niveau de log incorrect ${loglevel}"
      loglevel = "ERROR"
      logMessage("ERROR", message)
    }
  }
  /**
   * @param level
   * @param message
   */
  void logMessage(String level, String message) {
    int lvl = LOGLEVELS.indexOf(level)
    int lvg = LOGLEVELS.indexOf(loglevel)
    if (lvl >= lvg) {
      message = LocalDateTime.now().toString() + ": ${level} ${message}"
      logmessages.add(message)
    }
  }
  /**
   * Log messages
   */
  private void logMessages(){
    String message = String.join("\r\n", logmessages)
    Closure<?> consumer = { PrintWriter printWriter ->
      printWriter.println(message)
    }
    textFiles.write(logfile, "UTF-8", true, consumer)
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
