import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
/**
 * README
 * This extension is used by Mashup
 *
 * Name : EXT022
 * Description : Select the items based on the criteria contained in EXT021 table and add records to the EXT022 table (EXT022MI.SelAssortItems conversion)
 * Date         Changed By   Description
 * 20220112     YBLUTEAU     COMX01- Add assortment
 * 20230303     ARENARD      Constraints handling has been added and adjustments have been made
 * 20230717     ARENARD      csno handling has been fixed
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
  private String currentDivision
  private String ascd = ""
  private String cuno = ""
  private String cunt = ""
  private String fdat = ""
  private String prrf = ""
  private String cucd = ""
  private String fvdt = ""
  private String Status = ""
  private Integer STAT=0; private String stat=""
  private Integer SUNO=0; private String suno=""
  private Integer PROD=0; private String prod=""
  private Integer HIE1=0; private String hie1=""
  private Integer HIE2=0; private String hie2=""
  private Integer HIE3=0; private String hie3=""
  private Integer HIE4=0; private String hie4=""
  private Integer HIE5=0; private String hie5=""
  private Integer BUAR=0; private String buar=""
  private Integer CFI1=0; private String cfi1=""
  private Integer CSCD=0; private String cscd=""
  private Integer CSNO=0; private String csno=""
  private Integer CFI2=0; private String cfi2=""
  private Integer ITNO=0; private String itno=""
  private Integer POPN=0; private String popn=""
  private Integer ULTY=0; private String ulty=""
  private Integer SLDY=0; private String sldy=""
  private Integer CPFX=0; private String cpfx=""
  private Integer CMDE=0; private String cmde=""
  private Integer GOLD=0; private String gold=""
  private String spe1=""
  private String hazi
  private String cfi4
  private Integer zali
  private String constraint_CSCD
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
  private String data
  private Integer zohf

  private boolean constraint_isOK = false
  private boolean criteria_found = false
  private Integer Count = 0
  private String count  = 0
  private Integer count_item = 0
  private boolean IN60 = false
  private String rawData
  private int rawDataLength
  private int beginIndex
  private int endIndex
  private String logFileName
  private String jobNumber
  private String currentDate = ""

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
    LocalDateTime timeOfCreation = LocalDateTime.now()
    jobNumber = program.getJobNumber() + timeOfCreation.format(DateTimeFormatter.ofPattern("yyMMdd")) + timeOfCreation.format(DateTimeFormatter.ofPattern("HHmmss"))

    //logger.debug("Début EXT022")
    if(batch.getReferenceId().isPresent()){
      Optional<String> data = getJobData(batch.getReferenceId().get())
      performActualJob(data)
    } else {
      // No job data found
      //logger.debug("Job data for job ${batch.getJobId()} is missing")
    }
  }
  // Get job data
  private Optional<String> getJobData(String referenceId){
    def query = database.table("EXTJOB").index("00").selection("EXDATA").build()
    def container = query.createContainer()
    container.set("EXRFID", referenceId)
    if (query.read(container)){
      //logger.debug("EXDATA = " + container.getString("EXDATA"))
      return Optional.of(container.getString("EXDATA"))
    } else {
      //logger.debug("EXTJOB not found")
    }
    return Optional.empty()
  }
  // Perform actual job
  private performActualJob(Optional<String> data){
    if(!data.isPresent()){
      //logger.debug("Job reference Id ${batch.getReferenceId().get()} is passed but data was not found")
      return
    }
    rawData = data.get()
    logger.debug("Début performActualJob")
    String inASCD = getFirstParameter()
    String inCUNO = getNextParameter()
    String inFDAT = getNextParameter()
    String inITNO = getNextParameter()
    String inOPT2 = getNextParameter()
    String inPRRF = getNextParameter()
    String inCUCD = getNextParameter()
    String inFVDT = getNextParameter()
    String inCUNT = getNextParameter()

    logger.debug("inASCD = " + inASCD)
    logger.debug("inCUNO = " + inCUNO)
    logger.debug("inFDAT = " + inFDAT)
    logger.debug("inITNO = " + inITNO)
    logger.debug("inOPT2 = " + inOPT2)
    logger.debug("inPRRF = " + inPRRF)
    logger.debug("inCUCD = " + inCUCD)
    logger.debug("inFVDT = " + inFVDT)
    logger.debug("inCUNT = " + inCUNT)

    currentCompany = (Integer)program.getLDAZD().CONO
    currentDivision = program.getLDAZD().DIVI
    LocalDateTime timeOfCreation = LocalDateTime.now()

    ascd = inASCD
    cuno = inCUNO
    cunt = inCUNT

    if (inFDAT != null && !inFDAT.trim().isBlank()) {
      fdat = inFDAT
      if (!utility.call("DateUtil", "isDateValid", fdat, "yyyyMMdd")) {
        String header = "MSG;"+"FDAT"
        String message = "Date de début est incorrecte " + ";" + fdat
        logMessage(header, message)
        return
      }
    }else{
      fdat = "0"
    }

    // Check selection header
    DBAction query = database.table("EXT020").index("00").build()
    DBContainer EXT020 = query.getContainer()
    EXT020.set("EXCONO", currentCompany)
    EXT020.set("EXASCD", ascd)
    EXT020.set("EXCUNO", cuno)
    EXT020.setInt("EXFDAT", fdat as Integer)
    if(!query.readAll(EXT020, 4, outData_EXT020)){
      String header = "MSG"
      String message = "Entête sélection n'existe pas"
      logMessage(header, message)
      return
    }

    // Check option
    if(inOPT2 == null && !inOPT2.trim().isBlank()){
      String header = "MSG"
      String message = "Option est obligatoire"
      logMessage(header, message)
      return
    }

    if(inOPT2 != "1" && inOPT2 != "2"){
      String opt2 = inOPT2
      String header = "MSG"
      String message = "Option "+ opt2 + " est invalide"
      logMessage(header, message)
      return
    }

    prrf = inPRRF
    cucd = inCUCD

    if (inFVDT != null && !inFVDT.trim().isBlank()) {
      fvdt = inFVDT
      if (!utility.call("DateUtil", "isDateValid", fvdt, "yyyyMMdd")) {
        String header = "MSG"
        String message = "Date de début de validité " + fvdt + " est incorrecte"
        logMessage(header, message)
        return
      }
    }else{
      fvdt = "0"
    }

    constraint_CSCD = ""
    DBAction OCUSMA_query = database.table("OCUSMA").index("00").selection("OKCSCD").build()
    DBContainer OCUSMA = OCUSMA_query.getContainer()
    OCUSMA.set("OKCONO", currentCompany)
    OCUSMA.set("OKCUNO", cuno)
    if (OCUSMA_query.read(OCUSMA)) {
      //logger.debug("found OCUSMA")
      constraint_CSCD = OCUSMA.getString("OKCSCD")
    }

    // Check criteria used in the selection
    checkUsedCriteria()

    if(criteria_found){
      Update_CUGEX1("10", count)

      // Delete file EXT022
      deleteEXT022()

      ExpressionFactory expression_EXT010 = database.getExpressionFactory("EXT010")
      expression_EXT010 = expression_EXT010.le("EXFVDT", timeOfCreation.format(DateTimeFormatter.ofPattern("yyyyMMdd")))
      expression_EXT010 = expression_EXT010.and(expression_EXT010.ge("EXLVDT", timeOfCreation.format(DateTimeFormatter.ofPattern("yyyyMMdd"))))
      DBAction EXT010_query = database.table("EXT010").index("02").matching(expression_EXT010).selection("EXCUNO", "EXITNO", "EXCMDE", "EXFVDT", "EXLVDT", "EXSULE").build()
      DBContainer EXT010 = EXT010_query.getContainer()
      EXT010.set("EXCONO", currentCompany)
      EXT010.set("EXCUNO", cuno)
      if(!EXT010_query.readAll(EXT010, 2, outData_EXT010)) {}

      //logger.debug("EXT022MI SelAssortItems : count_item = " + count_item)
      // Add mode
      if (inOPT2 == "1") {
        // Add selected items in the assortment
        executeEXT820MISubmitBatch(currentCompany as String, "EXT023", ascd, cuno, fdat, "", "", "", "", "")
      }
      // Update mode
      if (inOPT2 == "2") {
        // Update selected items in the assortment
        executeEXT023MIUpdAssortItems(ascd, cuno, fdat)
      }
      count = Count
      Update_CUGEX1("90", count)
    }
    //    if (inPRRF != null && inCUCD != null && inFVDT != null) {
    //      Update_EXT080_EXT081()
    //    }
    // Delete file EXTJOB
    deleteEXTJOB()
  }
  /**
   * Check used criteria contained in EXT021 table
   */
  public void checkUsedCriteria(){
    criteria_found = false
    DBAction EXT021_query = database.table("EXT021").index("00").selection("EXCHB1").build()
    DBContainer EXT021 = EXT021_query.getContainer()
    EXT021.set("EXCONO", currentCompany)
    EXT021.set("EXASCD", ascd)
    EXT021.set("EXCUNO", cuno)
    EXT021.set("EXFDAT", fdat as Integer)
    // Initialization of a boolean for each criteria: 0 = not used, 1 = inclusion, 2 = exclusion
    EXT021.set("EXTYPE", "SUNO")
    if (!EXT021_query.readAll(EXT021, 5, outData_EXT021)) {}
    EXT021.set("EXTYPE", "PROD")
    if (!EXT021_query.readAll(EXT021, 5, outData_EXT021)) {}
    EXT021.set("EXTYPE", "HIE1")
    if (!EXT021_query.readAll(EXT021, 5, outData_EXT021)) {}
    EXT021.set("EXTYPE", "HIE2")
    if (!EXT021_query.readAll(EXT021, 5, outData_EXT021)) {}
    EXT021.set("EXTYPE", "HIE3")
    if (!EXT021_query.readAll(EXT021, 5, outData_EXT021)) {}
    EXT021.set("EXTYPE", "HIE4")
    if (!EXT021_query.readAll(EXT021, 5, outData_EXT021)) {}
    EXT021.set("EXTYPE", "HIE5")
    if (!EXT021_query.readAll(EXT021, 5, outData_EXT021)) {}
    EXT021.set("EXTYPE", "BUAR")
    if (!EXT021_query.readAll(EXT021, 5, outData_EXT021)) {}
    EXT021.set("EXTYPE", "CFI1")
    if (!EXT021_query.readAll(EXT021, 5, outData_EXT021)) {}
    EXT021.set("EXTYPE", "CSCD")
    if (!EXT021_query.readAll(EXT021, 5, outData_EXT021)) {}
    EXT021.set("EXTYPE", "CSNO")
    if (!EXT021_query.readAll(EXT021, 5, outData_EXT021)) {}
    EXT021.set("EXTYPE", "CFI2")
    if (!EXT021_query.readAll(EXT021, 5, outData_EXT021)) {}
    EXT021.set("EXTYPE", "ITNO")
    if (!EXT021_query.readAll(EXT021, 5, outData_EXT021)) {}
    EXT021.set("EXTYPE", "POPN")
    if (!EXT021_query.readAll(EXT021, 5, outData_EXT021)) {}
    EXT021.set("EXTYPE", "ULTY")
    if (!EXT021_query.readAll(EXT021, 5, outData_EXT021)) {}
    EXT021.set("EXTYPE", "SLDY")
    if (!EXT021_query.readAll(EXT021, 5, outData_EXT021)) {}
    EXT021.set("EXTYPE", "CPFX")
    if (!EXT021_query.readAll(EXT021, 5, outData_EXT021)) {}
    EXT021.set("EXTYPE", "CMDE")
    if (!EXT021_query.readAll(EXT021, 5, outData_EXT021)) {}
    EXT021.set("EXTYPE", "GOLD")
    if (!EXT021_query.readAll(EXT021, 5, outData_EXT021)) {}
    //logger.debug("checkUsedCriteria")
    //logger.debug("SUNO = " + SUNO)
    //logger.debug("PROD = " + PROD)
    //logger.debug("HIE1 = " + HIE1)
    //logger.debug("HIE2 = " + HIE2)
    //logger.debug("HIE3 = " + HIE3)
    //logger.debug("HIE4 = " + HIE4)
    //logger.debug("HIE5 = " + HIE5)
    //logger.debug("BUAR = " + BUAR)
    //logger.debug("CFI1 = " + CFI1)
    //logger.debug("CSCD = " + CSCD)
    //logger.debug("CSNO = " + CSNO)
    //logger.debug("CFI2 = " + CFI2)
    //logger.debug("ITNO = " + ITNO)
    //logger.debug("POPN = " + POPN)
    //logger.debug("ULTY = " + ULTY)
    //logger.debug("SLDY = " + SLDY)
    //logger.debug("CPFX = " + CPFX)
    //logger.debug("CMDE = " + CMDE)
    //logger.debug("GOLD = " + GOLD)
  }
  // Retrieve MITMAS
  Closure<?> outData_MITMAS = { DBContainer MITMAS ->
    // Get item criteria value
    itno = MITMAS.get("MMITNO")
    //logger.debug("EXT022 itno = " + itno)
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
    //logger.debug("stat = " + stat)
    //logger.debug("prod = " + prod)
    //logger.debug("hie1 = " + hie1)
    //logger.debug("hie2 = " + hie2)
    //logger.debug("hie3 = " + hie3)
    //logger.debug("hie4 = " + hie4)
    //logger.debug("hie5 = " + hie5)
    //logger.debug("buar = " + buar)
    //logger.debug("cfi1 = " + cfi1)
    if(cfi2.trim() != "") {
      //logger.debug("itno = " + itno)
      //logger.debug("cfi2 = " + cfi2)
    }
    //logger.debug("spe1 = " + spe1)
    sldy = 0
    if(spe1.trim() != "") {
      sldy = spe1.trim() as Integer
    }
    //logger.debug("sldy = " + sldy)
    hazi = MITMAS.get("MMHAZI")
    cfi4 = MITMAS.get("MMCFI4")
    suno = MITMAS.get("MMSUNO")

    zali = 0
    if(MITMAS.get("MMITGR") == "ALIM") {
      zali = 1
    }

    // Get the value for the other criteria used in the selection
    getCriteriaValue()

    //logger.debug("EXT022MI SelAssortItems : itno = " + itno)


    // Check if the item matches the selection
    logger.debug("Before itemSelection")
    if(itemSelectionOK()) {
      logger.debug("After itemSelection")
      count_item++
      logger.debug("Before constraint selection")
      if (constraintsOK()) {
        logger.debug("After constraint selection")
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
          logger.debug("EXT022MI insert item : itno = " + itno)
          Count++
        }
      }
    }
  }
  //  Delete records related to the assortment from EXT022 table
  public void deleteEXT022(){
    LocalDateTime timeOfCreation = LocalDateTime.now()
    DBAction query = database.table("EXT022").index("00").build()
    DBContainer EXT022 = query.getContainer()
    EXT022.set("EXCONO", currentCompany)
    EXT022.set("EXASCD", ascd)
    EXT022.set("EXCUNO", cuno)
    EXT022.set("EXFDAT", fdat as Integer)
    if(!query.readAllLock(EXT022, 4, updateCallBack)){
    }
  }
  // Delete EXTJOB
  Closure<?> updateCallBack = { LockedResult lockedResult ->
    lockedResult.delete()
  }
  /**
   * Retrieve criterias values
   */
  public void getCriteriaValue() {
    //logger.debug("getCriteriaValue")
    ulty = ""
    cpfx = ""
    if(ULTY != 0 || CPFX != 0){
      ExpressionFactory expression_CUGEX1 = database.getExpressionFactory("CUGEX1")
      DBAction CUGEX1_query = database.table("CUGEX1").index("00").matching(expression_CUGEX1).selection("F1A030", "F1A830").build()
      DBContainer CUGEX1 = CUGEX1_query.getContainer()
      CUGEX1.set("F1CONO", currentCompany)
      CUGEX1.set("F1FILE", "MITMAS")
      CUGEX1.set("F1PK01", itno)
      if(!CUGEX1_query.readAll(CUGEX1, 3, outData_CUGEX1)){}
    }
    //logger.debug("ulty = " + ulty)
    //logger.debug("cpfx = " + cpfx)
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
    //logger.debug("cscd = " + cscd)
    //logger.debug("csno = " + csno)
    popn = ""
    //if(POPN != 0){
    ExpressionFactory expression_MITPOP = database.getExpressionFactory("MITPOP")
    expression_MITPOP = expression_MITPOP.eq("MPREMK", "SIGMA6")
    DBAction MITPOP_query = database.table("MITPOP").index("00").matching(expression_MITPOP).selection("MPPOPN").build()
    DBContainer MITPOP = MITPOP_query.getContainer()
    MITPOP.set("MPCONO", currentCompany)
    MITPOP.set("MPALWT", 1)
    MITPOP.set("MPALWQ", "")
    MITPOP.set("MPITNO", itno)
    if (!MITPOP_query.readAll(MITPOP, 4, outData_MITPOP)) {
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
    //logger.debug("popn = " + popn)
    //logger.debug("suno = " + suno)
    //logger.debug("cscd = " + cscd)
    DBAction EXT032_query = database.table("EXT032").index("00").selection("EXZALC", "EXZSAN", "EXZCA1", "EXZCA2", "EXZCA3", "EXZCA4", "EXZCA5", "EXZCA6", "EXZCA7", "EXZCA8", "EXZORI", "EXZPHY", "EXZAGR").build()
    DBContainer EXT032 = EXT032_query.getContainer()
    EXT032.set("EXCONO", currentCompany)
    EXT032.set("EXPOPN", popn)
    EXT032.set("EXSUNO", suno)
    EXT032.set("EXORCO", cscd)
    if(EXT032_query.read(EXT032)){
      //logger.debug("found EXT032 xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx")
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
      ////logger.debug("zagr = " + EXT032.get("EXZAGR")
      zagr = EXT032.get("EXZAGR")
    }
    //logger.debug("zalc = " + zalc)
    //logger.debug("zsan = " + zsan)
    //logger.debug("zcap = " + zcap)
    //logger.debug("zca1 = " + zca1)
    //logger.debug("zca2 = " + zca2)
    //logger.debug("zca3 = " + zca3)
    //logger.debug("zca4 = " + zca4)
    //logger.debug("zca5 = " + zca5)
    //logger.debug("zca6 = " + zca6)
    //logger.debug("zca7 = " + zca7)
    //logger.debug("zca8 = " + zca8)
    //logger.debug("zori = " + zori)
    //logger.debug("zphy = " + zphy)
    //logger.debug("zagr = " + zagr)
    znag = ""
    DBAction CUGEX1_CIDMAS_query = database.table("CUGEX1").index("00").selection("F1A030").build()
    DBContainer CUGEX1_CIDMAS = CUGEX1_CIDMAS_query.getContainer()
    CUGEX1_CIDMAS.set("F1CONO", currentCompany)
    CUGEX1_CIDMAS.set("F1FILE",  "CIDMAS")
    if(prod.trim() != "") {
      CUGEX1_CIDMAS.set("F1PK01",  prod)
    } else {
      CUGEX1_CIDMAS.set("F1PK01",  suno)
    }
    CUGEX1_CIDMAS.set("F1PK02",  "")
    CUGEX1_CIDMAS.set("F1PK03",  "")
    CUGEX1_CIDMAS.set("F1PK04",  "")
    CUGEX1_CIDMAS.set("F1PK05",  "")
    CUGEX1_CIDMAS.set("F1PK06",  "")
    CUGEX1_CIDMAS.set("F1PK07",  "")
    CUGEX1_CIDMAS.set("F1PK08",  "")
    if(CUGEX1_CIDMAS_query.read(CUGEX1_CIDMAS)){
      //logger.debug("found CUGEX1")
      znag = CUGEX1_CIDMAS.get("F1A030")
    }
    //logger.debug("znag = " + znag)
  }
  /**
   * Return true if the item matches the selection criteria and not if it does not
   */
  public boolean itemSelectionOK() {
    // Status must be greater than or equal to 80
    if(stat >= "80")
      return false

    // If global assortment is selected, selection is ok for all the items
    if(GOLD == 1)
      return true

    DBAction EXT021_query = database.table("EXT021").index("00").selection("EXCHB1").build()
    DBContainer EXT021 = EXT021_query.getContainer()
    EXT021.set("EXCONO", currentCompany)
    EXT021.set("EXASCD", ascd)
    EXT021.set("EXCUNO", cuno)
    EXT021.set("EXFDAT", fdat as Integer)

    // cfi2 must be greater or equal than the criteria
    if(CFI2 != 0) {
      data = ""
      EXT021.set("EXTYPE", "CFI2")
      if(!EXT021_query.readAll(EXT021, 5, 1, outData_EXT021_2)){
      }

      if((cfi2.trim() as double) >= (data.trim() as double)) {
        if (CFI2 == 2) return false
      } else {
        if (CFI2 == 1) return false
      }
    }

    if(ITNO != 0) {
      EXT021.set("EXTYPE", "ITNO")
      EXT021.set("EXDATA", itno)
      if (EXT021_query.read(EXT021)) {
        if (ITNO == 2) return false
      } else {
        if (ITNO == 1) return false
      }
    }
    if(SUNO != 0) {
      if (SUNO == 2 && sule.trim() == "")
        return false

      EXT021.set("EXTYPE", "SUNO")
      EXT021.set("EXDATA", sule)
      if (EXT021_query.read(EXT021)) {
        if (SUNO == 2) return false
      } else {
        if (SUNO == 1) return false
      }
    }
    if(PROD != 0) {
      if (PROD == 2 && suno.trim() == "")
        return false

      EXT021.set("EXTYPE", "PROD")
      EXT021.set("EXDATA", suno)
      if (EXT021_query.read(EXT021)) {
        if (PROD == 2) return false
      } else {
        if (PROD == 1) return false
      }
    }
    if(HIE1 != 0) {
      EXT021.set("EXTYPE", "HIE1")
      EXT021.set("EXDATA", hie1)
      if (EXT021_query.read(EXT021)) {
        if (HIE1 == 2) return false
      } else {
        if (HIE1 == 1) return false
      }
    }
    if(HIE2 != 0) {
      EXT021.set("EXTYPE", "HIE2")
      EXT021.set("EXDATA", hie2)
      if (EXT021_query.read(EXT021)) {
        if (HIE2 == 2) return false
      } else {
        if (HIE2 == 1) return false
      }
    }
    if(HIE3 != 0) {
      EXT021.set("EXTYPE", "HIE3")
      EXT021.set("EXDATA", hie3)
      if (EXT021_query.read(EXT021)) {
        if (HIE3 == 2) return false
      } else {
        if (HIE3 == 1) return false
      }
    }
    if(HIE4 != 0) {
      EXT021.set("EXTYPE", "HIE4")
      EXT021.set("EXDATA", hie4)
      if (EXT021_query.read(EXT021)) {
        if (HIE4 == 2) return false
      } else {
        if (HIE4 == 1) return false
      }
    }
    if(HIE5 != 0) {
      EXT021.set("EXTYPE", "HIE5")
      EXT021.set("EXDATA", hie5)
      if (EXT021_query.read(EXT021)) {
        if (HIE5 == 2) return false
      } else {
        if (HIE5 == 1) return false
      }
    }
    if(BUAR != 0) {
      EXT021.set("EXTYPE", "BUAR")
      EXT021.set("EXDATA", buar)
      if (EXT021_query.read(EXT021)) {
        if (BUAR == 2) return false
      } else {
        if (BUAR == 1) return false
      }
    }
    if(CFI1 != 0) {
      EXT021.set("EXTYPE", "CFI1")
      EXT021.set("EXDATA", cfi1)
      if (EXT021_query.read(EXT021)) {
        if (CFI1 == 2) return false
      } else {
        if (CFI1 == 1) return false
      }
    }
    if(CSCD != 0) {
      EXT021.set("EXTYPE", "CSCD")
      EXT021.set("EXDATA", cscd)
      if (EXT021_query.read(EXT021)) {
        if (CSCD == 2) return false
      } else {
        if (CSCD == 1) return false
      }
    }
    if(CSNO != 0) {
      EXT021.set("EXTYPE", "CSNO")
      EXT021.set("EXDATA", csno)
      if (EXT021_query.read(EXT021)) {
        if (CSNO == 2) return false
      } else {
        if (CSNO == 1) return false
      }
    }
    if(POPN != 0) {
      EXT021.set("EXTYPE", "POPN")
      EXT021.set("EXDATA", popn)
      if (EXT021_query.read(EXT021)) {
        if (POPN == 2) return false
      } else {
        if (POPN == 1) return false
      }
    }
    if(ULTY != 0) {
      EXT021.set("EXTYPE", "ULTY")
      EXT021.set("EXDATA", ulty)
      if (EXT021_query.read(EXT021)) {
        if (ULTY == 2) return false
      } else {
        if (ULTY == 1) return false
      }
    }
    // sldy must be greater or equal than the criteria
    if(SLDY != 0) {
      data = ""
      EXT021.set("EXTYPE", "SLDY")
      if(!EXT021_query.readAll(EXT021, 5, 1, outData_EXT021_2)){
      }

      if((sldy.trim() as double) >= (data.trim() as double)) {
        if (SLDY == 2) return false
      } else {
        if (SLDY == 1) return false
      }
    }
    if(CPFX != 0) {
      EXT021.set("EXTYPE", "CPFX")
      EXT021.set("EXDATA", cpfx)
      if (EXT021_query.read(EXT021)) {
        if (CPFX == 2) return false
      } else {
        if (CPFX == 1) return false
      }
    }
    if(CMDE != 0) {
      EXT021.set("EXTYPE", "CMDE")
      EXT021.set("EXDATA", cmde)
      if (EXT021_query.read(EXT021)) {
        if (CMDE == 2) return false
      } else {
        if (CMDE == 1) return false
      }
    }
    return true
  }

  /**
   * Return true if no blocking constraint is found for the item
   */
  public boolean constraintsOK() {
    constraint_isOK = true
    ExpressionFactory expression_EXT030 = database.getExpressionFactory("EXT030")
    expression_EXT030 = (expression_EXT030.eq("EXCUNO", cuno)).or(expression_EXT030.eq("EXCUNO", ""))
    if(cuno == ""){
      expression_EXT030 = (expression_EXT030.eq("EXCSCD", constraint_CSCD)).or(expression_EXT030.eq("EXCSCD", ""))
    } else {
      expression_EXT030 = expression_EXT030.and((expression_EXT030.eq("EXCSCD", constraint_CSCD)).or(expression_EXT030.eq("EXCSCD", "")))
    }
    if(cuno == "" && constraint_CSCD == ""){
      expression_EXT030 = (expression_EXT030.eq("EXHAZI", hazi as String)).or(expression_EXT030.eq("EXHAZI", "2"))
    } else {
      expression_EXT030 = expression_EXT030.and((expression_EXT030.eq("EXHAZI", hazi as String)).or(expression_EXT030.eq("EXHAZI", "2")))
    }
    if(hie5 != "") {
      if(cuno == "" && constraint_CSCD == "" && hazi == 0){
        expression_EXT030 = (expression_EXT030.eq("EXHIE0", hie5)).or(expression_EXT030.eq("EXHIE0", hie5.substring(0,2)+"*")).or(expression_EXT030.eq("EXHIE0", hie5.substring(0,4)+"*")).or(expression_EXT030.eq("EXHIE0", hie5.substring(0,7)+"*")).or(expression_EXT030.eq("EXHIE0", hie5.substring(0,9)+"*")).or(expression_EXT030.eq("EXHIE0", hie5.substring(0,11)+"*")).or(expression_EXT030.eq("EXHIE0", ""))
      } else {
        expression_EXT030 = expression_EXT030.and((expression_EXT030.eq("EXHIE0", hie5)).or(expression_EXT030.eq("EXHIE0", hie5.substring(0,2)+"*")).or(expression_EXT030.eq("EXHIE0", hie5.substring(0,4)+"*")).or(expression_EXT030.eq("EXHIE0", hie5.substring(0,7)+"*")).or(expression_EXT030.eq("EXHIE0", hie5.substring(0,9)+"*")).or(expression_EXT030.eq("EXHIE0", hie5.substring(0,11)+"*")).or(expression_EXT030.eq("EXHIE0", "")))
      }
    } else {
      if(cuno == "" && constraint_CSCD == "" && hazi == 0){
        expression_EXT030 = (expression_EXT030.eq("EXHIE0", hie5)).or(expression_EXT030.eq("EXHIE0", ""))
      } else {
        expression_EXT030 = expression_EXT030.and((expression_EXT030.eq("EXHIE0", hie5)).or(expression_EXT030.eq("EXHIE0", "")))
      }
    }
    if(cuno == "" && constraint_CSCD == "" && hazi == 0 && hie5 == ""){
      expression_EXT030 = (expression_EXT030.eq("EXCFI4", cfi4)).or(expression_EXT030.eq("EXCFI4", ""))
    } else {
      expression_EXT030 = expression_EXT030.and((expression_EXT030.eq("EXCFI4", cfi4)).or(expression_EXT030.eq("EXCFI4", "")))
    }
    if(cuno == "" && constraint_CSCD == "" && hazi == 0 && hie5 == "" && cfi4 == ""){
      expression_EXT030 = (expression_EXT030.eq("EXPOPN", popn)).or(expression_EXT030.eq("EXPOPN", ""))
    } else {
      expression_EXT030 = expression_EXT030.and((expression_EXT030.eq("EXPOPN", popn)).or(expression_EXT030.eq("EXPOPN", "")))
    }
    if(csno != "") {
      if(cuno == "" && constraint_CSCD == "" && hazi == 0 && hie5 == "" && cfi4 == "" && popn == ""){
        if(csno.toString().length() == 16)
          expression_EXT030 = (expression_EXT030.eq("EXCSNO", csno)).or(expression_EXT030.eq("EXCSNO", csno.substring(0,1)+"*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0,2)+"*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0,3)+"*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0,4)+"*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0,5)+"*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0,6)+"*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0,7)+"*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0,8)+"*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0,9)+"*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0,10)+"*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0,11)+"*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0,12)+"*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0,13)+"*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0,14)+"*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0,15)+"*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0,16)+"*")).or(expression_EXT030.eq("EXCSNO", ""))
        if(csno.toString().length() == 15)
          expression_EXT030 = (expression_EXT030.eq("EXCSNO", csno)).or(expression_EXT030.eq("EXCSNO", csno.substring(0,1)+"*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0,2)+"*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0,3)+"*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0,4)+"*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0,5)+"*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0,6)+"*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0,7)+"*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0,8)+"*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0,9)+"*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0,10)+"*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0,11)+"*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0,12)+"*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0,13)+"*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0,14)+"*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0,15)+"*")).or(expression_EXT030.eq("EXCSNO", ""))
        if(csno.toString().length() == 14)
          expression_EXT030 = (expression_EXT030.eq("EXCSNO", csno)).or(expression_EXT030.eq("EXCSNO", csno.substring(0,1)+"*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0,2)+"*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0,3)+"*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0,4)+"*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0,5)+"*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0,6)+"*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0,7)+"*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0,8)+"*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0,9)+"*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0,10)+"*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0,11)+"*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0,12)+"*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0,13)+"*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0,14)+"*")).or(expression_EXT030.eq("EXCSNO", ""))
        if(csno.toString().length() == 13)
          expression_EXT030 = (expression_EXT030.eq("EXCSNO", csno)).or(expression_EXT030.eq("EXCSNO", csno.substring(0,1)+"*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0,2)+"*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0,3)+"*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0,4)+"*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0,5)+"*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0,6)+"*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0,7)+"*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0,8)+"*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0,9)+"*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0,10)+"*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0,11)+"*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0,12)+"*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0,13)+"*")).or(expression_EXT030.eq("EXCSNO", ""))
        if(csno.toString().length() == 12)
          expression_EXT030 = (expression_EXT030.eq("EXCSNO", csno)).or(expression_EXT030.eq("EXCSNO", csno.substring(0,1)+"*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0,2)+"*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0,3)+"*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0,4)+"*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0,5)+"*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0,6)+"*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0,7)+"*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0,8)+"*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0,9)+"*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0,10)+"*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0,11)+"*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0,12)+"*")).or(expression_EXT030.eq("EXCSNO", ""))
        if(csno.toString().length() == 11)
          expression_EXT030 = (expression_EXT030.eq("EXCSNO", csno)).or(expression_EXT030.eq("EXCSNO", csno.substring(0,1)+"*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0,2)+"*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0,3)+"*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0,4)+"*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0,5)+"*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0,6)+"*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0,7)+"*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0,8)+"*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0,9)+"*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0,10)+"*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0,11)+"*")).or(expression_EXT030.eq("EXCSNO", ""))
        if(csno.toString().length() == 10)
          expression_EXT030 = (expression_EXT030.eq("EXCSNO", csno)).or(expression_EXT030.eq("EXCSNO", csno.substring(0,1)+"*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0,2)+"*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0,3)+"*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0,4)+"*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0,5)+"*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0,6)+"*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0,7)+"*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0,8)+"*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0,9)+"*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0,10)+"*")).or(expression_EXT030.eq("EXCSNO", ""))
        if(csno.toString().length() == 9)
          expression_EXT030 = (expression_EXT030.eq("EXCSNO", csno)).or(expression_EXT030.eq("EXCSNO", csno.substring(0,1)+"*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0,2)+"*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0,3)+"*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0,4)+"*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0,5)+"*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0,6)+"*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0,7)+"*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0,8)+"*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0,9)+"*")).or(expression_EXT030.eq("EXCSNO", ""))
        if(csno.toString().length() == 8)
          expression_EXT030 = (expression_EXT030.eq("EXCSNO", csno)).or(expression_EXT030.eq("EXCSNO", csno.substring(0,1)+"*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0,2)+"*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0,3)+"*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0,4)+"*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0,5)+"*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0,6)+"*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0,7)+"*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0,8)+"*")).or(expression_EXT030.eq("EXCSNO", ""))
        if(csno.toString().length() == 7)
          expression_EXT030 = (expression_EXT030.eq("EXCSNO", csno)).or(expression_EXT030.eq("EXCSNO", csno.substring(0,1)+"*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0,2)+"*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0,3)+"*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0,4)+"*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0,5)+"*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0,6)+"*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0,7)+"*")).or(expression_EXT030.eq("EXCSNO", ""))
        if(csno.toString().length() == 6)
          expression_EXT030 = (expression_EXT030.eq("EXCSNO", csno)).or(expression_EXT030.eq("EXCSNO", csno.substring(0,1)+"*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0,2)+"*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0,3)+"*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0,4)+"*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0,5)+"*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0,6)+"*")).or(expression_EXT030.eq("EXCSNO", ""))
        if(csno.toString().length() == 5)
          expression_EXT030 = (expression_EXT030.eq("EXCSNO", csno)).or(expression_EXT030.eq("EXCSNO", csno.substring(0,1)+"*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0,2)+"*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0,3)+"*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0,4)+"*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0,5)+"*")).or(expression_EXT030.eq("EXCSNO", ""))
        if(csno.toString().length() == 4)
          expression_EXT030 = (expression_EXT030.eq("EXCSNO", csno)).or(expression_EXT030.eq("EXCSNO", csno.substring(0,1)+"*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0,2)+"*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0,3)+"*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0,4)+"*")).or(expression_EXT030.eq("EXCSNO", ""))
        if(csno.toString().length() == 3)
          expression_EXT030 = (expression_EXT030.eq("EXCSNO", csno)).or(expression_EXT030.eq("EXCSNO", csno.substring(0,1)+"*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0,2)+"*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0,3)+"*")).or(expression_EXT030.eq("EXCSNO", ""))
        if(csno.toString().length() == 2)
          expression_EXT030 = (expression_EXT030.eq("EXCSNO", csno)).or(expression_EXT030.eq("EXCSNO", csno.substring(0,1)+"*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0,2)+"*")).or(expression_EXT030.eq("EXCSNO", ""))
        if(csno.toString().length() == 1)
          expression_EXT030 = (expression_EXT030.eq("EXCSNO", csno)).or(expression_EXT030.eq("EXCSNO", csno.substring(0,1)+"*")).or(expression_EXT030.eq("EXCSNO", ""))
      } else {
        if(csno.toString().length() == 16)
          expression_EXT030 = expression_EXT030.and((expression_EXT030.eq("EXCSNO", csno)).or(expression_EXT030.eq("EXCSNO", csno.substring(0,1)+"*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0,2)+"*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0,3)+"*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0,4)+"*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0,5)+"*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0,6)+"*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0,7)+"*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0,8)+"*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0,9)+"*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0,10)+"*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0,11)+"*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0,12)+"*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0,13)+"*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0,14)+"*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0,15)+"*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0,16)+"*")).or(expression_EXT030.eq("EXCSNO", "")))
        if(csno.toString().length() == 15)
          expression_EXT030 = expression_EXT030.and((expression_EXT030.eq("EXCSNO", csno)).or(expression_EXT030.eq("EXCSNO", csno.substring(0,1)+"*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0,2)+"*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0,3)+"*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0,4)+"*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0,5)+"*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0,6)+"*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0,7)+"*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0,8)+"*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0,9)+"*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0,10)+"*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0,11)+"*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0,12)+"*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0,13)+"*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0,14)+"*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0,15)+"*")).or(expression_EXT030.eq("EXCSNO", "")))
        if(csno.toString().length() == 14)
          expression_EXT030 = expression_EXT030.and((expression_EXT030.eq("EXCSNO", csno)).or(expression_EXT030.eq("EXCSNO", csno.substring(0,1)+"*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0,2)+"*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0,3)+"*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0,4)+"*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0,5)+"*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0,6)+"*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0,7)+"*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0,8)+"*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0,9)+"*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0,10)+"*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0,11)+"*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0,12)+"*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0,13)+"*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0,14)+"*")).or(expression_EXT030.eq("EXCSNO", "")))
        if(csno.toString().length() == 13)
          expression_EXT030 = expression_EXT030.and((expression_EXT030.eq("EXCSNO", csno)).or(expression_EXT030.eq("EXCSNO", csno.substring(0,1)+"*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0,2)+"*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0,3)+"*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0,4)+"*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0,5)+"*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0,6)+"*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0,7)+"*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0,8)+"*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0,9)+"*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0,10)+"*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0,11)+"*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0,12)+"*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0,13)+"*")).or(expression_EXT030.eq("EXCSNO", "")))
        if(csno.toString().length() == 12)
          expression_EXT030 = expression_EXT030.and((expression_EXT030.eq("EXCSNO", csno)).or(expression_EXT030.eq("EXCSNO", csno.substring(0,1)+"*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0,2)+"*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0,3)+"*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0,4)+"*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0,5)+"*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0,6)+"*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0,7)+"*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0,8)+"*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0,9)+"*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0,10)+"*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0,11)+"*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0,12)+"*")).or(expression_EXT030.eq("EXCSNO", "")))
        if(csno.toString().length() == 11)
          expression_EXT030 = expression_EXT030.and((expression_EXT030.eq("EXCSNO", csno)).or(expression_EXT030.eq("EXCSNO", csno.substring(0,1)+"*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0,2)+"*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0,3)+"*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0,4)+"*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0,5)+"*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0,6)+"*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0,7)+"*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0,8)+"*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0,9)+"*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0,10)+"*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0,11)+"*")).or(expression_EXT030.eq("EXCSNO", "")))
        if(csno.toString().length() == 10)
          expression_EXT030 = expression_EXT030.and((expression_EXT030.eq("EXCSNO", csno)).or(expression_EXT030.eq("EXCSNO", csno.substring(0,1)+"*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0,2)+"*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0,3)+"*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0,4)+"*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0,5)+"*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0,6)+"*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0,7)+"*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0,8)+"*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0,9)+"*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0,10)+"*")).or(expression_EXT030.eq("EXCSNO", "")))
        if(csno.toString().length() == 9)
          expression_EXT030 = expression_EXT030.and((expression_EXT030.eq("EXCSNO", csno)).or(expression_EXT030.eq("EXCSNO", csno.substring(0,1)+"*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0,2)+"*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0,3)+"*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0,4)+"*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0,5)+"*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0,6)+"*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0,7)+"*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0,8)+"*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0,9)+"*")).or(expression_EXT030.eq("EXCSNO", "")))
        if(csno.toString().length() == 8)
          expression_EXT030 = expression_EXT030.and((expression_EXT030.eq("EXCSNO", csno)).or(expression_EXT030.eq("EXCSNO", csno.substring(0,1)+"*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0,2)+"*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0,3)+"*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0,4)+"*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0,5)+"*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0,6)+"*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0,7)+"*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0,8)+"*")).or(expression_EXT030.eq("EXCSNO", "")))
        if(csno.toString().length() == 7)
          expression_EXT030 = expression_EXT030.and((expression_EXT030.eq("EXCSNO", csno)).or(expression_EXT030.eq("EXCSNO", csno.substring(0,1)+"*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0,2)+"*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0,3)+"*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0,4)+"*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0,5)+"*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0,6)+"*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0,7)+"*")).or(expression_EXT030.eq("EXCSNO", "")))
        if(csno.toString().length() == 6)
          expression_EXT030 = expression_EXT030.and((expression_EXT030.eq("EXCSNO", csno)).or(expression_EXT030.eq("EXCSNO", csno.substring(0,1)+"*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0,2)+"*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0,3)+"*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0,4)+"*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0,5)+"*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0,6)+"*")).or(expression_EXT030.eq("EXCSNO", "")))
        if(csno.toString().length() == 5)
          expression_EXT030 = expression_EXT030.and((expression_EXT030.eq("EXCSNO", csno)).or(expression_EXT030.eq("EXCSNO", csno.substring(0,1)+"*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0,2)+"*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0,3)+"*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0,4)+"*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0,5)+"*")).or(expression_EXT030.eq("EXCSNO", "")))
        if(csno.toString().length() == 4)
          expression_EXT030 = expression_EXT030.and((expression_EXT030.eq("EXCSNO", csno)).or(expression_EXT030.eq("EXCSNO", csno.substring(0,1)+"*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0,2)+"*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0,3)+"*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0,4)+"*")).or(expression_EXT030.eq("EXCSNO", "")))
        if(csno.toString().length() == 3)
          expression_EXT030 = expression_EXT030.and((expression_EXT030.eq("EXCSNO", csno)).or(expression_EXT030.eq("EXCSNO", csno.substring(0,1)+"*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0,2)+"*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0,3)+"*")).or(expression_EXT030.eq("EXCSNO", "")))
        if(csno.toString().length() == 2)
          expression_EXT030 = expression_EXT030.and((expression_EXT030.eq("EXCSNO", csno)).or(expression_EXT030.eq("EXCSNO", csno.substring(0,1)+"*")).or(expression_EXT030.eq("EXCSNO", csno.substring(0,2)+"*")).or(expression_EXT030.eq("EXCSNO", "")))
        if(csno.toString().length() == 1)
          expression_EXT030 = expression_EXT030.and((expression_EXT030.eq("EXCSNO", csno)).or(expression_EXT030.eq("EXCSNO", csno.substring(0,1)+"*")).or(expression_EXT030.eq("EXCSNO", "")))
      }
    } else {
      if(cuno == "" && constraint_CSCD == "" && hazi == 0 && hie5 == "" && cfi4 == "" && popn == ""){
        expression_EXT030 = (expression_EXT030.eq("EXCSNO", csno)).or(expression_EXT030.eq("EXCSNO", ""))
      } else {
        expression_EXT030 = expression_EXT030.and((expression_EXT030.eq("EXCSNO", csno)).or(expression_EXT030.eq("EXCSNO", "")))
      }
    }
    if(cuno == "" && constraint_CSCD == "" && hazi == 0 && hie5 == "" && cfi4 == "" && popn == "" && csno == ""){
      expression_EXT030 = (expression_EXT030.eq("EXORCO", cscd)).or(expression_EXT030.eq("EXORCO", ""))
    } else {
      expression_EXT030 = expression_EXT030.and((expression_EXT030.eq("EXORCO", cscd)).or(expression_EXT030.eq("EXORCO", "")))
    }
    if(cuno == "" && constraint_CSCD == "" && hazi == 0 && hie5 == "" && cfi4 == "" && popn == "" && csno == "" && cscd == ""){
      expression_EXT030 = (expression_EXT030.eq("EXZALC", zalc as String)).or(expression_EXT030.eq("EXZALC", "2"))
    } else {
      expression_EXT030 = expression_EXT030.and((expression_EXT030.eq("EXZALC", zalc as String)).or(expression_EXT030.eq("EXZALC", "2")))
    }
    if(cuno == "" && constraint_CSCD == "" && hazi == 0 && hie5 == "" && cfi4 == "" && popn == "" && csno == "" && cscd == "" && zalc == 0){
      expression_EXT030 = (expression_EXT030.eq("EXZSAN", zsan as String)).or(expression_EXT030.eq("EXZSAN", "2"))
    } else {
      expression_EXT030 = expression_EXT030.and((expression_EXT030.eq("EXZSAN", zsan as String)).or(expression_EXT030.eq("EXZSAN", "2")))
    }
    if(cuno == "" && constraint_CSCD == "" && hazi == 0 && hie5 == "" && cfi4 == "" && popn == "" && csno == "" && cscd == "" && zalc == 0 && zsan == 0){
      expression_EXT030 = ((expression_EXT030.eq("EXZCAP", zca1)).or(expression_EXT030.eq("EXZCAP", "")))
    } else {
      expression_EXT030 = expression_EXT030.and((expression_EXT030.eq("EXZCAP", zca1)).or(expression_EXT030.eq("EXZCAP", "")))
    }
    if(cuno == "" && constraint_CSCD == "" && hazi == 0 && hie5 == "" && cfi4 == "" && popn == "" && csno == "" && cscd == "" && zalc == 0 && zsan == 0){
      expression_EXT030 = ((expression_EXT030.eq("EXZCAS", zca1)).or(expression_EXT030.eq("EXZCAS", zca2)).or(expression_EXT030.eq("EXZCAS", zca3)).or(expression_EXT030.eq("EXZCAS", zca4)).or(expression_EXT030.eq("EXZCAS", zca5)).or(expression_EXT030.eq("EXZCAS", zca6)).or(expression_EXT030.eq("EXZCAS", zca7)).or(expression_EXT030.eq("EXZCAS", zca8)).or(expression_EXT030.eq("EXZCAS", ""))).or(expression_EXT030.eq("EXZCAS", ""))
    } else {
      expression_EXT030 = expression_EXT030.and((expression_EXT030.eq("EXZCAS", zca1)).or(expression_EXT030.eq("EXZCAS", zca2)).or(expression_EXT030.eq("EXZCAS", zca3)).or(expression_EXT030.eq("EXZCAS", zca4)).or(expression_EXT030.eq("EXZCAS", zca5)).or(expression_EXT030.eq("EXZCAS", zca6)).or(expression_EXT030.eq("EXZCAS", zca7)).or(expression_EXT030.eq("EXZCAS", zca8)).or(expression_EXT030.eq("EXZCAS", "")))
    }
    if(znag != "") {
      if(cuno == "" && constraint_CSCD == "" && hazi == 0 && hie5 == "" && cfi4 == "" && popn == "" && csno == "" && cscd == "" && zalc == 0 && zsan == 0 && zca1 == "" && zca2 == "" && zca3 == "" && zca4 == "" && zca5 == "" && zca6 == "" && zca7 == "" && zca8 == ""){
        expression_EXT030 = (expression_EXT030.eq("EXZNAG", znag)).or(expression_EXT030.eq("EXZNAG", znag.substring(0,4)+"*")).or(expression_EXT030.eq("EXZNAG", ""))
      } else {
        expression_EXT030 = expression_EXT030.and((expression_EXT030.eq("EXZNAG", znag)).or(expression_EXT030.eq("EXZNAG", znag.substring(0,4)+"*")).or(expression_EXT030.eq("EXZNAG", "")))
      }
    } else {
      if(cuno == "" && constraint_CSCD == "" && hazi == 0 && hie5 == "" && cfi4 == "" && popn == "" && csno == "" && cscd == "" && zalc == 0 && zsan == 0 && zca1 == "" && zca2 == "" && zca3 == "" && zca4 == "" && zca5 == "" && zca6 == "" && zca7 == "" && zca8 == ""){
        expression_EXT030 = (expression_EXT030.eq("EXZNAG", znag)).or(expression_EXT030.eq("EXZNAG", ""))
      } else {
        expression_EXT030 = expression_EXT030.and((expression_EXT030.eq("EXZNAG", znag)).or(expression_EXT030.eq("EXZNAG", "")))
      }
    }
    if(cuno == "" && constraint_CSCD == "" && hazi == 0 && hie5 == "" && cfi4 == "" && popn == "" && csno == "" && cscd == "" && zalc == 0 && zsan == 0 && zca1 == "" && zca2 == "" && zca3 == "" && zca4 == "" && zca5 == "" && zca6 == "" && zca7 == "" && zca8 == "" && znag == ""){
      expression_EXT030 = (expression_EXT030.eq("EXZALI", zali as String)).or(expression_EXT030.eq("EXZALI", "2"))
    } else {
      expression_EXT030 = expression_EXT030.and((expression_EXT030.eq("EXZALI", zali as String)).or(expression_EXT030.eq("EXZALI", "2")))
    }
    if(cuno == "" && constraint_CSCD == "" && hazi == 0 && hie5 == "" && cfi4 == "" && popn == "" && csno == "" && cscd == "" && zalc == 0 && zsan == 0 && zca1 == "" && zca2 == "" && zca3 == "" && zca4 == "" && zca5 == "" && zca6 == "" && zca7 == "" && zca8 == "" && znag == "" && zali == 0){
      expression_EXT030 = (expression_EXT030.eq("EXZORI", zori as String)).or(expression_EXT030.eq("EXZORI", "2"))
    } else {
      expression_EXT030 = expression_EXT030.and((expression_EXT030.eq("EXZORI", zori as String)).or(expression_EXT030.eq("EXZORI", "2")))
    }
    if(cuno == "" && constraint_CSCD == "" && hazi == 0 && hie5 == "" && cfi4 == "" && popn == "" && csno == "" && cscd == "" && zalc == 0 && zsan == 0 && zca1 == "" && zca2 == "" && zca3 == "" && zca4 == "" && zca5 == "" && zca6 == "" && zca7 == "" && zca8 == "" && znag == "" && zali == 0 && zori == 0){
      expression_EXT030 = (expression_EXT030.eq("EXZPHY", zphy as String)).or(expression_EXT030.eq("EXZPHY", "2"))
    } else {
      expression_EXT030 = expression_EXT030.and((expression_EXT030.eq("EXZPHY", zphy as String)).or(expression_EXT030.eq("EXZPHY", "2")))
    }
    if(cuno == "" && constraint_CSCD == "" && hazi == 0 && hie5 == "" && cfi4 == "" && popn == "" && csno == "" && cscd == "" && zalc == 0 && zsan == 0 && zca1 == "" && zca2 == "" && zca3 == "" && zca4 == "" && zca5 == "" && zca6 == "" && zca7 == "" && zca8 == "" && znag == "" && zali == 0 && zori == 0 && zphy == 0){
      expression_EXT030 = (expression_EXT030.eq("EXZOHF", zohf as String)).or(expression_EXT030.eq("EXZOHF", "2"))
    } else {
      expression_EXT030 = expression_EXT030.and((expression_EXT030.eq("EXZOHF", zohf as String)).or(expression_EXT030.eq("EXZOHF", "2")))
    }
    DBAction EXT030_query = database.table("EXT030").index("10").matching(expression_EXT030).selection("EXZCID", "EXZCOD", "EXZBLO").build()
    DBContainer EXT030 = EXT030_query.getContainer()
    EXT030.set("EXCONO", currentCompany)
    EXT030.set("EXZBLO", 1)
    EXT030.set("EXSTAT", "20")
    if(!EXT030_query.readAll(EXT030, 3, outData_EXT030)){
      //logger.debug("-----------------------------------EXT030 not found-----------------------------------")
    }
    return constraint_isOK
  }
  // Retrieve EXT021
  Closure<?> outData_EXT021_2 = { DBContainer EXT021 ->
    data = EXT021.get("EXDATA")
  }
  // Retrieve EXT030
  Closure<?> outData_EXT030 = { DBContainer EXT030 ->
    constraint_isOK = false
    logger.debug("Constraint found - ZCID = " + EXT030.get("EXZCID"))
  }
  // Retrieve EXT010
  Closure<?> outData_EXT010 = { DBContainer EXT010 ->
    itno = EXT010.get("EXITNO")
    cmde = EXT010.get("EXCMDE")
    sule = EXT010.get("EXSULE")
    logger.debug("Lecture EXT010 - itno = " + itno)
    // Read items et insert in EXT022 the selected items
    if (itno != "") {
      //logger.debug("EXT022MI SelAssortItems = Read one MITMAS")
      DBAction MITMAS_query = database.table("MITMAS").index("00").selection("MMSTAT", "MMPROD", "MMHIE1", "MMHIE2", "MMHIE3", "MMHIE4", "MMHIE5", "MMBUAR", "MMCFI1", "MMCFI2", "MMSPE1", "MMHAZI", "MMCFI4", "MMSUNO").build()
      DBContainer MITMAS = MITMAS_query.getContainer()
      MITMAS.set("MMCONO", currentCompany)
      MITMAS.set("MMITNO", itno)
      if (!MITMAS_query.readAll(MITMAS, 2, outData_MITMAS)) {}
    }
  }
  // Retrieve EXT020
  Closure<?> outData_EXT020 = { DBContainer EXT020 ->
  }
  // Retrieve EXT081
  Closure<?> outData_EXT081 = { DBContainer EXT081 ->
    if(EXT081.get("EXSTAT") != "90")
      Status = EXT081.get("EXSTAT")
  }
  // Retrieve EXT021
  Closure<?> outData_EXT021 = { DBContainer EXT021 ->
    criteria_found = true
    // Initialization of a boolean for each criteria: 0 = not used, 1 = inclusion, 2 = exclusion
    if (EXT021.get("EXTYPE") == "SUNO" && EXT021.get("EXCHB1") == 0) SUNO = 1
    if (EXT021.get("EXTYPE") == "SUNO" && EXT021.get("EXCHB1") == 1) SUNO = 2
    if (EXT021.get("EXTYPE") == "PROD" && EXT021.get("EXCHB1") == 0) PROD = 1
    if (EXT021.get("EXTYPE") == "PROD" && EXT021.get("EXCHB1") == 1) PROD = 2
    if (EXT021.get("EXTYPE") == "HIE1" && EXT021.get("EXCHB1") == 0) HIE1 = 1
    if (EXT021.get("EXTYPE") == "HIE1" && EXT021.get("EXCHB1") == 1) HIE1 = 2
    if (EXT021.get("EXTYPE") == "HIE2" && EXT021.get("EXCHB1") == 0) HIE2 = 1
    if (EXT021.get("EXTYPE") == "HIE2" && EXT021.get("EXCHB1") == 1) HIE2 = 2
    if (EXT021.get("EXTYPE") == "HIE3" && EXT021.get("EXCHB1") == 0) HIE3 = 1
    if (EXT021.get("EXTYPE") == "HIE3" && EXT021.get("EXCHB1") == 1) HIE3 = 2
    if (EXT021.get("EXTYPE") == "HIE4" && EXT021.get("EXCHB1") == 0) HIE4 = 1
    if (EXT021.get("EXTYPE") == "HIE4" && EXT021.get("EXCHB1") == 1) HIE4 = 2
    if (EXT021.get("EXTYPE") == "HIE5" && EXT021.get("EXCHB1") == 0) HIE5 = 1
    if (EXT021.get("EXTYPE") == "HIE5" && EXT021.get("EXCHB1") == 1) HIE5 = 2
    if (EXT021.get("EXTYPE") == "BUAR" && EXT021.get("EXCHB1") == 0) BUAR = 1
    if (EXT021.get("EXTYPE") == "BUAR" && EXT021.get("EXCHB1") == 1) BUAR = 2
    if (EXT021.get("EXTYPE") == "CFI1" && EXT021.get("EXCHB1") == 0) CFI1 = 1
    if (EXT021.get("EXTYPE") == "CFI1" && EXT021.get("EXCHB1") == 1) CFI1 = 2
    if (EXT021.get("EXTYPE") == "CSCD" && EXT021.get("EXCHB1") == 0) CSCD = 1
    if (EXT021.get("EXTYPE") == "CSCD" && EXT021.get("EXCHB1") == 1) CSCD = 2
    if (EXT021.get("EXTYPE") == "CSNO" && EXT021.get("EXCHB1") == 0) CSNO = 1
    if (EXT021.get("EXTYPE") == "CSNO" && EXT021.get("EXCHB1") == 1) CSNO = 2
    if (EXT021.get("EXTYPE") == "CFI2" && EXT021.get("EXCHB1") == 0) CFI2 = 1
    if (EXT021.get("EXTYPE") == "CFI2" && EXT021.get("EXCHB1") == 1) CFI2 = 2
    if (EXT021.get("EXTYPE") == "ITNO" && EXT021.get("EXCHB1") == 0) ITNO = 1
    if (EXT021.get("EXTYPE") == "ITNO" && EXT021.get("EXCHB1") == 1) ITNO = 2
    if (EXT021.get("EXTYPE") == "POPN" && EXT021.get("EXCHB1") == 0) POPN = 1
    if (EXT021.get("EXTYPE") == "POPN" && EXT021.get("EXCHB1") == 1) POPN = 2
    if (EXT021.get("EXTYPE") == "ULTY" && EXT021.get("EXCHB1") == 0) ULTY = 1
    if (EXT021.get("EXTYPE") == "ULTY" && EXT021.get("EXCHB1") == 1) ULTY = 2
    if (EXT021.get("EXTYPE") == "SLDY" && EXT021.get("EXCHB1") == 0) SLDY = 1
    if (EXT021.get("EXTYPE") == "SLDY" && EXT021.get("EXCHB1") == 1) SLDY = 2
    if (EXT021.get("EXTYPE") == "CPFX" && EXT021.get("EXCHB1") == 0) CPFX = 1
    if (EXT021.get("EXTYPE") == "CPFX" && EXT021.get("EXCHB1") == 1) CPFX = 2
    if (EXT021.get("EXTYPE") == "CMDE" && EXT021.get("EXCHB1") == 0) CMDE = 1
    if (EXT021.get("EXTYPE") == "CMDE" && EXT021.get("EXCHB1") == 1) CMDE = 2
    if (EXT021.get("EXTYPE") == "GOLD" && EXT021.get("EXCHB1") == 0) GOLD = 1
    if (EXT021.get("EXTYPE") == "GOLD" && EXT021.get("EXCHB1") == 1) GOLD = 2
  }
  // Retrieve CUGEX1
  Closure<?> outData_CUGEX1 = { DBContainer CUGEX1 ->
    ulty = CUGEX1.get("F1A030")
    cpfx = CUGEX1.get("F1A830")
  }
  Closure<?> outData_MITPOP = { DBContainer MITPOP ->
    //logger.debug("found MITPOP")
    popn = MITPOP.get("MPPOPN")
    //logger.debug("popn = " + popn)
  }
  // Exceute EXT023MI AddAssortItems
  private executeEXT023MIAddAssortItems(String ASCD, String CUNO, String FDAT){
    def parameters = ["ASCD": ASCD, "CUNO": CUNO, "FDAT": FDAT]
    Closure<?> handler = { Map<String, String> response ->
      if (response.error != null) {
        String header = "MSG"
        String message = "Failed EXT023MI.AddAssortItems: "+ response.errorMessage
        logMessage(header, message)
        return
      } else {
      }
    }
    miCaller.call("EXT023MI", "AddAssortItems", parameters, handler)
  }
  // Execute EXT820MI.SubmitBatch
  private executeEXT820MISubmitBatch(String CONO, String JOID, String P001, String P002, String P003, String P004, String P005, String P006, String P007, String P008){
    def parameters = ["CONO": CONO, "JOID": JOID, "P001": P001, "P002": P002, "P003": P003, "P004": P004, "P005": P005, "P006": P006, "P007": P007, "P008": P008]
    Closure<?> handler = { Map<String, String> response ->
      if (response.error != null) {
      } else {
      }
    }
    miCaller.call("EXT820MI", "SubmitBatch", parameters, handler)
  }
  // Execute EXT023MI UpdAssortItems
  private executeEXT023MIUpdAssortItems(String ASCD, String CUNO, String FDAT){
    def parameters = ["ASCD": ASCD, "CUNO": CUNO, "FDAT": FDAT]
    Closure<?> handler = { Map<String, String> response ->
      if (response.error != null) {
        String header = "MSG"
        String message = "Failed EXT023MI.UpdAssortItems: "+ response.errorMessage
        logMessage(header, message)
        return
      } else {
      }
    }
    miCaller.call("EXT023MI", "UpdAssortItems", parameters, handler)
  }
  // Update CUGEX1
  public void Update_CUGEX1(String status, String count){
    DBAction CUGEX1_query = database.table("CUGEX1").index("00").build()
    DBContainer CUGEX1 = CUGEX1_query.getContainer()
    CUGEX1.set("F1CONO", currentCompany)
    CUGEX1.set("F1FILE", "OASCUS")
    CUGEX1.set("F1PK01", ascd)
    CUGEX1.set("F1PK02", cuno)
    CUGEX1.set("F1PK03", fdat)
    if (!CUGEX1_query.read(CUGEX1)) {
      //logger.debug("AddFieldValue ascd = " + ascd)
      //logger.debug("AddFieldValue cuno = " + cuno)
      //logger.debug("AddFieldValue fdat = " + fdat)
      //logger.debug("AddFieldValue status = " + status)
      //logger.debug("AddFieldValue count = " + count)
      executeCUSEXTMIAddFieldValue("OASCUS", ascd, cuno, fdat, "", "", "", "", "", status, count)
    } else {
      executeCUSEXTMIChgFieldValue("OASCUS", ascd, cuno, fdat, "", "", "", "", "", status, count)
    }
  }
  // Update EXT080 & EXT081
  public void Update_EXT080_EXT081(){
    // Update status to 90
    DBAction query = database.table("EXT081").index("00").build()
    DBContainer EXT081 = query.getContainer()
    EXT081.set("EXCONO", currentCompany)
    EXT081.set("EXPRRF", prrf)
    EXT081.set("EXCUCD", cucd)
    //EXT081.set("EXCUNO", cuno)
    EXT081.set("EXCUNO", cunt)
    EXT081.set("EXFVDT", fvdt as Integer)
    EXT081.set("EXASCD", ascd)
    EXT081.set("EXFDAT", fdat as Integer)
    if(!query.readLock(EXT081, updateCallBack_EXT081)){}
    //logger.debug(("Step 1"))
    Status = "90"
    DBAction queryEXT081 = database.table("EXT081").index("00").selection("EXSTAT").build()
    DBContainer EXT081_2 = queryEXT081.getContainer()
    EXT081_2.set("EXCONO", currentCompany)
    EXT081_2.set("EXPRRF", prrf)
    EXT081_2.set("EXCUCD", cucd)
    //EXT081_2.set("EXCUNO", cuno)
    EXT081_2.set("EXCUNO", cunt)
    EXT081_2.set("EXFVDT", fvdt as Integer)
    if(!queryEXT081.readAll(EXT081_2, 5, outData_EXT081)){}
    //logger.debug("Step 2 Status = " + Status)
    if (Status == "90"){
      //logger.debug(("Step 3"))
      // Update EXT080 status to 70 (Assortments updated)
      DBAction queryEXT080 = database.table("EXT080").index("00").build()
      DBContainer EXT080 = queryEXT080.getContainer()
      EXT080.set("EXCONO", currentCompany)
      EXT080.set("EXPRRF", prrf)
      EXT080.set("EXCUCD", cucd)
      //EXT080.set("EXCUNO", cuno)
      EXT080.set("EXCUNO", cunt)
      EXT080.set("EXFVDT", fvdt as Integer)
      if(!queryEXT080.readLock(EXT080, updateCallBack_EXT080)){}
    }
  }
  // Update EXT080
  Closure<?> updateCallBack_EXT080 = { LockedResult lockedResult ->
    LocalDateTime timeOfCreation = LocalDateTime.now()
    int changeNumber = lockedResult.get("EXCHNO")
    // Update status to 70
    lockedResult.set("EXSTAT", "70")
    lockedResult.setInt("EXLMDT", timeOfCreation.format(DateTimeFormatter.ofPattern("yyyyMMdd")) as Integer)
    lockedResult.setInt("EXCHNO", changeNumber + 1)
    lockedResult.set("EXCHID", program.getUser())
    lockedResult.update()
  }
  // Update EXT081
  Closure<?> updateCallBack_EXT081 = { LockedResult lockedResult ->
    LocalDateTime timeOfCreation = LocalDateTime.now()
    int changeNumber = lockedResult.get("EXCHNO")
    // Update status to 90
    lockedResult.set("EXSTAT", "90")
    lockedResult.setInt("EXLMDT", timeOfCreation.format(DateTimeFormatter.ofPattern("yyyyMMdd")) as Integer)
    lockedResult.setInt("EXCHNO", changeNumber + 1)
    lockedResult.set("EXCHID", program.getUser())
    lockedResult.update()
  }
  // Execute CUSEXTMI AddFieldValue
  private executeCUSEXTMIAddFieldValue(String FILE, String PK01, String PK02, String PK03, String PK04, String PK05, String PK06, String PK07, String PK08, String A030, String N096){
    def parameters = ["FILE": FILE, "PK01": PK01, "PK02": PK02, "PK03": PK03, "PK04": PK04, "PK05": PK05, "PK06": PK06, "PK07": PK07, "PK08": PK08, "A030": A030, "N096": N096]
    Closure<?> handler = { Map<String, String> response ->
      if (response.error != null) {
        String header = "MSG"
        String message = "Failed CUSEXTMI.AddFieldValue: "+ response.errorMessage
        logMessage(header, message)
        return
      } else {
      }
    }
    miCaller.call("CUSEXTMI", "AddFieldValue", parameters, handler)
  }
  // Exceute CUSEXTMI ChgFieldValue
  private executeCUSEXTMIChgFieldValue(String FILE, String PK01, String PK02, String PK03, String PK04, String PK05, String PK06, String PK07, String PK08, String A030, String N096){
    def parameters = ["FILE": FILE, "PK01": PK01, "PK02": PK02, "PK03": PK03, "PK04": PK04, "PK05": PK05, "PK06": PK06, "PK07": PK07, "PK08": PK08, "A030": A030, "N096": N096]
    Closure<?> handler = { Map<String, String> response ->
      if (response.error != null) {
        String header = "MSG"
        String message = "Failed CUSEXTMI.ChgFieldValue: "+ response.errorMessage
        logMessage(header, message)
        return
      } else {
      }
    }
    miCaller.call("CUSEXTMI", "ChgFieldValue", parameters, handler)
  }
  // Get first parameter
  private String getFirstParameter(){
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
  private String getNextParameter(){
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
  public void deleteEXTJOB(){
    LocalDateTime timeOfCreation = LocalDateTime.now()
    DBAction query = database.table("EXTJOB").index("00").build()
    DBContainer EXTJOB = query.getContainer()
    EXTJOB.set("EXRFID", batch.getReferenceId().get())
    if(!query.readAllLock(EXTJOB, 1, updateCallBack_EXTJOB)){
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
    if(!textFiles.exists(logFileName)) {
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
