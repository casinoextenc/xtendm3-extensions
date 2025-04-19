import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import mvx.db.common.PositionKey
import mvx.db.common.PositionEmpty

/**
 * Name : EXT022
 * COMX01 Gestion des assortiments clients
 * Description : Select the items based on the criteria contained in EXT021 table and add records to the EXT022 table (EXT022MI.SelAssortItems conversion)
 * Date         Changed By   Description
 * 20220112     YBLUTEAU     COMX01- Add assortment
 * 20230303     ARENARD      Constraints handling has been added and adjustments have been made
 * 20230717     ARENARD      csno handling has been fixed
 * 20240620     FLEBARS      COMX01 - Controle code pour validation Infor
 * 20240409     PBEAUDOUIN   COMX01 - Check for approval
 * 20240417     Sear         COMX01 - Check for approval
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

  private String ascd = ""
  private String cuno = ""
  private String fdat = ""
  private String fvdt = ""
  private String stat = ""
  private Integer iSuno = 0
  private String suno = ""
  private Integer iProd = 0
  private String prod = ""
  private Integer iHie1 = 0
  private String hie1 = ""
  private Integer iHie2 = 0
  private String hie2 = ""
  private Integer iHie3 = 0
  private String hie3 = ""
  private Integer iHie4 = 0
  private String hie4 = ""
  private Integer iHie5 = 0
  private String hie5 = ""
  private Integer iBuar = 0
  private String buar = ""
  private Integer iCfi1 = 0
  private String cfi1 = ""
  private Integer iCscd = 0
  private String cscd = ""
  private Integer iCsno = 0
  private String csno = ""
  private Integer iCfi2 = 0
  private String cfi2 = ""
  private Integer iItno = 0
  private String itno = ""
  private String lastItno = ""
  private Integer iPopn = 0
  private String popn = ""
  private Integer iUlty = 0
  private String ulty = ""
  private Integer iSldy = 0
  private String sldy = ""
  private Integer iCpfx = 0
  private String cpfx = ""
  private Integer iCmde = 0
  private String cmde = ""
  private Integer iGold = 0
  private String spe1 = ""
  private String hazi
  private String cfi4
  private Integer zali
  private String constraintCscd
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
  private Integer itemsInOasitn = 0
  private Integer countExt010Records = 0
  private String rawData
  private int rawDataLength
  private int beginIndex
  private int endIndex
  private String jobNumber
  private String referenceId

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


    if (batch.getReferenceId().isPresent()) {
      referenceId = batch.getReferenceId()
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
    String inAscd = getFirstParameter()
    String inCuno = getNextParameter()
    String inFdat = getNextParameter()
    String inItno = getNextParameter()
    String inOpt2 = getNextParameter()
    String inFvdt = getNextParameter()
    String inLevl = getNextParameter()

    LocalDateTime timeOfCreation = LocalDateTime.now()
    ascd = inAscd
    cuno = inCuno

    if (inFdat != null && !inFdat.trim().isBlank()) {
      fdat = inFdat
      if (!utility.call("DateUtil", "isDateValid", fdat, "yyyyMMdd")) {
        String message = "Date de début est incorrecte " + ";" + fdat
        logMessage("ERROR", message)
        return
      }
    } else {
      fdat = "0"
    }
    logMessage("INFO", "start job EXT022 ascd:${ascd} cuno:${cuno} fdat:${fdat} itno:${inItno} opt2:${inOpt2} fvdt:${inFvdt} loglevel:${inLevl}")

    // Check option
    if (inOpt2 == null && !inOpt2.trim().isBlank()) {
      String message = "Option est obligatoire"
      logMessage("ERROR", message)
      return
    }
    if (inOpt2 != "1" && inOpt2 != "2") {
      String opt2 = inOpt2
      String header = "MSG"
      String message = "Option ${opt2} est invalide"
      logMessage("ERROR", message)
      return
    }

    // Check date
    if (inFvdt != null && !inFvdt.trim().isBlank()) {
      fvdt = inFvdt
      if (!utility.call("DateUtil", "isDateValid", fvdt, "yyyyMMdd")) {
        String header = "MSG"
        String message = "Date de début de validité ${fvdt} est incorrecte"
        logMessage("ERROR", message)
        return
      }
    } else {
      fvdt = "0"
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

    //Get customer country
    constraintCscd = ""
    DBAction ocusmaQuery = database.table("OCUSMA").index("00").selection("OKCSCD").build()
    DBContainer ocusmaRequest = ocusmaQuery.getContainer()
    ocusmaRequest.set("OKCONO", currentCompany)
    ocusmaRequest.set("OKCUNO", cuno)
    if (ocusmaQuery.read(ocusmaRequest)) {
      constraintCscd = ocusmaRequest.getString("OKCSCD")
    }
    // Check criteria used in the selection
    checkUsedCriteria()

    if (criteriaFound) {
      updateCugex1("10", "" + itemsInOasitn)

      // Delete file EXT022 last submission datas
      deleteExt022()

      ExpressionFactory ext010Expression = database.getExpressionFactory("EXT010")
      ext010Expression = ext010Expression.le("EXFVDT", timeOfCreation.format(DateTimeFormatter.ofPattern("yyyyMMdd")))
      ext010Expression = ext010Expression.and(ext010Expression.ge("EXLVDT", timeOfCreation.format(DateTimeFormatter.ofPattern("yyyyMMdd"))))
      if (!inItno.trim().isEmpty()) {
        ext010Expression = ext010Expression.and(ext010Expression.ge("EXITNO", inItno.trim()))
      }

      DBAction ext010Query = database.table("EXT010")
        .index("02")
        .matching(ext010Expression)
        .selection("EXCUNO", "EXITNO", "EXCMDE", "EXFVDT", "EXLVDT", "EXSULE", "EXSULD")
        .build()

      DBContainer ext010Request = ext010Query.getContainer()
      ext010Request.set("EXCONO", currentCompany)
      ext010Request.set("EXCUNO", cuno)

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
              .matching(ext010Expression)
              .selection("EXCUNO", "EXITNO", "EXCMDE", "EXFVDT", "EXLVDT", "EXSULE", "EXSULD")
              .position(position)
              .build()
          }
        }
        if (!ext010Query.readAll(ext010Request, 2, 10000, ext010Reader)) {
        }
        updateCugex1("90", "" + itemsInOasitn)
        nbIteration++
        if (nbIteration > 5) {//max 50 0000 records
          logMessage("ERROR", "Nombre d'itération trop important cuno:${cuno} ascd:${ascd} fdat:${fdat}")
          break
        }
      }
      logMessage("INFO", "End job EXT022 ascd:${ascd} cuno:${cuno} fdat:${fdat} nombre articles OASITN:${itemsInOasitn}")
    } else {
      logMessage("ERROR", "Pas de critères trouvés pour la selection ascd:${ascd} cuno:${cuno} fdat:${fdat}")
    }
    // Delete file EXTJOB
    deleteEXTJOB()
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
    getCriteriaValue()

    boolean itemOk = itemSelectionOK()
    if (itemOk) {
      logMessage("DEBUG", "Item selection : ITNO:${itno} ok:${itemOk}")
    }

    // Check if the item matches the selection
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
          itemsInOasitn++
        }
      }
    }
  }


  /**
   * Execute CRS105MI.AddAssmItem
   * @parameter ASCD
   * @parameter ITNO
   * @return
   */
  private executeCRS105MIAddAssmItem(String pAscd, String pItno, String pFdat) {
    Map<String, String> parameters = ["ASCD": pAscd, "ITNO": pItno, "FDAT": pFdat]
    Closure<?> handler = { Map<String, String> response ->
      if (response.error != null) {
        String message = "Failed CRS105MI.AddAssmItem: " + response.errorMessage
        logMessage("ERROR", message)
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

    if (!ext025Query.readAll(ext025Request, 3, 1, ext025Reader)) {
    }
    if (!exclu) {
      miCaller.call("CRS105MI", "AddAssmItem", parameters, handler)
    }
  }

  /**
   * Delete records related to the assortment from EXT022 table
   */
  public void deleteExt022() {
    DBAction ext022Query = database.table("EXT022").index("00").build()
    DBContainer ext022Request = ext022Query.getContainer()
    ext022Request.set("EXCONO", currentCompany)
    ext022Request.set("EXASCD", ascd)
    ext022Request.set("EXCUNO", cuno)
    ext022Request.set("EXFDAT", fdat as Integer)

    // Delete EXTJOB
    Closure<?> ext022Reader = { DBContainer ext022Result ->
      Closure<?> ext022Updater = { LockedResult ext022LockedResult ->
        ext022LockedResult.delete()
      }
      ext022Query.readLock(ext022Result, ext022Updater)
    }
    boolean doLoop = true
    int nbIteration = 0
    while (doLoop) {
      if (nbIteration > 0) {
        PositionKey position = ext022Query.getPositionKey()
        if (position instanceof PositionEmpty) {
          doLoop = false
          break
        } else {
          ext022Query = database.table("EXT022")
            .index("00")
            .position(position)
            .build()
        }
      }
      if (!ext022Query.readAll(ext022Request, 4, 10000, ext022Reader)) {
      }
      nbIteration++
      if (nbIteration > 5) {//max 50 0000 records
        logMessage("ERROR", "Nombre d'itération trop important cuno:${cuno} ascd:${ascd} fdat:${fdat}")
        break
      }
    }
  }
  /**
   * Retrieve criterias values
   */
  public void getCriteriaValue() {
    ulty = ""
    cpfx = ""
    if (iUlty != 0 || iCpfx != 0) {
      ExpressionFactory expressionCUGEX1 = database.getExpressionFactory("CUGEX1")
      DBAction queryCUGEX1 = database.table("CUGEX1").index("00").matching(expressionCUGEX1).selection("F1A030", "F1A830").build()
      DBContainer CUGEX1 = queryCUGEX1.getContainer()
      CUGEX1.set("F1CONO", currentCompany)
      CUGEX1.set("F1FILE", "MITMAS")
      CUGEX1.set("F1PK01", itno)
      if (!queryCUGEX1.readAll(CUGEX1, 3, 10000, outDataCUGEX1)) {
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
    if (stat >= "80"){
      logMessage("DEBUG", "Controle statut article : ITNO:${itno} stat:${stat} KO")
      return false
    }

    // Check double
    if (!checkDouble(cuno, itno, ext010Fvdt as String, ext010Lvdt as String)) {
      logMessage("DEBUG", "Controle doublon : ITNO:${itno} fvdt:${ext010Fvdt} lvdt:${ext010Lvdt} KO")
      return false
    }

    //chech sule / suld
    if (sule.length() == 0 && suld.length() == 0) {
      logMessage("DEBUG", "Controle filiere vide : ITNO:${itno} KO")
      return false
    }

    //chech suno / itno
    if (sule.length() > 0) {
      if (!checkSunoItno(sule, itno)) {
        logMessage("DEBUG", "Controle filiere entrepot : ITNO:${itno} sule:${sule} KO")
        return false
      }
    } else {
      if (!checkSunoItno(suld, itno)) {
        logMessage("DEBUG", "Controle filiere directe : ITNO:${itno} suld:${suld}  KO")
        return false
      }
    }

    //check price
    price = 0
    if (sule.length() > 0) {
      executePPS106MIGetPrice(itno, sule)
      if (price == 0) {
        logMessage("DEBUG", "Controle prix de cession : ITNO:${itno} sule:${sule} KO")
        return false
      }
    } else {
      executePPS106MIGetPrice(itno, suld)
      if (price == 0) {
        logMessage("DEBUG", "Controle prix filiere directe : ITNO:${itno} suld:${suld} KO")
        return false
      }
    }

    // If global assortment is selected, selection is ok for all the items
    if (iGold == 1)
      return true

    DBAction queryEXT021 = database.table("EXT021").index("00").selection("EXCHB1").build()
    DBContainer EXT021 = queryEXT021.getContainer()
    EXT021.set("EXCONO", currentCompany)
    EXT021.set("EXASCD", ascd)
    EXT021.set("EXCUNO", cuno)
    EXT021.set("EXFDAT", fdat as Integer)

    // cfi2 must be greater or equal than the criteria
    if (iCfi2 != 0) {
      data = ""
      EXT021.set("EXTYPE", "CFI2")
      if (!queryEXT021.readAll(EXT021, 5, 1, outDataEXT0212)) {
      }

      if ((cfi2.trim() as double) >= (data.trim() as double)) {
        if (iCfi2 == 2) {
          logMessage("DEBUG", "Controle EXT021 ITNO:${itno} CFI2 data:${data} KO")
          return false
        }
      } else {
        if (iCfi2 == 1) {
          logMessage("DEBUG", "Controle EXT021 ITNO:${itno} CFI2 data:${data} KO")
          return false
        }
      }
    }
    if (iItno != 0) {
      EXT021.set("EXTYPE", "ITNO")
      EXT021.set("EXDATA", itno)
      if (queryEXT021.read(EXT021)) {
        if (iItno == 2) {
          logMessage("DEBUG", "Controle EXT021 ITNO:${itno} ITNO data:${data} KO")
          return false
        }
      } else {
        if (iItno == 1) {
          logMessage("DEBUG", "Controle EXT021 ITNO:${itno} ITNO data:${data} KO")
          return false
        }
      }
    }
    if (iSuno != 0) {
      if (iSuno == 2 && sule.trim() == "") {
        logMessage("DEBUG", "Controle EXT021 ITNO:${itno} SUNO = 2 et SULE vide")
        return false
      }

      EXT021.set("EXTYPE", "SUNO")
      EXT021.set("EXDATA", sule)
      if (queryEXT021.read(EXT021)) {
        if (iSuno == 2) {
          logMessage("DEBUG", "Controle EXT021 ITNO:${itno} sule:${sule} KO")
          return false
        }
      } else {
        if (iSuno == 1) {
          logMessage("DEBUG", "Controle EXT021 ITNO:${itno} sule:${sule} KO")
          return false
        }
      }
    }
    if (iProd != 0) {
      if (iProd == 2 && suno.trim() == "") {
        return false
        logMessage("DEBUG", "Controle EXT021 ITNO:${itno} PROD = 2 et SUNO vide")
      }

      EXT021.set("EXTYPE", "PROD")
      EXT021.set("EXDATA", suno)
      if (queryEXT021.read(EXT021)) {
        if (iProd == 2) {
          logMessage("DEBUG", "Controle EXT021 ITNO:${itno} PROD suno:${suno} KO")
          return false
        }
      } else {
        if (iProd == 1) {
          logMessage("DEBUG", "Controle EXT021 ITNO:${itno} PROD suno:${suno} KO")
          return false
        }
      }
    }
    if (iHie1 != 0) {
      EXT021.set("EXTYPE", "HIE1")
      EXT021.set("EXDATA", hie1)
      if (queryEXT021.read(EXT021)) {
        if (iHie1 == 2) {
          logMessage("DEBUG", "Controle EXT021 ITNO:${itno} HIE1:${hie1} KO")
          return false
        }
      } else {
        if (iHie1 == 1) {
          logMessage("DEBUG", "Controle EXT021 ITNO:${itno} HIE1:${hie1} KO")
          return false
        }
      }
    }
    if (iHie2 != 0) {
      EXT021.set("EXTYPE", "HIE2")
      EXT021.set("EXDATA", hie2)
      if (queryEXT021.read(EXT021)) {
        if (iHie2 == 2) {
          logMessage("DEBUG", "Controle EXT021 ITNO:${itno} HIE2:${hie2} KO")
          return false
        }
      } else {
        if (iHie2 == 1) {
          logMessage("DEBUG", "Controle EXT021 ITNO:${itno} HIE2:${hie2} KO")
          return false
        }
      }
    }
    if (iHie3 != 0) {
      EXT021.set("EXTYPE", "HIE3")
      EXT021.set("EXDATA", hie3)
      if (queryEXT021.read(EXT021)) {
        if (iHie3 == 2) {
          logMessage("DEBUG", "Controle EXT021 ITNO:${itno} HIE3:${hie3} KO")
          return false
        }
      } else {
        if (iHie3 == 1) {
          logMessage("DEBUG", "Controle EXT021 ITNO:${itno} HIE3:${hie3} KO")
          return false
        }
      }
    }
    if (iHie4 != 0) {
      EXT021.set("EXTYPE", "HIE4")
      EXT021.set("EXDATA", hie4)
      if (queryEXT021.read(EXT021)) {
        if (iHie4 == 2) {
          logMessage("DEBUG", "Controle EXT021 ITNO:${itno} HIE4:${hie4} KO")
          return false
        }
      } else {
        if (iHie4 == 1) {
          logMessage("DEBUG", "Controle EXT021 ITNO:${itno} HIE4:${hie4} KO")
          return false
        }
      }
    }
    if (iHie5 != 0) {
      EXT021.set("EXTYPE", "HIE5")
      EXT021.set("EXDATA", hie5)
      if (queryEXT021.read(EXT021)) {
        if (iHie5 == 2) {
          logMessage("DEBUG", "Controle EXT021 ITNO:${itno} HIE5:${hie5} KO")
          return false
        }
      } else {
        if (iHie5 == 1) {
          logMessage("DEBUG", "Controle EXT021 ITNO:${itno} HIE5:${hie5} KO")
          return false
        }
      }
    }
    if (iBuar != 0) {
      EXT021.set("EXTYPE", "BUAR")
      EXT021.set("EXDATA", buar)
      if (queryEXT021.read(EXT021)) {
        if (iBuar == 2) {
          logMessage("DEBUG", "Controle EXT021 ITNO:${itno} BUAR:${buar} KO")
          return false
        }
      } else {
        if (iBuar == 1) {
          logMessage("DEBUG", "Controle EXT021 ITNO:${itno} BUAR:${buar} KO")
          return false
        }
      }
    }
    if (iCfi1 != 0) {
      EXT021.set("EXTYPE", "CFI1")
      EXT021.set("EXDATA", cfi1)
      if (queryEXT021.read(EXT021)) {
        if (iCfi1 == 2) {
          logMessage("DEBUG", "Controle EXT021 ITNO:${itno} CFI1:${cfi1} KO")
          return false
        }
      } else {
        if (iCfi1 == 1) {
          logMessage("DEBUG", "Controle EXT021 ITNO:${itno} CFI1:${cfi1} KO")
          return false
        }
      }
    }
    if (iCscd != 0) {
      EXT021.set("EXTYPE", "CSCD")
      EXT021.set("EXDATA", cscd)
      if (queryEXT021.read(EXT021)) {
        if (iCscd == 2) {
          logMessage("DEBUG", "Controle EXT021 ITNO:${itno} CSCD:${cscd} KO")
          return false
        }
      } else {
        if (iCscd == 1) {
          logMessage("DEBUG", "Controle EXT021 ITNO:${itno} CSCD:${cscd} KO")
          return false
        }
      }
    }
    if (iCsno != 0) {
      EXT021.set("EXTYPE", "CSNO")
      EXT021.set("EXDATA", csno)
      if (queryEXT021.read(EXT021)) {
        if (iCsno == 2) {
          logMessage("DEBUG", "Controle EXT021 ITNO:${itno} CSNO:${csno} KO")
          return false
        }
      } else {
        if (iCsno == 1) {
          logMessage("DEBUG", "Controle EXT021 ITNO:${itno} CSNO:${csno} KO")
          return false
        }
      }
    }
    if (iPopn != 0) {
      EXT021.set("EXTYPE", "POPN")
      EXT021.set("EXDATA", popn)
      if (queryEXT021.read(EXT021)) {
        if (iPopn == 2) {
          logMessage("DEBUG", "Controle EXT021 ITNO:${itno} POPN:${popn} KO")
          return false
        }
      } else {
        if (iPopn == 1) {
          logMessage("DEBUG", "Controle EXT021 ITNO:${itno} POPN:${popn} KO")
          return false
        }
      }
    }
    if (iUlty != 0) {
      EXT021.set("EXTYPE", "ULTY")
      EXT021.set("EXDATA", ulty)
      if (queryEXT021.read(EXT021)) {
        if (iUlty == 2) {
          logMessage("DEBUG", "Controle EXT021 ITNO:${itno} ULTY:${ulty} KO")
          return false
        }
      } else {
        if (iUlty == 1) {
          logMessage("DEBUG", "Controle EXT021 ITNO:${itno} ULTY:${ulty} KO")
          return false
        }
      }
    }
    // sldy must be greater or equal than the criteria
    if (iSldy != 0) {
      data = ""
      EXT021.set("EXTYPE", "SLDY")
      if (!queryEXT021.readAll(EXT021, 5, 1, outDataEXT0212)) {
      }

      if ((sldy.trim() as double) >= (data.trim() as double)) {
        if (iSldy == 2) {
          logMessage("DEBUG", "Controle EXT021 ITNO:${itno} SLDY:${sldy} KO")
          return false
        }
      } else {
        if (iSldy == 1) {
          logMessage("DEBUG", "Controle EXT021 ITNO:${itno} SLDY:${sldy} KO")
          return false
        }
      }
    }
    if (iCpfx != 0) {
      EXT021.set("EXTYPE", "CPFX")
      EXT021.set("EXDATA", cpfx)
      if (queryEXT021.read(EXT021)) {
        if (iCpfx == 2) {
          logMessage("DEBUG", "Controle EXT021 ITNO:${itno} CPFX:${cpfx} KO")
          return false
        }
      } else {
        if (iCpfx == 1) {
          logMessage("DEBUG", "Controle EXT021 ITNO:${itno} CPFX:${cpfx} KO")
          return false
        }
      }
    }
    if (iCmde != 0) {
      EXT021.set("EXTYPE", "CMDE")
      EXT021.set("EXDATA", cmde)
      if (queryEXT021.read(EXT021)) {
        if (iCmde == 2) {
          logMessage("DEBUG", "Controle EXT021 ITNO:${itno} CMDE:${cmde} KO")
          return false
        }
      } else {
        if (iCmde == 1) {
          logMessage("DEBUG", "Controle EXT021 ITNO:${itno} CMDE:${cmde} KO")
          return false
        }
      }
    }
    return true
  }

  // Retrieve EXT021
  Closure<?> outDataEXT0212 = { DBContainer EXT021 ->
    data = EXT021.get("EXDATA")
  }
  /**
   * Read information from DB EXT010
   * Double check
   *
   * @parameter Customer, Item, From date, To date
   * @return true if ok false otherwise
   * */
  private boolean checkDouble(String cuno, String itno, String fvdt, String lvdt) {
    boolean errorIndicator = false
    DBAction ext010Query = database.table("EXT010")
      .index("02")
      .selection("EXFVDT", "EXLVDT")
      .build()
    DBContainer ext010Request = ext010Query.getContainer()
    ext010Request.set("EXCONO", currentCompany)
    ext010Request.set("EXCUNO", cuno)
    ext010Request.set("EXITNO", itno)

    int ifvdt = fvdt as Integer
    int ilvdt = lvdt as Integer

    Closure<?> outEXT0101 = { DBContainer EXT0101result ->
      int tfvdt = EXT0101result.get("EXFVDT") as Integer
      int tlvdt = EXT0101result.get("EXLVDT") as Integer
      if (!checkDates(ifvdt, ilvdt, tfvdt, tlvdt)) {
        errorIndicator = true
        return
      }
    }
    ext010Query.readAll(ext010Request, 3, 10000, outEXT0101)
    return !errorIndicator
  }
  /**
   * Check overlap dates
   * @param fvdt1
   * @param lvdt1
   * @param fvdt2
   * @param lvdt2
   * @return
   */
  private boolean checkDates(int fvdt1, int lvdt1, int fvdt2, int lvdt2) {
    if (fvdt1 == fvdt2 && lvdt1 <= lvdt2) {
      return true
    }
    if (fvdt1 == fvdt2 && lvdt1 == lvdt2)
      return true

    if (lvdt1 < fvdt1 || lvdt2 < fvdt2)
      return false

    if (lvdt1 < fvdt2 || lvdt1 < fvdt2)
      return false

    return true
  }
  /**
   * Read information from DB EXT010
   * Double check
   *
   * @parameter Customer, Item, From date, To date
   * @return true if ok false otherwise
   * */
  private boolean checkSunoItno(String suno, String itno) {
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
    if (found) {
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
   * Check Contrainst from EXT030
   * Karnaugh is used to check the constraints
   * @Return true if no blocking constraint is found for the item
   */
  public boolean constraintsOK() {
    constraintIsOK = true
    ExpressionFactory ext030Expression = database.getExpressionFactory("EXT030")
    ext030Expression = (ext030Expression.eq("EXCUNO", cuno)).or(ext030Expression.eq("EXCUNO", ""))
    if (cuno == "") {
      ext030Expression = (ext030Expression.eq("EXCSCD", constraintCscd)).or(ext030Expression.eq("EXCSCD", ""))
    } else {
      ext030Expression = ext030Expression.and((ext030Expression.eq("EXCSCD", constraintCscd)).or(ext030Expression.eq("EXCSCD", "")))
    }
    if (cuno == "" && constraintCscd == "") {
      ext030Expression = (ext030Expression.eq("EXHAZI", hazi as String)).or(ext030Expression.eq("EXHAZI", "2"))
    } else {
      ext030Expression = ext030Expression.and((ext030Expression.eq("EXHAZI", hazi as String)).or(ext030Expression.eq("EXHAZI", "2")))
    }
    if (hie5 != "") {
      if (cuno == "" && constraintCscd == "" && hazi == 0) {
        ext030Expression = (ext030Expression.eq("EXHIE0", hie5)).or(ext030Expression.eq("EXHIE0", hie5.substring(0, 2) + "*")).or(ext030Expression.eq("EXHIE0", hie5.substring(0, 4) + "*")).or(ext030Expression.eq("EXHIE0", hie5.substring(0, 7) + "*")).or(ext030Expression.eq("EXHIE0", hie5.substring(0, 9) + "*")).or(ext030Expression.eq("EXHIE0", hie5.substring(0, 11) + "*")).or(ext030Expression.eq("EXHIE0", ""))
      } else {
        ext030Expression = ext030Expression.and((ext030Expression.eq("EXHIE0", hie5)).or(ext030Expression.eq("EXHIE0", hie5.substring(0, 2) + "*")).or(ext030Expression.eq("EXHIE0", hie5.substring(0, 4) + "*")).or(ext030Expression.eq("EXHIE0", hie5.substring(0, 7) + "*")).or(ext030Expression.eq("EXHIE0", hie5.substring(0, 9) + "*")).or(ext030Expression.eq("EXHIE0", hie5.substring(0, 11) + "*")).or(ext030Expression.eq("EXHIE0", "")))
      }
    } else {
      if (cuno == "" && constraintCscd == "" && hazi == 0) {
        ext030Expression = (ext030Expression.eq("EXHIE0", hie5)).or(ext030Expression.eq("EXHIE0", ""))
      } else {
        ext030Expression = ext030Expression.and((ext030Expression.eq("EXHIE0", hie5)).or(ext030Expression.eq("EXHIE0", "")))
      }
    }
    if (cuno == "" && constraintCscd == "" && hazi == 0 && hie5 == "") {
      ext030Expression = (ext030Expression.eq("EXCFI4", cfi4)).or(ext030Expression.eq("EXCFI4", ""))
    } else {
      ext030Expression = ext030Expression.and((ext030Expression.eq("EXCFI4", cfi4)).or(ext030Expression.eq("EXCFI4", "")))
    }
    if (cuno == "" && constraintCscd == "" && hazi == 0 && hie5 == "" && cfi4 == "") {
      ext030Expression = (ext030Expression.eq("EXPOPN", popn)).or(ext030Expression.eq("EXPOPN", ""))
    } else {
      ext030Expression = ext030Expression.and((ext030Expression.eq("EXPOPN", popn)).or(ext030Expression.eq("EXPOPN", "")))
    }
    if (csno != "") {
      if (cuno == "" && constraintCscd == "" && hazi == 0 && hie5 == "" && cfi4 == "" && popn == "") {
        if (csno.toString().length() == 16)
          ext030Expression = (ext030Expression.eq("EXCSNO", csno)).or(ext030Expression.eq("EXCSNO", csno.substring(0, 1) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 2) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 3) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 4) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 5) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 6) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 7) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 8) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 9) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 10) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 11) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 12) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 13) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 14) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 15) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 16) + "*")).or(ext030Expression.eq("EXCSNO", ""))
        if (csno.toString().length() == 15)
          ext030Expression = (ext030Expression.eq("EXCSNO", csno)).or(ext030Expression.eq("EXCSNO", csno.substring(0, 1) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 2) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 3) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 4) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 5) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 6) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 7) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 8) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 9) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 10) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 11) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 12) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 13) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 14) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 15) + "*")).or(ext030Expression.eq("EXCSNO", ""))
        if (csno.toString().length() == 14)
          ext030Expression = (ext030Expression.eq("EXCSNO", csno)).or(ext030Expression.eq("EXCSNO", csno.substring(0, 1) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 2) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 3) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 4) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 5) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 6) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 7) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 8) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 9) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 10) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 11) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 12) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 13) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 14) + "*")).or(ext030Expression.eq("EXCSNO", ""))
        if (csno.toString().length() == 13)
          ext030Expression = (ext030Expression.eq("EXCSNO", csno)).or(ext030Expression.eq("EXCSNO", csno.substring(0, 1) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 2) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 3) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 4) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 5) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 6) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 7) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 8) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 9) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 10) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 11) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 12) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 13) + "*")).or(ext030Expression.eq("EXCSNO", ""))
        if (csno.toString().length() == 12)
          ext030Expression = (ext030Expression.eq("EXCSNO", csno)).or(ext030Expression.eq("EXCSNO", csno.substring(0, 1) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 2) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 3) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 4) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 5) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 6) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 7) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 8) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 9) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 10) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 11) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 12) + "*")).or(ext030Expression.eq("EXCSNO", ""))
        if (csno.toString().length() == 11)
          ext030Expression = (ext030Expression.eq("EXCSNO", csno)).or(ext030Expression.eq("EXCSNO", csno.substring(0, 1) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 2) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 3) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 4) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 5) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 6) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 7) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 8) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 9) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 10) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 11) + "*")).or(ext030Expression.eq("EXCSNO", ""))
        if (csno.toString().length() == 10)
          ext030Expression = (ext030Expression.eq("EXCSNO", csno)).or(ext030Expression.eq("EXCSNO", csno.substring(0, 1) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 2) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 3) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 4) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 5) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 6) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 7) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 8) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 9) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 10) + "*")).or(ext030Expression.eq("EXCSNO", ""))
        if (csno.toString().length() == 9)
          ext030Expression = (ext030Expression.eq("EXCSNO", csno)).or(ext030Expression.eq("EXCSNO", csno.substring(0, 1) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 2) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 3) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 4) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 5) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 6) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 7) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 8) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 9) + "*")).or(ext030Expression.eq("EXCSNO", ""))
        if (csno.toString().length() == 8)
          ext030Expression = (ext030Expression.eq("EXCSNO", csno)).or(ext030Expression.eq("EXCSNO", csno.substring(0, 1) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 2) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 3) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 4) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 5) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 6) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 7) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 8) + "*")).or(ext030Expression.eq("EXCSNO", ""))
        if (csno.toString().length() == 7)
          ext030Expression = (ext030Expression.eq("EXCSNO", csno)).or(ext030Expression.eq("EXCSNO", csno.substring(0, 1) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 2) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 3) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 4) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 5) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 6) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 7) + "*")).or(ext030Expression.eq("EXCSNO", ""))
        if (csno.toString().length() == 6)
          ext030Expression = (ext030Expression.eq("EXCSNO", csno)).or(ext030Expression.eq("EXCSNO", csno.substring(0, 1) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 2) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 3) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 4) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 5) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 6) + "*")).or(ext030Expression.eq("EXCSNO", ""))
        if (csno.toString().length() == 5)
          ext030Expression = (ext030Expression.eq("EXCSNO", csno)).or(ext030Expression.eq("EXCSNO", csno.substring(0, 1) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 2) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 3) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 4) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 5) + "*")).or(ext030Expression.eq("EXCSNO", ""))
        if (csno.toString().length() == 4)
          ext030Expression = (ext030Expression.eq("EXCSNO", csno)).or(ext030Expression.eq("EXCSNO", csno.substring(0, 1) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 2) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 3) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 4) + "*")).or(ext030Expression.eq("EXCSNO", ""))
        if (csno.toString().length() == 3)
          ext030Expression = (ext030Expression.eq("EXCSNO", csno)).or(ext030Expression.eq("EXCSNO", csno.substring(0, 1) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 2) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 3) + "*")).or(ext030Expression.eq("EXCSNO", ""))
        if (csno.toString().length() == 2)
          ext030Expression = (ext030Expression.eq("EXCSNO", csno)).or(ext030Expression.eq("EXCSNO", csno.substring(0, 1) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 2) + "*")).or(ext030Expression.eq("EXCSNO", ""))
        if (csno.toString().length() == 1)
          ext030Expression = (ext030Expression.eq("EXCSNO", csno)).or(ext030Expression.eq("EXCSNO", csno.substring(0, 1) + "*")).or(ext030Expression.eq("EXCSNO", ""))
      } else {
        if (csno.toString().length() == 16)
          ext030Expression = ext030Expression.and((ext030Expression.eq("EXCSNO", csno)).or(ext030Expression.eq("EXCSNO", csno.substring(0, 1) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 2) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 3) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 4) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 5) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 6) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 7) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 8) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 9) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 10) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 11) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 12) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 13) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 14) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 15) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 16) + "*")).or(ext030Expression.eq("EXCSNO", "")))
        if (csno.toString().length() == 15)
          ext030Expression = ext030Expression.and((ext030Expression.eq("EXCSNO", csno)).or(ext030Expression.eq("EXCSNO", csno.substring(0, 1) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 2) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 3) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 4) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 5) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 6) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 7) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 8) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 9) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 10) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 11) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 12) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 13) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 14) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 15) + "*")).or(ext030Expression.eq("EXCSNO", "")))
        if (csno.toString().length() == 14)
          ext030Expression = ext030Expression.and((ext030Expression.eq("EXCSNO", csno)).or(ext030Expression.eq("EXCSNO", csno.substring(0, 1) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 2) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 3) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 4) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 5) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 6) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 7) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 8) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 9) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 10) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 11) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 12) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 13) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 14) + "*")).or(ext030Expression.eq("EXCSNO", "")))
        if (csno.toString().length() == 13)
          ext030Expression = ext030Expression.and((ext030Expression.eq("EXCSNO", csno)).or(ext030Expression.eq("EXCSNO", csno.substring(0, 1) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 2) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 3) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 4) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 5) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 6) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 7) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 8) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 9) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 10) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 11) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 12) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 13) + "*")).or(ext030Expression.eq("EXCSNO", "")))
        if (csno.toString().length() == 12)
          ext030Expression = ext030Expression.and((ext030Expression.eq("EXCSNO", csno)).or(ext030Expression.eq("EXCSNO", csno.substring(0, 1) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 2) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 3) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 4) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 5) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 6) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 7) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 8) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 9) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 10) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 11) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 12) + "*")).or(ext030Expression.eq("EXCSNO", "")))
        if (csno.toString().length() == 11)
          ext030Expression = ext030Expression.and((ext030Expression.eq("EXCSNO", csno)).or(ext030Expression.eq("EXCSNO", csno.substring(0, 1) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 2) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 3) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 4) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 5) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 6) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 7) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 8) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 9) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 10) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 11) + "*")).or(ext030Expression.eq("EXCSNO", "")))
        if (csno.toString().length() == 10)
          ext030Expression = ext030Expression.and((ext030Expression.eq("EXCSNO", csno)).or(ext030Expression.eq("EXCSNO", csno.substring(0, 1) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 2) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 3) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 4) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 5) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 6) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 7) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 8) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 9) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 10) + "*")).or(ext030Expression.eq("EXCSNO", "")))
        if (csno.toString().length() == 9)
          ext030Expression = ext030Expression.and((ext030Expression.eq("EXCSNO", csno)).or(ext030Expression.eq("EXCSNO", csno.substring(0, 1) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 2) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 3) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 4) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 5) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 6) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 7) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 8) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 9) + "*")).or(ext030Expression.eq("EXCSNO", "")))
        if (csno.toString().length() == 8)
          ext030Expression = ext030Expression.and((ext030Expression.eq("EXCSNO", csno)).or(ext030Expression.eq("EXCSNO", csno.substring(0, 1) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 2) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 3) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 4) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 5) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 6) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 7) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 8) + "*")).or(ext030Expression.eq("EXCSNO", "")))
        if (csno.toString().length() == 7)
          ext030Expression = ext030Expression.and((ext030Expression.eq("EXCSNO", csno)).or(ext030Expression.eq("EXCSNO", csno.substring(0, 1) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 2) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 3) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 4) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 5) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 6) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 7) + "*")).or(ext030Expression.eq("EXCSNO", "")))
        if (csno.toString().length() == 6)
          ext030Expression = ext030Expression.and((ext030Expression.eq("EXCSNO", csno)).or(ext030Expression.eq("EXCSNO", csno.substring(0, 1) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 2) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 3) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 4) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 5) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 6) + "*")).or(ext030Expression.eq("EXCSNO", "")))
        if (csno.toString().length() == 5)
          ext030Expression = ext030Expression.and((ext030Expression.eq("EXCSNO", csno)).or(ext030Expression.eq("EXCSNO", csno.substring(0, 1) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 2) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 3) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 4) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 5) + "*")).or(ext030Expression.eq("EXCSNO", "")))
        if (csno.toString().length() == 4)
          ext030Expression = ext030Expression.and((ext030Expression.eq("EXCSNO", csno)).or(ext030Expression.eq("EXCSNO", csno.substring(0, 1) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 2) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 3) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 4) + "*")).or(ext030Expression.eq("EXCSNO", "")))
        if (csno.toString().length() == 3)
          ext030Expression = ext030Expression.and((ext030Expression.eq("EXCSNO", csno)).or(ext030Expression.eq("EXCSNO", csno.substring(0, 1) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 2) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 3) + "*")).or(ext030Expression.eq("EXCSNO", "")))
        if (csno.toString().length() == 2)
          ext030Expression = ext030Expression.and((ext030Expression.eq("EXCSNO", csno)).or(ext030Expression.eq("EXCSNO", csno.substring(0, 1) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 2) + "*")).or(ext030Expression.eq("EXCSNO", "")))
        if (csno.toString().length() == 1)
          ext030Expression = ext030Expression.and((ext030Expression.eq("EXCSNO", csno)).or(ext030Expression.eq("EXCSNO", csno.substring(0, 1) + "*")).or(ext030Expression.eq("EXCSNO", "")))
      }
    } else {
      if (cuno == "" && constraintCscd == "" && hazi == 0 && hie5 == "" && cfi4 == "" && popn == "") {
        ext030Expression = (ext030Expression.eq("EXCSNO", csno)).or(ext030Expression.eq("EXCSNO", ""))
      } else {
        ext030Expression = ext030Expression.and((ext030Expression.eq("EXCSNO", csno)).or(ext030Expression.eq("EXCSNO", "")))
      }
    }
    if (cuno == "" && constraintCscd == "" && hazi == 0 && hie5 == "" && cfi4 == "" && popn == "" && csno == "") {
      ext030Expression = (ext030Expression.eq("EXORCO", cscd)).or(ext030Expression.eq("EXORCO", ""))
    } else {
      ext030Expression = ext030Expression.and((ext030Expression.eq("EXORCO", cscd)).or(ext030Expression.eq("EXORCO", "")))
    }
    if (cuno == "" && constraintCscd == "" && hazi == 0 && hie5 == "" && cfi4 == "" && popn == "" && csno == "" && cscd == "") {
      ext030Expression = (ext030Expression.eq("EXZALC", zalc as String)).or(ext030Expression.eq("EXZALC", "2"))
    } else {
      ext030Expression = ext030Expression.and((ext030Expression.eq("EXZALC", zalc as String)).or(ext030Expression.eq("EXZALC", "2")))
    }
    if (cuno == "" && constraintCscd == "" && hazi == 0 && hie5 == "" && cfi4 == "" && popn == "" && csno == "" && cscd == "" && zalc == 0) {
      ext030Expression = (ext030Expression.eq("EXZSAN", zsan as String)).or(ext030Expression.eq("EXZSAN", "2"))
    } else {
      ext030Expression = ext030Expression.and((ext030Expression.eq("EXZSAN", zsan as String)).or(ext030Expression.eq("EXZSAN", "2")))
    }
    if (cuno == "" && constraintCscd == "" && hazi == 0 && hie5 == "" && cfi4 == "" && popn == "" && csno == "" && cscd == "" && zalc == 0 && zsan == 0) {
      ext030Expression = ((ext030Expression.eq("EXZCAP", zca1)).or(ext030Expression.eq("EXZCAP", "")))
    } else {
      ext030Expression = ext030Expression.and((ext030Expression.eq("EXZCAP", zca1)).or(ext030Expression.eq("EXZCAP", "")))
    }
    if (cuno == "" && constraintCscd == "" && hazi == 0 && hie5 == "" && cfi4 == "" && popn == "" && csno == "" && cscd == "" && zalc == 0 && zsan == 0) {
      ext030Expression = ((ext030Expression.eq("EXZCAS", zca1)).or(ext030Expression.eq("EXZCAS", zca2)).or(ext030Expression.eq("EXZCAS", zca3)).or(ext030Expression.eq("EXZCAS", zca4)).or(ext030Expression.eq("EXZCAS", zca5)).or(ext030Expression.eq("EXZCAS", zca6)).or(ext030Expression.eq("EXZCAS", zca7)).or(ext030Expression.eq("EXZCAS", zca8)).or(ext030Expression.eq("EXZCAS", ""))).or(ext030Expression.eq("EXZCAS", ""))
    } else {
      ext030Expression = ext030Expression.and((ext030Expression.eq("EXZCAS", zca1)).or(ext030Expression.eq("EXZCAS", zca2)).or(ext030Expression.eq("EXZCAS", zca3)).or(ext030Expression.eq("EXZCAS", zca4)).or(ext030Expression.eq("EXZCAS", zca5)).or(ext030Expression.eq("EXZCAS", zca6)).or(ext030Expression.eq("EXZCAS", zca7)).or(ext030Expression.eq("EXZCAS", zca8)).or(ext030Expression.eq("EXZCAS", "")))
    }
    if (znag != "") {
      if (cuno == "" && constraintCscd == "" && hazi == 0 && hie5 == "" && cfi4 == "" && popn == "" && csno == "" && cscd == "" && zalc == 0 && zsan == 0 && zca1 == "" && zca2 == "" && zca3 == "" && zca4 == "" && zca5 == "" && zca6 == "" && zca7 == "" && zca8 == "") {
        ext030Expression = (ext030Expression.eq("EXZNAG", znag)).or(ext030Expression.eq("EXZNAG", znag.substring(0, 4) + "*")).or(ext030Expression.eq("EXZNAG", ""))
      } else {
        ext030Expression = ext030Expression.and((ext030Expression.eq("EXZNAG", znag)).or(ext030Expression.eq("EXZNAG", znag.substring(0, 4) + "*")).or(ext030Expression.eq("EXZNAG", "")))
      }
    } else {
      if (cuno == "" && constraintCscd == "" && hazi == 0 && hie5 == "" && cfi4 == "" && popn == "" && csno == "" && cscd == "" && zalc == 0 && zsan == 0 && zca1 == "" && zca2 == "" && zca3 == "" && zca4 == "" && zca5 == "" && zca6 == "" && zca7 == "" && zca8 == "") {
        ext030Expression = (ext030Expression.eq("EXZNAG", znag)).or(ext030Expression.eq("EXZNAG", ""))
      } else {
        ext030Expression = ext030Expression.and((ext030Expression.eq("EXZNAG", znag)).or(ext030Expression.eq("EXZNAG", "")))
      }
    }
    if (cuno == "" && constraintCscd == "" && hazi == 0 && hie5 == "" && cfi4 == "" && popn == "" && csno == "" && cscd == "" && zalc == 0 && zsan == 0 && zca1 == "" && zca2 == "" && zca3 == "" && zca4 == "" && zca5 == "" && zca6 == "" && zca7 == "" && zca8 == "" && znag == "") {
      ext030Expression = (ext030Expression.eq("EXZALI", zali as String)).or(ext030Expression.eq("EXZALI", "2"))
    } else {
      ext030Expression = ext030Expression.and((ext030Expression.eq("EXZALI", zali as String)).or(ext030Expression.eq("EXZALI", "2")))
    }
    if (cuno == "" && constraintCscd == "" && hazi == 0 && hie5 == "" && cfi4 == "" && popn == "" && csno == "" && cscd == "" && zalc == 0 && zsan == 0 && zca1 == "" && zca2 == "" && zca3 == "" && zca4 == "" && zca5 == "" && zca6 == "" && zca7 == "" && zca8 == "" && znag == "" && zali == 0) {
      ext030Expression = (ext030Expression.eq("EXZORI", zori as String)).or(ext030Expression.eq("EXZORI", "2"))
    } else {
      ext030Expression = ext030Expression.and((ext030Expression.eq("EXZORI", zori as String)).or(ext030Expression.eq("EXZORI", "2")))
    }
    if (cuno == "" && constraintCscd == "" && hazi == 0 && hie5 == "" && cfi4 == "" && popn == "" && csno == "" && cscd == "" && zalc == 0 && zsan == 0 && zca1 == "" && zca2 == "" && zca3 == "" && zca4 == "" && zca5 == "" && zca6 == "" && zca7 == "" && zca8 == "" && znag == "" && zali == 0 && zori == 0) {
      ext030Expression = (ext030Expression.eq("EXZPHY", zphy as String)).or(ext030Expression.eq("EXZPHY", "2"))
    } else {
      ext030Expression = ext030Expression.and((ext030Expression.eq("EXZPHY", zphy as String)).or(ext030Expression.eq("EXZPHY", "2")))
    }
    if (cuno == "" && constraintCscd == "" && hazi == 0 && hie5 == "" && cfi4 == "" && popn == "" && csno == "" && cscd == "" && zalc == 0 && zsan == 0 && zca1 == "" && zca2 == "" && zca3 == "" && zca4 == "" && zca5 == "" && zca6 == "" && zca7 == "" && zca8 == "" && znag == "" && zali == 0 && zori == 0 && zphy == 0) {
      ext030Expression = (ext030Expression.eq("EXZOHF", zohf as String)).or(ext030Expression.eq("EXZOHF", "2"))
    } else {
      ext030Expression = ext030Expression.and((ext030Expression.eq("EXZOHF", zohf as String)).or(ext030Expression.eq("EXZOHF", "2")))
    }
    DBAction ext030Query = database.table("EXT030").index("10").matching(ext030Expression).selection("EXZCID", "EXZCOD", "EXZBLO").build()
    DBContainer ext030Request = ext030Query.getContainer()
    ext030Request.set("EXCONO", currentCompany)
    ext030Request.set("EXZBLO", 1)
    ext030Request.set("EXSTAT", "20")
    // Retrieve EXT030
    Closure<?> outDataEXT030 = { DBContainer ext030Result ->
      constraintIsOK = false
      return//todo ca le fait ????
    }
    if (!ext030Query.readAll(ext030Request, 3, 10000, outDataEXT030)) {
    }
    return constraintIsOK
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
    if (EXT021.get("EXTYPE") == "SUNO" && EXT021.get("EXCHB1") == 0) iSuno = 1
    if (EXT021.get("EXTYPE") == "iSUNO" && EXT021.get("EXCHB1") == 1) iSuno = 2
    if (EXT021.get("EXTYPE") == "PROD" && EXT021.get("EXCHB1") == 0) iProd = 1
    if (EXT021.get("EXTYPE") == "PROD" && EXT021.get("EXCHB1") == 1) iProd = 2
    if (EXT021.get("EXTYPE") == "HIE1" && EXT021.get("EXCHB1") == 0) iHie1 = 1
    if (EXT021.get("EXTYPE") == "HIE1" && EXT021.get("EXCHB1") == 1) iHie1 = 2
    if (EXT021.get("EXTYPE") == "HIE2" && EXT021.get("EXCHB1") == 0) iHie2 = 1
    if (EXT021.get("EXTYPE") == "HIE2" && EXT021.get("EXCHB1") == 1) iHie2 = 2
    if (EXT021.get("EXTYPE") == "HIE3" && EXT021.get("EXCHB1") == 0) iHie3 = 1
    if (EXT021.get("EXTYPE") == "HIE3" && EXT021.get("EXCHB1") == 1) iHie3 = 2
    if (EXT021.get("EXTYPE") == "HIE4" && EXT021.get("EXCHB1") == 0) iHie4 = 1
    if (EXT021.get("EXTYPE") == "HIE4" && EXT021.get("EXCHB1") == 1) iHie4 = 2
    if (EXT021.get("EXTYPE") == "HIE5" && EXT021.get("EXCHB1") == 0) iHie5 = 1
    if (EXT021.get("EXTYPE") == "HIE5" && EXT021.get("EXCHB1") == 1) iHie5 = 2
    if (EXT021.get("EXTYPE") == "BUAR" && EXT021.get("EXCHB1") == 0) iBuar = 1
    if (EXT021.get("EXTYPE") == "BUAR" && EXT021.get("EXCHB1") == 1) iBuar = 2
    if (EXT021.get("EXTYPE") == "CFI1" && EXT021.get("EXCHB1") == 0) iCfi1 = 1
    if (EXT021.get("EXTYPE") == "CFI1" && EXT021.get("EXCHB1") == 1) iCfi1 = 2
    if (EXT021.get("EXTYPE") == "CSCD" && EXT021.get("EXCHB1") == 0) iCscd = 1
    if (EXT021.get("EXTYPE") == "CSCD" && EXT021.get("EXCHB1") == 1) iCscd = 2
    if (EXT021.get("EXTYPE") == "CSNO" && EXT021.get("EXCHB1") == 0) iCsno = 1
    if (EXT021.get("EXTYPE") == "CSNO" && EXT021.get("EXCHB1") == 1) iCsno = 2
    if (EXT021.get("EXTYPE") == "CFI2" && EXT021.get("EXCHB1") == 0) iCfi2 = 1
    if (EXT021.get("EXTYPE") == "CFI2" && EXT021.get("EXCHB1") == 1) iCfi2 = 2
    if (EXT021.get("EXTYPE") == "ITNO" && EXT021.get("EXCHB1") == 0) iItno = 1
    if (EXT021.get("EXTYPE") == "ITNO" && EXT021.get("EXCHB1") == 1) iItno = 2
    if (EXT021.get("EXTYPE") == "POPN" && EXT021.get("EXCHB1") == 0) iPopn = 1
    if (EXT021.get("EXTYPE") == "POPN" && EXT021.get("EXCHB1") == 1) iPopn = 2
    if (EXT021.get("EXTYPE") == "ULTY" && EXT021.get("EXCHB1") == 0) iUlty = 1
    if (EXT021.get("EXTYPE") == "ULTY" && EXT021.get("EXCHB1") == 1) iUlty = 2
    if (EXT021.get("EXTYPE") == "SLDY" && EXT021.get("EXCHB1") == 0) iSldy = 1
    if (EXT021.get("EXTYPE") == "SLDY" && EXT021.get("EXCHB1") == 1) iSldy = 2
    if (EXT021.get("EXTYPE") == "CPFX" && EXT021.get("EXCHB1") == 0) iCpfx = 1
    if (EXT021.get("EXTYPE") == "CPFX" && EXT021.get("EXCHB1") == 1) iCpfx = 2
    if (EXT021.get("EXTYPE") == "CMDE" && EXT021.get("EXCHB1") == 0) iCmde = 1
    if (EXT021.get("EXTYPE") == "CMDE" && EXT021.get("EXCHB1") == 1) iCmde = 2
    if (EXT021.get("EXTYPE") == "GOLD" && EXT021.get("EXCHB1") == 0) iGold = 1
    if (EXT021.get("EXTYPE") == "GOLD" && EXT021.get("EXCHB1") == 1) iGold = 2
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
        String header = "MSG"
        String message = "Failed EXT820MI.SubmitBatch: " + response.errorMessage
        logMessage(header, message)
        return
      } else {
      }
    }
    miCaller.call("EXT820MI", "SubmitBatch", parameters, handler)
  }

  /**
   * EXECUTE EXT023MI.UpdAssortItem
   * @parameter ASCD
   * @parameter CUNO
   * @parameter FDAT
   * @return
   */
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
  public void updateCugex1(String status, String count) {
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
    DBAction extjobQuery = database.table("EXTJOB").index("00").build()
    DBContainer extJobRequest = extjobQuery.getContainer()
    extJobRequest.set("EXRFID", batch.getReferenceId().get())
    // Delete EXTJOB
    Closure<?> updateCallBackEXTJOB = { LockedResult lockedResult ->
      lockedResult.delete()
    }

    if (!extjobQuery.readLock(extJobRequest, updateCallBackEXTJOB)) {
    }
  }
  /**
   * Initialize log management
   */
  private void initializeLogManagement() {
    textFiles.open("log")
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
