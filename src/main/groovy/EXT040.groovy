/**
 * README
 * This extension is used by Mashup
 *
 * Name : EXT040
 * Description : Generates the calendar file and associated files
 * Date         Changed By  Version      Description
 * 20230309     RENARN      1.0            COMX02 - Cadencier
 * 20231122     RENARN      1.0            New columns added. Columns heading rectified
 * 20240327     PATBEA      1.0            Correction Key to read table CCUCON. CCEMRE missing
 * 20240402     PATBEA      1.0            Execute EXT041 (ECOM_PROD_STO File) when assortiment is complet
 * 20240412     YVOYOU      1.0            Addition of delete EXT045,EXT046 and no line in calendar if sapr < pupr
 * 20240419     FLEBARS     1.0             Chemin fichier dans docnumber à rendre dynamique **ATTENTION NE PAS FAIRE VALIDER**
 * 20240524     FLEBARS     1.0             Alcool et controle prix
 * 20240806     FLEBARS     1.0             Evolution 20, 52, 56
 * 20250424     FLEBARS     1.1             Changement IDM + SFTP
 */
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import mvx.db.common.PositionKey
import mvx.db.common.PositionEmpty

public class EXT040 extends ExtendM3Batch {
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
  private boolean IN60
  private String jobNumber
  private Integer currentDate
  private String inCustomer
  private String inCalendar
  private Integer inFormatTXT
  private Integer inFormatCSV
  private Integer inFormatXLSX


  private Integer inAllContacts
  private Integer inSchedule
  private String previousCalendar
  private String previousCalendarYearWeek
  private String previousCalendarSuffix
  private String previousCalendarNextSuffix
  private String sigma6
  private String sigma9
  private String sigma9DirectDelivery
  private String sigma9_NoDirectDelivery
  private String sigma9DirectDeliveryassortment
  private String sigma9_NoDirectDelivery_assortment
  private String stat
  private int chb7
  private String cmde
  private String customerName
  private String creationDate
  private String creationTime
  private String header
  private String line
  private String lines
  private String assortment
  private int countLines

  private int lineIncadencier
  private int lineInGapFile
  private int lineInAlert
  private int lineInControlPrice

  private double currentCOFA
  private double savedCOFA

  private String ul
  private String ean13
  private String libelleArticle
  private String libelleSigma6
  private String codeArticleRemplace
  private String libelleArticleRemplace
  private String prixCadencierPrecedent
  private String prixVente
  private Integer savedFromValidDate
  private Integer currentFromValidDate
  private String evolutionPrixCadencierPrecedent
  private String unitePrixVente
  private String dlgr
  private String dlc
  private String codeMarque
  private String libelleCodeMarque
  private String codeMarketing
  private String libelleCodeMarketing
  private String saisonnalite
  private String nouveaute
  private String languePackaging
  private String codeDepartement
  private String nomDepartement
  private String codeRayon
  private String nomRayon
  private String codeFamille
  private String nomFamille
  private String codeSousFamille
  private String nomSousFamille
  private String uniteBesoin
  private String nomUniteBesoin
  private String fournisseur
  private String nomFournisseur
  private String fournisseurOrigine
  private String nomFournisseurOrigine
  private String paysOrigine
  private String nomPaysOrigine
  private String codeDouanier
  private String typeAppro
  private String entrepot
  private String taille
  private String couleur
  private String modele
  private String diffusion
  private String fba
  private String dun14
  private String minimumCommande
  private String parCombien
  private String nbCouchePalette
  private String nbColisParCouche
  private String nbColisParPalette
  private String nbUVCParPalette
  private String contenance
  private String poidsNet
  private String poidsBrut
  private String volumeColis
  private String hauteurUVC
  private String largeurUVC
  private String longueurUVC
  private String hauteurColis
  private String largeurColis
  private String longueurColis
  private String degreAlcool
  private String refDroit
  private String popn
  private String rscl
  private String assortimentLogistique
  private String pltb
  private String prrf
  private String spe1
  private String spe2
  private String hie2
  private String hie3
  private String hie4
  private String hie5
  private boolean foundSigma6
  private double sapr
  private double appr
  private String lhcd
  private String docnumber
  private String modl
  private String cfc1
  private String deliveryMethodName
  private String temperature
  private double sumCNQT
  private String typeBox
  private boolean isAlcool
  private String nombreComposantBox
  private int countOasitn = 0
  private String fileJobNumber
  private boolean isMainAssortment = false
  private String mainAssortment
  private boolean isReedition = false
  private Map<String, String> mithryMap


  public EXT040(LoggerAPI logger, DatabaseAPI database, ProgramAPI program, BatchAPI batch, MICallerAPI miCaller, TextFilesAPI textFiles, UtilityAPI utility) {
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
    fileJobNumber = program.getJobNumber()
    jobNumber = program.getJobNumber() + timeOfCreation.format(DateTimeFormatter.ofPattern("yyMMdd")) + timeOfCreation.format(DateTimeFormatter.ofPattern("HHmmss"))

    //log management
    initializeLogManagement()


    if (batch.getReferenceId().isPresent()) {
      Optional<String> data = getJobData(batch.getReferenceId().get())
      performActualJob(data)
    } else {
      // No job data found
    }
    logMessages()
  }
  // Perform actual job
  private performActualJob(Optional<String> data) {
    if (!data.isPresent()) {
      return
    }
    rawData = data.get()
    inCustomer = getFirstParameter()
    inCalendar = getNextParameter()
    inAllContacts = getNextParameter() as Integer
    inSchedule = 0
    try {
      inSchedule = getNextParameter() as Integer
    } catch (NumberFormatException e) {

    }

    logMessage("INFO", "Start cadencier for customer:${inCustomer} calendar:${inCalendar} allContacts:${inAllContacts} schedule:${inSchedule}")
    //Output Type
    inFormatTXT = 0
    inFormatCSV = 0
    inFormatXLSX = 0
    DBAction ext042Query = database.table("EXT042").index("10").selection("EXASCD", "EXFLTX", "EXFLCS", "EXFLXL").build()
    DBContainer ext042Request = ext042Query.getContainer()
    ext042Request.set("EXCONO", currentCompany)
    ext042Request.set("EXCDNN", inCalendar)
    ext042Request.set("EXCUNO", inCustomer) // 20240507 PBEAUDOUIN

    Closure<?> ext042Reader = { DBContainer EXT042_Type ->
      inFormatTXT = EXT042_Type.get("EXFLTX")
      inFormatCSV = EXT042_Type.get("EXFLCS")
      inFormatXLSX = EXT042_Type.get("EXFLXL")
      assortment = EXT042_Type.get("EXASCD") as String//A°20240503 FLEBARS

    }
    if (!ext042Query.readAll(ext042Request, 3, 1, ext042Reader)) {// 20240507 PBEAUDOUIN
      logMessage("ERROR", "Pas de record EXT042 :${inCustomer} calendar:${inCalendar} allContacts:${inAllContacts} schedule:${inSchedule}")
      return

    }
    retrieveCustomerInformations()
    isMainAssortment = assortment.trim().equals(mainAssortment)

    LocalDateTime timeOfCreation = LocalDateTime.now()
    currentDate = timeOfCreation.format(DateTimeFormatter.ofPattern("yyyyMMdd")) as Integer
    creationDate = timeOfCreation.format(DateTimeFormatter.ofPattern("yyyyMMdd")) as String
    creationTime = timeOfCreation.format(DateTimeFormatter.ofPattern("HHmmss")) as String

    // Perform Job
    createFormatFile()
    createCustomerEmailFile()
    createInternalEmailFile()
    writeInExt040Ext041()
    createCalendarFile()
    createGapFile()
    createAlertFile()
    createPriceControlFile()

    createControlFile()
    createControlFilesFtp()
    if (isMainAssortment) {
      executeEXT820MISubmitBatch(String.valueOf(currentCompany), "EXT041", program.getUser(), String.valueOf(inCustomer), String.valueOf(inCalendar), String.valueOf(inAllContacts))
    }
    if (!isReedition)
      deleteTemporaryFiles()

    deleteEXTJOB()
  }
  // Retrieve previous calendar name
  private void retrievePreviousCalendar() {
    previousCalendar = ""
    previousCalendarYearWeek = ""
    previousCalendarSuffix = ""
    previousCalendarNextSuffix = ""
    DBAction ext041Query = database.table("EXT041").index("10").selection("EXCDNN").reverse().build()
    DBContainer ext041Request = ext041Query.getContainer()
    ext041Request.set("EXCONO", currentCompany)
    ext041Request.set("EXCUNO", inCustomer)

    // Retrieve EXT041
    Closure<?> ext041Reader = { DBContainer EXT041 ->
      previousCalendar = EXT041.get("EXCDNN")
    }
    if (!ext041Query.readAll(ext041Request, 2, 1, ext041Reader)) {
    }
    if (inCalendar == previousCalendar)
      isReedition = true
    if (previousCalendar.trim() != "") {
      previousCalendarYearWeek = previousCalendar.substring(0, 6)
      previousCalendarSuffix = previousCalendar.substring(6, 9)
      previousCalendarNextSuffix = "00" + ((previousCalendarSuffix as int) + 1) as String
    }
  }
  // Retrieve customer name
  private void retrieveCustomerInformations() {
    customerName = ""
    modl = ""
    deliveryMethodName = ""
    pltb = ""
    lhcd = ""
    DBAction ocusmaQuery = database.table("OCUSMA").index("00")
      .selection("OKCUNM", "OKPLTB", "OKLHCD", "OKMODL", "OKCFC1").build()
    DBContainer ocusmaRequest = ocusmaQuery.getContainer()
    ocusmaRequest.set("OKCONO", currentCompany)
    ocusmaRequest.set("OKCUNO", inCustomer)
    if (ocusmaQuery.read(ocusmaRequest)) {
      customerName = ocusmaRequest.get("OKCUNM")
      pltb = ocusmaRequest.get("OKPLTB")
      lhcd = ocusmaRequest.get("OKLHCD")
      modl = ocusmaRequest.get("OKMODL")
      cfc1 = ocusmaRequest.get("OKCFC1")
      cfc1 = cfc1.trim()

      if (modl.trim() != "") {
        DBAction csytabQuery = database.table("CSYTAB").index("00").selection("CTTX15").build()
        DBContainer csytabRequest = csytabQuery.getContainer()
        csytabRequest.set("CTCONO", currentCompany)
        csytabRequest.set("CTSTCO", "MODL")
        csytabRequest.set("CTSTKY", modl.trim())
        if (lhcd.trim() == "FR") {
          csytabRequest.set("CTLNCD", "FR")
        } else {
          csytabRequest.set("CTLNCD", "GB")
        }
        if (csytabQuery.read(csytabRequest)) {
          deliveryMethodName = csytabRequest.get("CTTX15")
        }
      }
    }
    mainAssortment = inCustomer.trim() + "0"
    DBAction cugex1Query = database.table("CUGEX1").index("00").selection("F1A230").build()
    DBContainer cugex1Request = cugex1Query.getContainer()
    cugex1Request.set("F1CONO", currentCompany)
    cugex1Request.set("F1FILE", "OCUSMA")
    cugex1Request.set("F1PK01", inCustomer)
    cugex1Request.set("F1PK02", "")
    cugex1Request.set("F1PK03", "")
    cugex1Request.set("F1PK04", "")
    cugex1Request.set("F1PK05", "")
    cugex1Request.set("F1PK06", "")
    cugex1Request.set("F1PK07", "")
    cugex1Request.set("F1PK08", "")
    if (cugex1Query.read(cugex1Request)) {
      String ta230 = cugex1Request.get("F1A230") as String
      if (ta230.trim().length() > 0) {
        mainAssortment = ta230.trim()
      }
    }

    DBAction oprmtxQuery = database.table("OPRMTX").index("00").selection("DXPRRF").build()
    DBContainer oprmtxRequest = oprmtxQuery.getContainer()
    oprmtxRequest.set("DXCONO", currentCompany)
    oprmtxRequest.set("DXPLTB", pltb)
    oprmtxRequest.set("DXPREX", " 5")
    oprmtxRequest.set("DXOBV1", inCustomer)
    oprmtxRequest.set("DXOBV2", "")
    oprmtxRequest.set("DXOBV3", "")
    oprmtxRequest.set("DXOBV4", "")
    oprmtxRequest.set("DXOBV5", "")
    if (oprmtxQuery.read(oprmtxRequest)) {
      prrf = oprmtxRequest.get("DXPRRF") as String
    }

  }

  // Retrieve EXT010
  Closure<?> ext010Reader3 = { DBContainer ext010Result ->
    String suno = ""
    String sule = ext010Result.get("EXSULE")
    String suld = ext010Result.get("EXSULD")
    if (sule.trim() != "") {
      suno = sule
    } else {
      suno = suld
    }
    if (suno.trim() != "") {
      DBAction cidvenQuery = database.table("CIDVEN").index("00").selection("IISUCL").build()
      DBContainer cidvenRequest = cidvenQuery.getContainer()
      cidvenRequest.set("IICONO", currentCompany)
      cidvenRequest.set("IISUNO", suno)
      if (cidvenQuery.read(cidvenRequest)) {
        String sucl = cidvenRequest.get("IISUCL")
        if (sucl.trim() == "100" || sucl.trim() == "200") {
          DBAction csytabSuclQuery = database.table("CSYTAB").index("00").selection("CTTX40").build()
          DBContainer csytabSuclRequest = csytabSuclQuery.getContainer()
          csytabSuclRequest.set("CTCONO", currentCompany)
          csytabSuclRequest.set("CTSTCO", "SUCL")
          csytabSuclRequest.set("CTSTKY", cidvenRequest.get("IISUCL"))
          if (csytabSuclQuery.read(csytabSuclRequest)) {
            fournisseur = csytabSuclRequest.get("CTTX40")
          }
        }
        if (sucl.trim() == "200") {
          entrepot = sule
        }
      }
      DBAction cidmasQuery = database.table("CIDMAS").index("00").selection("IDSUNM").build()
      DBContainer cidmasRequest = cidmasQuery.getContainer()
      cidmasRequest.set("IDCONO", currentCompany)
      cidmasRequest.set("IDSUNO", suno)
      if (cidmasQuery.read(cidmasRequest)) {
        nomFournisseur = cidmasRequest.getString("IDSUNM")
      }
    }
    rscl = ext010Result.get("EXRSCL")
    assortimentLogistique = ext010Result.get("EXASGD")
  }
  // Retrieve EXT040
  Closure<?> ext040ReaderWriteInCalendarFile = { DBContainer ext040Result ->
    chb7 = 0
    DBAction cugex1MitmasQuery = database.table("CUGEX1").index("00").selection("F1A830").build()
    DBContainer cugex1MitmasRequest = cugex1MitmasQuery.getContainer()
    cugex1MitmasRequest.set("F1CONO", currentCompany)
    cugex1MitmasRequest.set("F1FILE", "MITMAS")
    cugex1MitmasRequest.set("F1PK01", ext040Result.get("EXITNO"))
    cugex1MitmasRequest.set("F1PK02", "")
    cugex1MitmasRequest.set("F1PK03", "")
    cugex1MitmasRequest.set("F1PK04", "")
    cugex1MitmasRequest.set("F1PK05", "")
    cugex1MitmasRequest.set("F1PK06", "")
    cugex1MitmasRequest.set("F1PK07", "")
    cugex1MitmasRequest.set("F1PK08", "")
    if (cugex1MitmasQuery.read(cugex1MitmasRequest)) {
      if (cugex1MitmasRequest.get("F1A830") == "20") {
        sigma9DirectDelivery = ext040Result.get("EXITNO")
        sigma9DirectDeliveryassortment = ext040Result.get("EXASCD")
      } else {
        DBAction mitaunQuery = database.table("MITAUN").index("00").selection("MUCOFA").build()
        DBContainer mitaunRequest = mitaunQuery.getContainer()
        mitaunRequest.set("MUCONO", currentCompany)
        mitaunRequest.set("MUITNO", ext040Result.get("EXITNO"))
        mitaunRequest.set("MUAUTP", 1)
        mitaunRequest.set("MUALUN", "COL")
        if (cugex1MitmasRequest.get("F1A830") == "30") {
          savedCOFA = -1
          sigma9_NoDirectDelivery = ext040Result.get("EXITNO")
          sigma9_NoDirectDelivery_assortment = ext040Result.get("EXASCD")
        }
        if (mitaunQuery.read(mitaunRequest)) {
          currentCOFA = mitaunRequest.get("MUCOFA")
          if (savedCOFA == 0) {
            savedCOFA = currentCOFA
            sigma9_NoDirectDelivery = ext040Result.get("EXITNO")
            sigma9_NoDirectDelivery_assortment = ext040Result.get("EXASCD")
          } else {
            if (currentCOFA < savedCOFA) {
              savedCOFA = currentCOFA
              sigma9_NoDirectDelivery = ext040Result.get("EXITNO")
              sigma9_NoDirectDelivery_assortment = ext040Result.get("EXASCD")
            }
          }
        }
      }
    }
  }

  // Retrieve EXT041
  Closure<?> ext041ReaderWriteInCalendarFile = { DBContainer ext041Result ->
    sigma6 = ext041Result.get("EXPOPN")
    logMessage("DEBUG", "ext041ReaderWriteInCalendarFile ${sigma6}")
    sigma9DirectDelivery = ""
    sigma9_NoDirectDelivery = ""
    sigma9DirectDeliveryassortment = ""
    sigma9_NoDirectDelivery_assortment = ""
    savedCOFA = 0
    // Read all ITNOs attached to the current POPN
    DBAction ext040Query = database.table("EXT040").index("00").selection("EXITNO", "EXASCD").build()
    DBContainer ext040Request = ext040Query.getContainer()
    ext040Request.set("EXCONO", currentCompany)
    ext040Request.set("EXCUNO", inCustomer)
    ext040Request.set("EXCDNN", inCalendar)
    ext040Request.set("EXPOPN", sigma6)
    if (!ext040Query.readAll(ext040Request, 4, 10000, ext040ReaderWriteInCalendarFile)) {
    }
    writeInCalendarFile()
  }
  // Retrieve EXT041
  Closure<?> ext041ReaderWriteGapFile = { DBContainer ext041Record ->
    sigma6 = ext041Record.get("EXPOPN")
    libelleSigma6 = ext041Record.get("EXFUDS")
    foundSigma6 = false
    DBAction ext040Query = database.table("EXT040").index("00").selection("EXITNO", "EXASCD").build()
    DBContainer ext040Request = ext040Query.getContainer()
    ext040Request.set("EXCONO", currentCompany)
    ext040Request.set("EXCUNO", inCustomer)
    ext040Request.set("EXCDNN", inCalendar)
    ext040Request.set("EXPOPN", sigma6)

    // Retrieve EXT040
    Closure<?> ext040Reader = { DBContainer EXT040 ->
      foundSigma6 = true
    }
    if (!ext040Query.readAll(ext040Request, 4, 1, ext040Reader)) {
    }

    if (!foundSigma6) {
      Map<String, String> dtaMITHRY = getMithry(sigma6)
      line = sigma6 + ";" + libelleSigma6 + ";" + "1"
      line += ";" + dtaMITHRY["HIE1"] + ";" + dtaMITHRY["HIE1D"]
      line += ";" + dtaMITHRY["HIE2"].replace(dtaMITHRY["HIE1"], "") + ";" + dtaMITHRY["HIE2D"]
      line += ";" + dtaMITHRY["HIE3"].replace(dtaMITHRY["HIE2"], "") + ";" + dtaMITHRY["HIE3D"]
      line += ";" + dtaMITHRY["HIE4"].replace(dtaMITHRY["HIE3"], "") + ";" + dtaMITHRY["HIE4D"]
      line += ";" + dtaMITHRY["HIE5"].replace(dtaMITHRY["HIE4"], "") + ";" + dtaMITHRY["HIE5D"]

      countLines++
      lines += line + (countLines < 5000 ? "\r\n" : "")
      if (countLines == 5000) {
        writeInFile("", lines)
        countLines = 0
        lines = ""
      }
      lineInGapFile++
    }
  }

  /**
   * Retrieve Item by reading MITPOP
   * Get Item Data by Reading MITMAS
   * Get Hierarchy Informations by Reading MITHRY
   * for each Hierarchy retrieve Description
   * @param sigma6
   * @return
   */
  private Map<String, String> getMithry(String sigma6) {
    Map<String, String> response = [
      "HIE1"   : ""
      , "HIE1D": ""
      , "HIE2" : ""
      , "HIE2D": ""
      , "HIE3" : ""
      , "HIE3D": ""
      , "HIE4" : ""
      , "HIE4D": ""
      , "HIE5" : ""
      , "HIE5D": ""
    ]


    ExpressionFactory mitpopExpression = database.getExpressionFactory("MITPOP")
    mitpopExpression = mitpopExpression.eq("MPREMK", "SIGMA6")
    DBAction mitpopQuery = database.table("MITPOP").index("10").matching(mitpopExpression).selection("MPITNO").build()
    DBContainer mitpopRequest = mitpopQuery.getContainer()
    mitpopRequest.set("MPCONO", currentCompany)
    mitpopRequest.set("MPALWT", 1)
    mitpopRequest.set("MPALWQ", "")
    mitpopRequest.set("MPPOPN", sigma6)

    String tITNO = ""
    Closure<?> mitpopReaderLocal = { DBContainer mitpopResult ->
      tITNO = mitpopResult.get("MPITNO") as String
    }
    if (!mitpopQuery.readAll(mitpopRequest, 4, 1, mitpopReaderLocal)) {
    }

    DBAction mitmasQuery = database.table("MITMAS").index("00").selection(
      "MMHIE1"
      , "MMHIE2"
      , "MMHIE3"
      , "MMHIE4"
      , "MMHIE5"
    )
      .build()
    DBContainer mitmasRequest = mitmasQuery.getContainer()
    mitmasRequest.set("MMCONO", currentCompany)
    mitmasRequest.set("MMITNO", tITNO)
    if (mitmasQuery.read(mitmasRequest)) {
      response["HIE1"] = mitmasRequest.getString("MMHIE1").trim()
      response["HIE2"] = mitmasRequest.getString("MMHIE2").trim()
      response["HIE3"] = mitmasRequest.getString("MMHIE3").trim()
      response["HIE4"] = mitmasRequest.getString("MMHIE4").trim()
      response["HIE5"] = mitmasRequest.getString("MMHIE5").trim()
    }
    response["HIE1D"] = getMithryDescription(1, response["HIE1"]).trim()
    response["HIE2D"] = getMithryDescription(2, response["HIE2"]).trim()
    response["HIE3D"] = getMithryDescription(3, response["HIE3"]).trim()
    response["HIE4D"] = getMithryDescription(4, response["HIE4"]).trim()
    response["HIE5D"] = getMithryDescription(5, response["HIE5"]).trim()

    return response

  }

  /**
   *
   * @param level
   * @param hie0
   * @return
   */
  private String getMithryDescription(int level, String hie0) {
    if (mithryMap == null) {
      mithryMap = new LinkedHashMap<String, String>()
    }

    if (mithryMap.containsKey(hie0)) {
      return mithryMap.get(level + "_" + hie0)
    }
    DBAction mithryQuery = database.table("MITHRY").index("00").selection(
      "HITX40"
    )
      .build()
    DBContainer mithryRequest = mithryQuery.getContainer()
    mithryRequest.set("HICONO", currentCompany)
    mithryRequest.set("HIHLVL", level)
    mithryRequest.set("HIHIE0", hie0)
    if (mithryQuery.read(mithryRequest)) {
      String tx40 = mithryRequest.get("HITX40") as String
      mithryMap.put(level + "_" + hie0, tx40)
      return tx40
    }
    return ""
  }


  // Retrieve EXT043
  Closure<?> ext043Reader = { DBContainer ext043Result ->
    line = ext043Result.get("EXEMAL")
    countLines++
    lines += line + (countLines < 5000 ? "\r\n" : "")
    if (countLines == 5000) {
      writeInFile("", lines)
      countLines = 0
      lines = ""
    }
  }
  // Retrieve CCUCON
  Closure<?> ccuconReader = { DBContainer ccuconResult ->
    line = ccuconResult.get("CCEMAL")
    countLines++
    lines += line + (countLines < 5000 ? "\r\n" : "")
    if (countLines == 5000) {
      writeInFile("", lines)
      countLines = 0
      lines = ""
    }
  }


  // Retrieve sigma6
  public void retrieveSigma6() {
    sigma6 = ""
    ExpressionFactory mitpopExpression = database.getExpressionFactory("MITPOP")
    mitpopExpression = mitpopExpression.eq("MPREMK", "SIGMA6")
    DBAction mitpopQuery = database.table("MITPOP").index("30").matching(mitpopExpression).selection("MPPOPN").build()
    DBContainer mitpopRequest = mitpopQuery.getContainer()
    mitpopRequest.set("MPCONO", currentCompany)
    mitpopRequest.set("MPALWT", 1)
    mitpopRequest.set("MPITNO", sigma9)
    // Retrieve MITPOP
    Closure<?> mitpopReaderS6 = { DBContainer mitpopResult ->
      sigma6 = mitpopResult.get("MPPOPN")
    }
    if (!mitpopQuery.readAll(mitpopRequest, 3, 1, mitpopReaderS6)) {
    }
  }
  // Retrieve sales price
  public void retrieveSalesPrice() {
    prixVente = "0"
    savedFromValidDate = 0
    currentFromValidDate = 0
    ExpressionFactory oprbasExpression = database.getExpressionFactory("OPRBAS")
    oprbasExpression = oprbasExpression.le("ODVFDT", currentDate as String).and(oprbasExpression.ge("ODLVDT", currentDate as String))
    oprbasExpression = oprbasExpression.and(oprbasExpression.eq("ODITNO", sigma9))
    oprbasExpression = oprbasExpression.and(oprbasExpression.eq("ODCUNO", inCustomer))
    DBAction oprbasQuery = database.table("OPRBAS").index("00").matching(oprbasExpression).selection("ODVFDT", "ODSAPR", "ODSPUN").build()
    DBContainer oprbasRequest = oprbasQuery.getContainer()
    oprbasRequest.set("ODCONO", currentCompany)
    oprbasRequest.set("ODPRRF", prrf)
    oprbasQuery.readAll(oprbasRequest, 2, 10000, oprbasReader)
  }
  // write EXT040 and EXT041
  public void writeInExt040Ext041() {
    retrievePreviousCalendar()
    // Check customer assortment
    DBAction ext042Query = database.table("EXT042").index("00").selection("EXASCD").build()
    DBContainer ext042Request = ext042Query.getContainer()
    ext042Request.set("EXCONO", currentCompany)
    ext042Request.set("EXCUNO", inCustomer)
    ext042Request.set("EXCDNN", inCalendar)

    // Retrieve EXT042
    Closure<?> ext042ReaderWriteExt040Ext041 = { DBContainer ext042Result ->
      //assortment = EXT042.get("EXASCD")//D° 20240503 FLEBARS
      DBAction oasitnQuery = database.table("OASITN").index("00").selection("OIITNO", "OIASCD").build()
      DBContainer oasitnRequest = oasitnQuery.getContainer()
      oasitnRequest.set("OICONO", currentCompany)
      oasitnRequest.set("OIASCD", ext042Result.get("EXASCD"))

      // Retrieve OASCUS
      Closure<?> oasitnReader = { DBContainer oasitnResult ->
        sigma9 = oasitnResult.get("OIITNO")
        if (itemIsOK()) {
          retrieveSigma6()
          retrieveSalesPrice()
          writeExt040()
          writeExt041()
          countOasitn++
        }
      }
      //Manage loop on oasitn records we can have max 50 000 records per customer
      boolean doLoop = true
      int nbIteration = 0

      while (doLoop) {
        if (nbIteration > 0) {
          PositionKey position = oasitnQuery.getPositionKey()
          if (position instanceof PositionEmpty) {
            doLoop = false
            break
          } else {
            oasitnQuery = database.table("OASITN")
              .index("00")
              .selection("OIITNO", "OIASCD")
              .position(position)
              .build()
          }
        }
        if (!oasitnQuery.readAll(oasitnRequest, 2, 10000, oasitnReader)) {
        }
        nbIteration++
        if (nbIteration > 5) {//max 50 0000 records
          logMessage("ERROR", "Nombre d'itération OASITN trop important cuno:${inCustomer} cdnn:${inCalendar}")
          break
        }
      }
    }
    if (!ext042Query.readAll(ext042Request, 3, 1, ext042ReaderWriteExt040Ext041)) {
    }
  }
  // write EXT040
  public void writeExt040() {
    LocalDateTime timeOfCreation = LocalDateTime.now()
    DBAction ext040Query = database.table("EXT040").index("00").build()
    DBContainer ext040Request = ext040Query.getContainer()
    ext040Request.set("EXCONO", currentCompany)
    ext040Request.set("EXCDNN", inCalendar)
    ext040Request.set("EXCUNO", inCustomer)
    ext040Request.set("EXPOPN", sigma6)
    ext040Request.set("EXITNO", sigma9)
    if (!ext040Query.read(ext040Request)) {
      ext040Request.set("EXASCD", assortment)
      ext040Request.set("EXSAPR", prixVente as double)
      ext040Request.setInt("EXRGDT", timeOfCreation.format(DateTimeFormatter.ofPattern("yyyyMMdd")) as Integer)
      ext040Request.setInt("EXRGTM", timeOfCreation.format(DateTimeFormatter.ofPattern("HHmmss")) as Integer)
      ext040Request.setInt("EXLMDT", timeOfCreation.format(DateTimeFormatter.ofPattern("yyyyMMdd")) as Integer)
      ext040Request.setInt("EXCHNO", 1)
      ext040Request.set("EXCHID", program.getUser())
      ext040Query.insert(ext040Request)
    }
  }
  // write EXT041
  public void writeExt041() {
    LocalDateTime timeOfCreation = LocalDateTime.now()
    DBAction ext041Query = database.table("EXT041").index("00").build()
    DBContainer ext041Request = ext041Query.getContainer()
    ext041Request.set("EXCONO", currentCompany)
    ext041Request.set("EXCDNN", inCalendar)
    ext041Request.set("EXCUNO", inCustomer)
    ext041Request.set("EXPOPN", sigma6)
    if (!ext041Query.read(ext041Request)) {
      ext041Request.set("EXFUDS", libelleArticle)
      ext041Request.setInt("EXRGDT", timeOfCreation.format(DateTimeFormatter.ofPattern("yyyyMMdd")) as Integer)
      ext041Request.setInt("EXRGTM", timeOfCreation.format(DateTimeFormatter.ofPattern("HHmmss")) as Integer)
      ext041Request.setInt("EXLMDT", timeOfCreation.format(DateTimeFormatter.ofPattern("yyyyMMdd")) as Integer)
      ext041Request.setInt("EXCHNO", 1)
      ext041Request.set("EXCHID", program.getUser())
      ext041Query.insert(ext041Request)
    }
  }
  // Check if item must be selected
  public boolean itemIsOK() {
    stat = ""
    libelleArticle = ""
    DBAction mitmasQuery = database.table("MITMAS").index("00").selection("MMSTAT", "MMFUDS").build()
    DBContainer mitmasRequest = mitmasQuery.getContainer()
    mitmasRequest.set("MMCONO", currentCompany)
    mitmasRequest.set("MMITNO", sigma9)
    if (mitmasQuery.read(mitmasRequest)) {
      stat = mitmasRequest.get("MMSTAT")
      libelleArticle = mitmasRequest.get("MMFUDS")
      if (lhcd.trim() != "FR") {
        DBAction mitladQuery = database.table("MITLAD").index("00").selection("MDFUDS").build()
        DBContainer mitladRequest = mitladQuery.getContainer()
        mitladRequest.set("MDCONO", currentCompany)
        mitladRequest.set("MDITNO", sigma9)
        mitladRequest.set("MDLNCD", "GB")
        if (mitladQuery.read(mitladRequest)) {
          libelleArticle = mitladRequest.get("MDFUDS")
        }
      }
    }

    chb7 = 0
    DBAction cugex1MitmasQuery = database.table("CUGEX1").index("00").selection("F1CHB7").build()
    DBContainer cugex1MitmasRequest = cugex1MitmasQuery.getContainer()
    cugex1MitmasRequest.set("F1CONO", currentCompany)
    cugex1MitmasRequest.set("F1FILE", "MITMAS")
    cugex1MitmasRequest.set("F1PK01", sigma9)
    cugex1MitmasRequest.set("F1PK02", "")
    cugex1MitmasRequest.set("F1PK03", "")
    cugex1MitmasRequest.set("F1PK04", "")
    cugex1MitmasRequest.set("F1PK05", "")
    cugex1MitmasRequest.set("F1PK06", "")
    cugex1MitmasRequest.set("F1PK07", "")
    cugex1MitmasRequest.set("F1PK08", "")
    if (cugex1MitmasQuery.read(cugex1MitmasRequest)) {
      chb7 = cugex1MitmasRequest.get("F1CHB7")
    }

    cmde = ""
    ExpressionFactory ext010Expression = database.getExpressionFactory("EXT010")
    ext010Expression = ext010Expression.le("EXFVDT", currentDate as String).and(ext010Expression.ge("EXLVDT", currentDate as String))
    DBAction ext010Query = database.table("EXT010").index("02").matching(ext010Expression).selection("EXCMDE").build()
    DBContainer ext010Request = ext010Query.getContainer()
    ext010Request.set("EXCONO", currentCompany)
    ext010Request.set("EXCUNO", inCustomer)
    ext010Request.set("EXITNO", sigma9)
    // Retrieve EXT010
    Closure<?> ext010Reader = { DBContainer ext010Result ->
      cmde = ext010Result.get("EXCMDE")
    }
    if (ext010Query.readAll(ext010Request, 3, 1, ext010Reader)) {
    }

    if (stat == "20" && chb7 == 0 && cmde == "1") {
      return true
    } else {
      return false
    }
  }

  // Write to format file
  public void createFormatFile() {
    if (inSchedule == 1) {
      inFormatTXT = 0
      inFormatCSV = 0
      inFormatXLSX = 0
      DBAction cugex1OcusmaQuery = database.table("CUGEX1").index("00").selection("F1N296", "F1N396", "F1N496").build()
      DBContainer CUGEX1 = cugex1OcusmaQuery.getContainer()
      CUGEX1.set("F1CONO", currentCompany)
      CUGEX1.set("F1FILE", "OCUSMA")
      CUGEX1.set("F1PK01", inCustomer)
      if (cugex1OcusmaQuery.read(CUGEX1)) {
        inFormatTXT = CUGEX1.get("F1N296") as Integer
        inFormatCSV = CUGEX1.get("F1N396") as Integer
        inFormatXLSX = CUGEX1.get("F1N496") as Integer
      }
    }
    logFileName = fileJobNumber + "-" + inCustomer + "-" + inCalendar + "-" + "formats.txt"
    header = "CSV" + ";" + "TXT" + ";" + "XLSX"
    writeInFile(header, "")
    line = inFormatCSV + ";" + inFormatTXT + ";" + inFormatXLSX
    writeInFile("", line)
  }

  // Write to customer email file
  public void createCustomerEmailFile() {
    logFileName = fileJobNumber + "-" + inCustomer + "-" + inCalendar + "-" + "mailsClients.txt"
    header = "Mail client"
    writeInFile(header, "")
    countLines = 0
    lines = ""

    if (inAllContacts == 1) {
      DBAction ccuconQuery = database.table("CCUCON").index("10").selection("CCEMAL").build()
      DBContainer ccuconRequest = ccuconQuery.getContainer()
      ccuconRequest.set("CCCONO", currentCompany)
      ccuconRequest.set("CCERTP", 1)
      ccuconRequest.set("CCEMRE", inCustomer)
      if (!ccuconQuery.readAll(ccuconRequest, 3, 10000, ccuconReader)) {
      }
    } else {
      DBAction ext043Query = database.table("EXT043").index("00").selection("EXEMAL").build()
      DBContainer ext043Reqquest = ext043Query.getContainer()
      ext043Reqquest.set("EXCONO", currentCompany)
      ext043Reqquest.set("EXCUNO", inCustomer)
      ext043Reqquest.set("EXCDNN", inCalendar)
      if (!ext043Query.readAll(ext043Reqquest, 3, 10000, ext043Reader)) {
      }
    }

    if (countLines > 0) {
      writeInFile("", lines)
    }
  }
  // Write to internal email file
  public void createInternalEmailFile() {
    logFileName = fileJobNumber + "-" + inCustomer + "-" + inCalendar + "-" + "mailsInternes.txt"
    header = "Mail interne"
    writeInFile(header, "")
    countLines = 0
    lines = ""

    if (inSchedule == 1) {
      ExpressionFactory ccuconExpression = database.getExpressionFactory("CCUCON")
      ccuconExpression = ccuconExpression.eq("CCRFTP", "I-COM")
      DBAction ccuconQuery = database.table("CCUCON").index("10").matching(ccuconExpression).selection("CCEMAL").build()
      DBContainer ccuconRequest = ccuconQuery.getContainer()
      ccuconRequest.set("CCCONO", currentCompany)
      ccuconRequest.set("CCERTP", 0)
      ccuconRequest.set("CCEMRE", inCustomer)
      if (!ccuconQuery.readAll(ccuconRequest, 3, 10000, ccuconReader)) {
      }
    } else {
      DBAction ext044Query = database.table("EXT044").index("00").selection("EXEMAL").build()
      DBContainer ext044Request = ext044Query.getContainer()
      ext044Request.set("EXCONO", currentCompany)
      ext044Request.set("EXCUNO", inCustomer)
      ext044Request.set("EXCDNN", inCalendar)

      // Retrieve EXT044
      Closure<?> ext044Reader = { DBContainer ext044Result ->
        line = ext044Result.get("EXEMAL")
        countLines++
        lines += line + (countLines < 5000 ? "\r\n" : "")
        if (countLines == 5000) {
          writeInFile("", lines)
          countLines = 0
          lines = ""
        }
      }
      if (!ext044Query.readAll(ext044Request, 3, 10000, ext044Reader)) {
      }
    }

    if (countLines > 0) {
      writeInFile("", lines)
    }
  }

  // write EXT045
  public void writeExt045() {
    LocalDateTime timeOfCreation = LocalDateTime.now()
    DBAction ext045Query = database.table("EXT045").index("00").build()
    DBContainer ext045Request = ext045Query.getContainer()
    ext045Request.set("EXCONO", currentCompany)
    ext045Request.set("EXCDNN", inCalendar)
    ext045Request.set("EXCUNO", inCustomer)
    ext045Request.set("EXPOPN", sigma6)
    ext045Request.set("EXITNO", sigma9)
    if (!ext045Query.read(ext045Request)) {
      ext045Request.setInt("EXRGDT", timeOfCreation.format(DateTimeFormatter.ofPattern("yyyyMMdd")) as Integer)
      ext045Request.setInt("EXRGTM", timeOfCreation.format(DateTimeFormatter.ofPattern("HHmmss")) as Integer)
      ext045Request.setInt("EXLMDT", timeOfCreation.format(DateTimeFormatter.ofPattern("yyyyMMdd")) as Integer)
      ext045Request.setInt("EXCHNO", 1)
      ext045Request.set("EXCHID", program.getUser())
      ext045Query.insert(ext045Request)
    }
  }

  // write EXT046
  public void writeExt046() {
    LocalDateTime timeOfCreation = LocalDateTime.now()
    DBAction ext046Query = database.table("EXT046").index("00").build()
    DBContainer ext046Request = ext046Query.getContainer()
    ext046Request.set("EXCONO", currentCompany)
    ext046Request.set("EXCDNN", inCalendar)
    ext046Request.set("EXCUNO", inCustomer)
    ext046Request.set("EXPOPN", sigma6)
    ext046Request.set("EXITNO", sigma9)
    if (!ext046Query.read(ext046Request)) {
      ext046Request.set("EXSAPR", sapr)
      ext046Request.set("EXAPPR", appr)
      ext046Request.setInt("EXRGDT", timeOfCreation.format(DateTimeFormatter.ofPattern("yyyyMMdd")) as Integer)
      ext046Request.setInt("EXRGTM", timeOfCreation.format(DateTimeFormatter.ofPattern("HHmmss")) as Integer)
      ext046Request.setInt("EXLMDT", timeOfCreation.format(DateTimeFormatter.ofPattern("yyyyMMdd")) as Integer)
      ext046Request.setInt("EXCHNO", 1)
      ext046Request.set("EXCHID", program.getUser())
      ext046Query.insert(ext046Request)
    }
  }

  // Write to calendar file
  public void createCalendarFile() {
    if (lhcd.trim() == "FR") {
      logFileName = fileJobNumber + "-" + inCustomer + "-" + inCalendar + "-" + "cadencier.txt"
      header = "Numéro cadencier" + ";" + "Date création" + ";" + "Heure création" + ";" + "Code magasin" + ";" + "Nom magasin" + ";" + "Mode de livraison"
      writeInFile(header, "")
      line = inCalendar + ";" + creationDate + ";" + creationTime + ";" + inCustomer + ";" + customerName + ";" + deliveryMethodName
      writeInFile("", line + "\r\n")
      header = "SIGMA6" + ";" + "UL" + ";" + "Code article" + ";" + "EAN13" + ";" + "Libellé article" + ";" + "Code article remplacé" + ";" + "Libellé article remplacé" + ";" + "Prix cadencier précédent" + ";" + "Prix de vente" + ";" + "Evolution prix cadencier précédent (%)" + ";" + "Unité prix de vente" + ";" + "DLC_Totale" + ";" + "DLGR" + ";" + "Code marque" + ";" + "Libellé code marque" + ";" + "Code marketing" + ";" + "Libellé code marketing" + ";" + "Saisonnalité" + ";" + "Nouveauté" + ";" + "Langue packaging" + ";" + "Code département " + ";" + "Libellé département" + ";" + "Code rayon " + ";" + "Libellé rayon" + ";" + "Code famille" + ";" + "Libellé famille" + ";" + "Code sous famille" + ";" + "Libellé Sous famille" + ";" + "Unité de besoin" + ";" + "Libellé unité de besoin" + ";" + "Fournisseur (CASINO ou industriel)" + ";" + "Nom du fournisseur" + ";" + "Fournisseur d'origine" + ";" + "Nom du fournisseur d'origine" + ";" + "Pays d'origine" + ";" + "Code douanier" + ";" + "Type Appro" + ";" + "Entrepôt Casino" + ";" + "Taille" + ";" + "Couleur" + ";" + "Modèle" + ";" + "Diffusion" + ";" + "FBA" + ";" + "DUN14" + ";" + "Minimum de commande" + ";" + "PCB" + ";" + "Nbre couche / Palette" + ";" + "Nbre colis par couche" + ";" + "Nbre colis par palette" + ";" + "Nbre UVC par palette" + ";" + "Contenance (exprimée en L pour l'UC)" + ";" + "Poids net (exprimé en Kg pour l'UC)" + ";" + "Poids brut (exprimé en Kg pour le colis)" + ";" + "Volume colis (exprimé en m3)" + ";" + "Hauteur UVC (exprimé en m)" + ";" + "Largeur UVC (exprimé en m)" + ";" + "Longueur UVC (exprimé en m)" + ";" + "Hauteur colis (exprimé en m)" + ";" + "Largeur colis (exprimé en m)" + ";" + "Longueur colis (exprimé en m)" + ";" + "Degré d'alcool" + ";" + "Réf en droit" + ";" + "Type de box" + ";" + "Nbre de composant box" + ";" + "Température" + ";" + "Assortiment logistique"
    } else {
      logFileName = fileJobNumber + "-" + inCustomer + "-" + inCalendar + "-" + "cadencier.txt"
      header = "Schedule number" + ";" + "Creation date" + ";" + "Creation time" + ";" + "Store" + ";" + "Store name" + ";" + "Default delivery method"
      writeInFile(header, "")
      line = inCalendar + ";" + creationDate + ";" + creationTime + ";" + inCustomer + ";" + customerName + ";" + deliveryMethodName
      writeInFile("", line + "\r\n")
      header = "SIGMA6" + ";" + "UL" + ";" + "Item code" + ";" + "Barcode" + ";" + "Item designation" + ";" + "Replaced item code" + ";" + "Replaced item designation" + ";" + "Previous unit invoiced price" + ";" + "Unit invoiced price" + ";" + "Price evolution" + ";" + "Selling unit" + ";" + "Total DLC" + ";" + "Warehouse shelf life" + ";" + "Brand code" + ";" + "Brand code name" + ";" + "Marketing code" + ";" + "Marketing code name" + ";" + "Seasonality (P = Permanent S = Seasonality)" + ";" + "New" + ";" + "Pack language" + ";" + "Department Code" + ";" + "Department" + ";" + "Section code" + ";" + "Section" + ";" + "Family code" + ";" + "Family" + ";" + "Sub Family code" + ";" + "Sub family" + ";" + "Unit code" + ";" + "Unit" + ";" + "Supplier" + ";" + "Supplier name" + ";" + "Manufacturer" + ";" + "Manufacturer name" + ";" + "Origin" + ";" + "Custom code" + ";" + "Supply type" + ";" + "Warehouse" + ";" + "Size" + ";" + "Color" + ";" + "Box type" + ";" + "Diffusion" + ";" + "FBA" + ";" + "DUN14" + ";" + "MOQ" + ";" + "SKU/Case" + ";" + "Layer/Pallet" + ";" + "Case/Layer" + ";" + "Case/Pallet" + ";" + "SKU/Pallet" + ";" + "Containing" + ";" + "Net weight" + ";" + "Gross weight" + ";" + "Case volume" + ";" + "Unit height" + ";" + "Unit width" + ";" + "Unit length" + ";" + "Case height" + ";" + "Case width" + ";" + "Case length" + ";" + "Alcohol degree" + ";" + "Legal reference" + ";" + "Box type" + ";" + "Number of box components" + ";" + "Temperature" + ";" + "Logistic assortment"
    }
    writeInFile(header, "")
    countLines = 0
    lines = ""

    // Read all POPNs in the current calendar
    DBAction ext041Query = database.table("EXT041").index("00").selection("EXPOPN").build()
    DBContainer ext041Request = ext041Query.getContainer()
    ext041Request.set("EXCONO", currentCompany)
    ext041Request.set("EXCUNO", inCustomer)
    ext041Request.set("EXCDNN", inCalendar)

    //Manage loop on ext041 records we can have max 50 000 records per customer
    boolean doLoop = true
    int nbIteration = 0
    while (doLoop) {
      if (nbIteration > 0) {
        PositionKey position = ext041Query.getPositionKey()
        if (position instanceof PositionEmpty) {
          doLoop = false
          break
        } else {
          ext041Query = database.table("EXT041")
            .index("00")
            .selection("EXPOPN")
            .position(position)
            .build()
        }
      }
      if (!ext041Query.readAll(ext041Request, 3, 10000, ext041ReaderWriteInCalendarFile)) {
      }
      nbIteration++
      if (nbIteration > 5) {//max 50 0000 records
        logMessage("ERROR", "Nombre d'itération trop important cuno:${inCustomer} cdnn:${inCalendar}")
        break
      }
    }

    if (countLines > 0) {
      writeInFile("", lines)
    }
    logMessage("INFO", "client:${inCustomer} cadencier:${inCalendar} nombre de lignes:${lineIncadencier}")
  }

  /**
   * Write to calendar file
   */
  public void writeInCalendarFile() {

    Closure<?> ext010Reader = { DBContainer ext010Result ->
      initLine()

      if (prixVente == "0")
        writeExt045()

      sapr = prixVente as double
      appr = 0
      LocalDateTime timeOfCreation = LocalDateTime.now()
      DBAction mitfacQuery = database.table("MITFAC").index("00").selection("M9APPR").build()
      DBContainer mitfacRequest = mitfacQuery.getContainer()
      mitfacRequest.set("M9CONO", currentCompany)
      mitfacRequest.set("M9FACI", "E10")
      mitfacRequest.set("M9ITNO", sigma9)
      if (mitfacQuery.read(mitfacRequest)) {
        appr = mitfacRequest.get("M9APPR")
      }
      if (sapr < appr && !isAlcool)//A°FLB 20240524
        writeExt046()

      if (sapr >= appr || isAlcool) {//A°FLB 20240524
        line = sigma6.trim() + ";" + ul.trim() + ";" + sigma9.trim() + ";" + ean13.trim() + ";" + libelleArticle.trim() + ";" + codeArticleRemplace.trim() + ";" + libelleArticleRemplace.trim() + ";" + prixCadencierPrecedent.trim() + ";" + prixVente.trim() + ";" + evolutionPrixCadencierPrecedent.trim() + ";" + unitePrixVente.trim() + ";" + dlc.trim() + ";" + dlgr.trim() + ";" + codeMarque.trim() + ";" + libelleCodeMarque.trim() + ";" + codeMarketing.trim() + ";" + libelleCodeMarketing.trim() + ";" + saisonnalite.trim() + ";" + nouveaute.trim() + ";" + languePackaging.trim() + ";" + codeDepartement.trim() + ";" + nomDepartement.trim() + ";" + codeRayon.trim() + ";" + nomRayon.trim() + ";" + codeFamille.trim() + ";" + nomFamille.trim() + ";" + codeSousFamille.trim() + ";" + nomSousFamille.trim() + ";" + uniteBesoin.trim() + ";" + nomUniteBesoin.trim() + ";" + fournisseur.trim() + ";" + nomFournisseur.trim() + ";" + fournisseurOrigine.trim() + ";" + nomFournisseurOrigine.trim() + ";" + nomPaysOrigine.trim() + ";" + codeDouanier.trim() + ";" + typeAppro.trim() + ";" + entrepot.trim() + ";" + taille.trim() + ";" + couleur.trim() + ";" + modele.trim() + ";" + diffusion.trim() + ";" + fba.trim() + ";" + dun14.trim() + ";" + minimumCommande.trim() + ";" + parCombien.trim() + ";" + nbCouchePalette.trim() + ";" + nbColisParCouche.trim() + ";" + nbColisParPalette.trim() + ";" + nbUVCParPalette.trim() + ";" + contenance.trim() + ";" + poidsNet.trim() + ";" + poidsBrut.trim() + ";" + volumeColis.trim() + ";" + hauteurUVC.trim() + ";" + largeurUVC.trim() + ";" + longueurUVC.trim() + ";" + hauteurColis.trim() + ";" + largeurColis.trim() + ";" + longueurColis.trim() + ";" + degreAlcool.trim() + ";" + refDroit.trim() + ";" + typeBox.trim() + ";" + nombreComposantBox.trim() + ";" + temperature.trim() + ";" + assortimentLogistique.trim()
        lines += line + "\r\n"
        countLines++
        lineIncadencier++
        if (countLines == 5000) {
          writeInFile("", lines)
          countLines = 0
          lines = ""
        }
      }
    }


    if (sigma9DirectDelivery.trim() != "") {
      sigma9 = sigma9DirectDelivery
      ExpressionFactory ext010Expression = database.getExpressionFactory("EXT010")
      ext010Expression = ext010Expression.le("EXFVDT", currentDate as String).and(ext010Expression.ge("EXLVDT", currentDate as String))
      DBAction ext010Query = database.table("EXT010").index("02").matching(ext010Expression).selection("EXCMDE").build()
      DBContainer ext010Request = ext010Query.getContainer()
      ext010Request.set("EXCONO", currentCompany)
      ext010Request.set("EXCUNO", inCustomer)
      ext010Request.set("EXITNO", sigma9DirectDelivery)

      if (ext010Query.readAll(ext010Request, 3, 1, ext010Reader)) {
      }
    }
    if (sigma9_NoDirectDelivery.trim() != "") {
      sigma9 = sigma9_NoDirectDelivery
      //assortment = sigma9_NoDirectDelivery_assortment//D°20240503 FLEBARS
      ExpressionFactory ext010Expression = database.getExpressionFactory("EXT010")
      ext010Expression = ext010Expression.le("EXFVDT", currentDate as String).and(ext010Expression.ge("EXLVDT", currentDate as String))
      DBAction ext010Query = database.table("EXT010").index("02").matching(ext010Expression).selection("EXCMDE").build()
      DBContainer ext010Request = ext010Query.getContainer()
      ext010Request.set("EXCONO", currentCompany)
      ext010Request.set("EXCUNO", inCustomer)
      ext010Request.set("EXITNO", sigma9_NoDirectDelivery)
      if (ext010Query.readAll(ext010Request, 3, 1, ext010Reader)) {
      }
    }
  }

  // Write to gap file
  public void createGapFile() {
    logFileName = fileJobNumber + "-" + inCustomer + "-" + inCalendar + "-" + "ecarts.txt"
    header = "Numéro cadencier" + ";" + "Date création" + ";" + "Heure création" + ";" + "Code magasin" + ";" + "Nom magasin"
    writeInFile(header, "")
    line = inCalendar + ";" + creationDate + ";" + creationTime + ";" + inCustomer + ";" + customerName
    writeInFile("", line + "\r\n")
    header = "SIGMA6" + ";" + "Nom du SIGMA6" + ";" + "Supprimé"
    header += ";" + "Département" + ";" + "Description"
    header += ";" + "Rayon" + ";" + "Description"
    header += ";" + "Famille" + ";" + "Description"
    header += ";" + "ss famille" + ";" + "Description"
    header += ";" + "Unité Besoin" + ";" + "Description"
    writeInFile(header, "")
    countLines = 0
    lines = ""

    // Read all POPNs in the previous calendar
    DBAction ext041Query = database.table("EXT041").index("00").selection("EXPOPN", "EXFUDS").build()
    DBContainer ext041Request = ext041Query.getContainer()
    ext041Request.set("EXCONO", currentCompany)
    ext041Request.set("EXCUNO", inCustomer)
    ext041Request.set("EXCDNN", previousCalendar)

    //Manage loop on ext041 records we can have max 50 000 records per customer
    boolean doLoop = true
    int nbIteration = 0
    while (doLoop) {
      if (nbIteration > 0) {
        PositionKey position = ext041Query.getPositionKey()
        if (position instanceof PositionEmpty) {
          doLoop = false
          break
        } else {
          ext041Query = database.table("EXT041")
            .index("00")
            .selection("EXPOPN", "EXFUDS")
            .position(position)
            .build()
        }
      }
      if (!ext041Query.readAll(ext041Request, 3, 10000, ext041ReaderWriteGapFile)) {
      }
      nbIteration++
      if (nbIteration > 5) {//max 50 0000 records
        logMessage("ERROR", "Nombre d'itération trop important cuno:${inCalendar} cdnn:${inCalendar}")
        break
      }
    }
    if (countLines > 0) {
      writeInFile("", lines)
    }
    logMessage("INFO", "client:${inCustomer} ecarts cadenciers:${inCalendar}/${previousCalendar} nombre de lignes:${lineInGapFile}")
  }
  // Write to alert file
  public void createAlertFile() {
    logFileName = fileJobNumber + "-" + inCustomer + "-" + inCalendar + "-" + "alertes.txt"
    header = "Numéro cadencier" + ";" + "Date création" + ";" + "Heure création" + ";" + "Code magasin" + ";" + "Nom magasin"
    writeInFile(header, "")
    line = inCalendar + ";" + creationDate + ";" + creationTime + ";" + inCustomer + ";" + customerName
    writeInFile("", line + "\r\n")
    header = "Code article" + ";" + "Nom article" + ";" + "Prix à 0"
    writeInFile(header, "")
    countLines = 0
    lines = ""

    DBAction ext045Query = database.table("EXT045").index("00").selection("EXITNO").build()
    DBContainer ext045Request = ext045Query.getContainer()
    ext045Request.set("EXCONO", currentCompany)
    ext045Request.set("EXCUNO", inCustomer)
    ext045Request.set("EXCDNN", inCalendar)

    // Retrieve EXT045
    Closure<?> ext045Reader = { DBContainer ext045Result ->
      sigma9 = ext045Result.get("EXITNO")
      libelleArticle = ""
      DBAction mitmasQuery = database.table("MITMAS").index("00").selection("MMFUDS").build()
      DBContainer mitmasRequest = mitmasQuery.getContainer()
      mitmasRequest.set("MMCONO", currentCompany)
      mitmasRequest.set("MMITNO", sigma9)
      if (mitmasQuery.read(mitmasRequest)) {
        libelleArticle = mitmasRequest.get("MMFUDS")
      }
      if (!lhcd.trim().equals("FR")) {
        DBAction mitladQuery = database.table("MITLAD").index("00").selection("MDFUDS").build()
        DBContainer mitladRequest = mitladQuery.getContainer()
        mitladRequest.set("MDCONO", currentCompany)
        mitladRequest.set("MDITNO", sigma9)
        mitladRequest.set("MDLNCD", "GB")
        if (mitladQuery.read(mitladRequest)) {
          libelleArticle = mitladRequest.get("MDFUDS")
        }
      }
      line = sigma9 + ";" + libelleArticle + ";" + "1"
      countLines++
      lines += line + (countLines < 5000 ? "\r\n" : "")
      if (countLines == 5000) {
        writeInFile("", lines)
        countLines = 0
        lines = ""
      }
      lineInAlert++
    }

    //Manage loop on ext045 records we can have max 50 000 records per customer
    boolean doLoop = true
    int nbIteration = 0
    while (doLoop) {
      if (nbIteration > 0) {
        PositionKey position = ext045Query.getPositionKey()
        if (position instanceof PositionEmpty) {
          doLoop = false
          break
        } else {
          ext045Query = database.table("EXT045")
            .index("00")
            .selection("EXITNO")
            .position(position)
            .build()
        }
      }
      if (!ext045Query.readAll(ext045Request, 3, 10000, ext045Reader)) {
      }
      nbIteration++
      if (nbIteration > 5) {//max 50 0000 records
        logMessage("ERROR", "Nombre d'itération trop important cuno:${inCustomer} cdnn:${inCalendar}")
        break
      }
    }
    if (countLines > 0) {
      writeInFile("", lines)
    }
    logMessage("INFO", "client:${inCustomer} alertes cadenciers:${inCalendar} nombre de lignes:${lineInAlert}")
  }
  // Write to price control file
  public void createPriceControlFile() {
    logFileName = fileJobNumber + "-" + inCustomer + "-" + inCalendar + "-" + "controlePrix.txt"
    header = "Numéro cadencier" + ";" + "Date création" + ";" + "Heure création" + ";" + "Code magasin" + ";" + "Nom magasin"
    writeInFile(header, "")
    line = inCalendar + ";" + creationDate + ";" + creationTime + ";" + inCustomer + ";" + customerName
    writeInFile("", line + "\r\n")
    header = "Code article" + ";" + "Nom article" + ";" + "Prix de vente" + ";" + "Prix de revient"
    writeInFile(header, "")
    countLines = 0
    lines = ""

    DBAction ext046Query = database.table("EXT046").index("00").selection("EXITNO", "EXSAPR", "EXAPPR").build()
    DBContainer ext046Request = ext046Query.getContainer()
    ext046Request.set("EXCONO", currentCompany)
    ext046Request.set("EXCUNO", inCustomer)
    ext046Request.set("EXCDNN", inCalendar)

    // Retrieve EXT046
    Closure<?> ext046Reader = { DBContainer ext046Result ->
      sigma9 = ext046Result.get("EXITNO")
      sapr = ext046Result.get("EXSAPR")
      appr = ext046Result.get("EXAPPR")
      libelleArticle = ""

      DBAction mitmasQuery = database.table("MITMAS").index("00").selection("MMFUDS").build()
      DBContainer mitmasRequest = mitmasQuery.getContainer()
      mitmasRequest.set("MMCONO", currentCompany)
      mitmasRequest.set("MMITNO", sigma9)
      if (mitmasQuery.read(mitmasRequest)) {
        libelleArticle = mitmasRequest.get("MMFUDS")
      }
      if (lhcd.trim() != "FR") {
        DBAction mitladQuery = database.table("MITLAD").index("00").selection("MDFUDS").build()
        DBContainer mitladRequest = mitladQuery.getContainer()
        mitladRequest.set("MDCONO", currentCompany)
        mitladRequest.set("MDITNO", sigma9)
        mitladRequest.set("MDLNCD", "GB")
        if (mitladQuery.read(mitladRequest)) {
          libelleArticle = mitladRequest.get("MDFUDS")
        }
        lineInControlPrice++
      }
      line = sigma9 + ";" + libelleArticle + ";" + (sapr as String) + ";" + (appr as String)
      lines += line + "\r\n"
      countLines++
      if (countLines == 5000) {
        writeInFile("", lines)
        countLines = 0
        lines = ""
      }
    }
    //Manage loop on ext046 records we can have max 50 000 records per customer
    boolean doLoop = true
    int nbIteration = 0
    while (doLoop) {
      if (nbIteration > 0) {
        PositionKey position = ext046Query.getPositionKey()
        if (position instanceof PositionEmpty) {
          doLoop = false
          break
        } else {
          ext046Query = database.table("EXT046")
            .index("00")
            .selection("EXITNO", "EXSAPR", "EXAPPR")
            .position(position)
            .build()
        }
      }
      if (!ext046Query.readAll(ext046Request, 3, 10000, ext046Reader)) {
      }
      nbIteration++
      if (nbIteration > 5) {//max 50 0000 records
        logMessage("ERROR", "Nombre d'itération trop important cuno:${inCustomer} cdnn:${inCalendar}")
        break
      }
    }

    if (countLines > 0) {
      writeInFile("", lines)
    }
    logMessage("INFO", "client:${inCustomer} controle prix cadencier:${inCalendar} nombre de lignes:${lineInControlPrice}")
  }

  /**
   *
   */
  public void createControlFile() {
    logFileName = fileJobNumber + "-" + inCustomer + "-" + inCalendar + "-" + "docNumber.xml"
    docnumber = fileJobNumber + "-" + inCustomer + "-" + inCalendar
    //todo changer le chemin serveur
    header = "<?xml version=\"1.0\" encoding=\"UTF-8\"?><Document>  <DocumentType>CADENCIER</DocumentType>  <DocumentNumber>${docnumber}</DocumentNumber>  <DocumentPath>\\\\XC006WKS\\CadencierClient\\</DocumentPath></Document>"
    writeInFile(header, "")
  }

  public void createControlFilesFtp() {
    String server  = getCRS881("", "EXTENC", "1", "ExtendM3", "I", "Generic", "Server", "", "", "TDTX40")
    String inpath  = getCRS881("", "EXTENC", "1", "ExtendM3", "I", "CadencierClient", "Path", "", "", "TDTX40")
    String outpath  = getCRS881("", "EXTENC", "1", "ExtendM3", "I", "CadencierClient", "OutPutPath", "", "", "TDTX40")


    //logFileName = fileJobNumber + "-" +inCustomer + "-" + inCalendar + "-" + "docNumber.xml"
    logFileName = fileJobNumber + "-" + inCustomer + "-" + inCalendar + "-" + "cadencier-docNumber.xml"
    //docnumber = fileJobNumber + "-" + inCustomer + "-" + inCalendar
    docnumber = fileJobNumber + "-" + inCustomer + "-" + inCalendar
    header = "<?xml version='1.0' encoding='UTF-8' standalone='no'?>"
    header += "<LoaddocumentNumber xmlns='http://schema.infor.com/InforOAGIS/2' xmlns:xsi='http://www.w3.org/2001/XMLSchema-instance' releaseID='9.2' versionID='2.14.6' xsi:schemaLocation='http://schema.infor.com/InforOAGIS/2 http://schema.infor.com/InforOAGIS/BODs/LoaddocumentNumber.xsd'>"
    header += "<ApplicationArea>"
    header += "<Sender>"
    header += "<LogicalID>casino.com</LogicalID>"
    header += "<ConfirmationCode>OnError</ConfirmationCode>"
    header += "</Sender>"
    header += "<CreationDateTime>2023-04-04</CreationDateTime>"
    header += "</ApplicationArea>"
    header += "<DataArea>"
    header += "<Load>"
    header += "<TenantID>CASINO_TST</TenantID>"
    header += "<AccountingEntityID>100_</AccountingEntityID>"
    header += "<ActionCriteria>"
    header += "<ActionExpression actionCode='' expressionLanguage=''/>"
    header += "</ActionCriteria>"
    header += "</Load>"
    header += "<documentNumber>"
    header += "<DocumentType>CADENCIER</DocumentType>"
    header += "<DocumentNumber>${docnumber}</DocumentNumber>"
    header += "<CFC1>${cfc1}</CFC1>"
    header += "<DocumentPath>\\\\${server}\\${inpath}</DocumentPath>"
    header += "<DocumentOutPath>\\\\${server}\\${outpath}</DocumentOutPath>"
    header += "</documentNumber>"
    header += "</DataArea>"
    header += "</LoaddocumentNumber>"
    writeInFile(header, "")
  }


  // Init line informations
  public void initLine() {
    ul = ""
    libelleArticle = ""
    dlgr = ""
    dlc = ""
    codeMarque = ""
    libelleCodeMarque = ""
    codeMarketing = ""
    libelleCodeMarketing = ""
    languePackaging = ""
    codeDepartement = ""
    codeRayon = ""
    codeFamille = ""
    codeSousFamille = ""
    uniteBesoin = ""
    nomDepartement = ""
    nomRayon = ""
    nomFamille = ""
    nomSousFamille = ""
    nomUniteBesoin = ""
    fournisseurOrigine = ""
    nomFournisseurOrigine = ""
    taille = ""
    couleur = ""
    modele = ""
    contenance = ""
    poidsNet = ""
    hauteurUVC = ""
    largeurUVC = ""
    longueurUVC = ""
    degreAlcool = ""
    temperature = ""
    typeBox = ""
    DBAction mitmasQuery = database.table("MITMAS").index("00").selection("MMECVE", "MMFUDS", "MMSPE2", "MMSPE1", "MMBUAR", "MMCFI1", "MMSPE3", "MMHIE1", "MMHIE2", "MMHIE3", "MMHIE4", "MMHIE5", "MMSUNO", "MMDIM1", "MMDIM2", "MMDIM3", "MMSPGV", "MMNEWE", "MMIHEI", "MMIWID", "MMILEN", "MMCFI2", "MMITCL", "MMCFI4").build()
    DBContainer mitmasRequest = mitmasQuery.getContainer()
    mitmasRequest.set("MMCONO", currentCompany)
    mitmasRequest.set("MMITNO", sigma9)

    if (mitmasQuery.read(mitmasRequest)) {
      ul = mitmasRequest.get("MMECVE")
      libelleArticle = mitmasRequest.get("MMFUDS")
      if (lhcd.trim() != "FR") {
        DBAction mitladQuery = database.table("MITLAD").index("00").selection("MDFUDS").build()
        DBContainer mitladRequest = mitladQuery.getContainer()
        mitladRequest.set("MDCONO", currentCompany)
        mitladRequest.set("MDITNO", sigma9)
        mitladRequest.set("MDLNCD", "GB")
        if (mitladQuery.read(mitladRequest)) {
          libelleArticle = mitladRequest.get("MDFUDS")
        }
      }
      spe2 = mitmasRequest.get("MMSPE2")

      if (spe2.trim() != "") {
        //
        dlgr = spe2
      }

      spe1 = mitmasRequest.get("MMSPE1")
      if (spe1.trim() != "") {
        //
        dlc = spe1
      }
      codeMarque = mitmasRequest.get("MMBUAR")
      if (codeMarque.trim() != "") {
        DBAction csytabBuarQuery = database.table("CSYTAB").index("00").selection("CTTX40").build()
        DBContainer csytabBuarRequest = csytabBuarQuery.getContainer()
        csytabBuarRequest.set("CTCONO", currentCompany)
        csytabBuarRequest.set("CTSTCO", "BUAR")
        csytabBuarRequest.set("CTSTKY", codeMarque)
        if (csytabBuarQuery.read(csytabBuarRequest)) {
          libelleCodeMarque = csytabBuarRequest.get("CTTX40")
        }
      }
      codeMarketing = mitmasRequest.get("MMCFI1")
      if (codeMarketing.trim() != "") {
        DBAction csytabCfi1Query = database.table("CSYTAB").index("00").selection("CTTX40").build()
        DBContainer csytabCfi1Request = csytabCfi1Query.getContainer()
        csytabCfi1Request.set("CTCONO", currentCompany)
        csytabCfi1Request.set("CTSTCO", "CFI1")
        csytabCfi1Request.set("CTSTKY", codeMarketing)
        if (csytabCfi1Query.read(csytabCfi1Request)) {
          libelleCodeMarketing = csytabCfi1Request.get("CTTX40")
        }
      }

      languePackaging = mitmasRequest.get("MMSPE3")

      codeDepartement = mitmasRequest.get("MMHIE1")
      DBAction mithryQuery = database.table("MITHRY").index("00").selection("HITX40").build()
      DBContainer mithryRequest = mithryQuery.getContainer()
      mithryRequest.set("HICONO", currentCompany)
      mithryRequest.set("HIHLVL", 1)
      mithryRequest.set("HIHIE0", codeDepartement)
      if (mithryQuery.read(mithryRequest)) {
        nomDepartement = mithryRequest.get("HITX40")
      }
      hie2 = mitmasRequest.get("MMHIE2")
      if (hie2.trim() != "")
        codeRayon = (hie2 as String).substring((hie2.trim().length() - 2), hie2.trim().length())
      mithryRequest.set("HIHLVL", 2)
      mithryRequest.set("HIHIE0", hie2)
      if (mithryQuery.read(mithryRequest)) {
        nomRayon = mithryRequest.get("HITX40")
      }
      hie3 = mitmasRequest.get("MMHIE3")
      if (hie3.trim() != "")
        codeFamille = (hie3 as String).substring((hie3.trim().length() - 3), hie3.trim().length())
      mithryRequest.set("HIHLVL", 3)
      mithryRequest.set("HIHIE0", hie3)
      if (mithryQuery.read(mithryRequest)) {
        nomFamille = mithryRequest.get("HITX40")
      }

      hie4 = mitmasRequest.get("MMHIE4")
      if (hie4.trim() != "")
        codeSousFamille = (hie4 as String).substring((hie4.trim().length() - 2), hie4.trim().length())
      mithryRequest.set("HIHLVL", 4)
      mithryRequest.set("HIHIE0", hie4)
      if (mithryQuery.read(mithryRequest)) {
        nomSousFamille = mithryRequest.get("HITX40")
      }

      hie5 = mitmasRequest.get("MMHIE5")
      if (hie5.trim() != "")
        uniteBesoin = (hie5 as String).substring((hie5.trim().length() - 2), hie5.trim().length())
      mithryRequest.set("HIHLVL", 5)
      mithryRequest.set("HIHIE0", hie5)
      if (mithryQuery.read(mithryRequest)) {
        nomUniteBesoin = mithryRequest.get("HITX40")
        nomUniteBesoin = nomUniteBesoin.replaceAll(/[<>&"\\]/, " ")
      }
      fournisseurOrigine = mitmasRequest.get("MMSUNO")
      DBAction cidmasQuery = database.table("CIDMAS").index("00").selection("IDSUNM").build()
      DBContainer cidmasRequest = cidmasQuery.getContainer()
      cidmasRequest.set("IDCONO", currentCompany)
      cidmasRequest.set("IDSUNO", fournisseurOrigine)
      if (cidmasQuery.read(cidmasRequest)) {
        nomFournisseurOrigine = cidmasRequest.getString("IDSUNM")
      }

      taille = mitmasRequest.get("MMDIM2")
      couleur = mitmasRequest.get("MMDIM1")
      modele = mitmasRequest.get("MMDIM3")
      contenance = mitmasRequest.get("MMSPGV")
      poidsNet = mitmasRequest.get("MMNEWE")
      hauteurUVC = mitmasRequest.get("MMIHEI")
      largeurUVC = mitmasRequest.get("MMIWID")
      longueurUVC = mitmasRequest.get("MMILEN")
      degreAlcool = mitmasRequest.get("MMCFI2")
      temperature = mitmasRequest.get("MMITCL")
      typeBox = mitmasRequest.get("MMDIM3")
      String cfi4 = mitmasRequest.get("MMCFI4") as String
      isAlcool = cfi4.trim().length() > 0
    }

    typeAppro = ""
    refDroit = ""
    String a830 = ""
    int CHB1 = 0
    DBAction cugex1MitmasQuery = database.table("CUGEX1").index("00").selection("F1A330", "F1A830", "F1CHB1").build()
    DBContainer cugex1MitmasRequest = cugex1MitmasQuery.getContainer()
    cugex1MitmasRequest.set("F1CONO", currentCompany)
    cugex1MitmasRequest.set("F1FILE", "MITMAS")
    cugex1MitmasRequest.set("F1PK01", sigma9)
    cugex1MitmasRequest.set("F1PK02", "")
    cugex1MitmasRequest.set("F1PK03", "")
    cugex1MitmasRequest.set("F1PK04", "")
    cugex1MitmasRequest.set("F1PK05", "")
    cugex1MitmasRequest.set("F1PK06", "")
    cugex1MitmasRequest.set("F1PK07", "")
    cugex1MitmasRequest.set("F1PK08", "")
    if (cugex1MitmasQuery.read(cugex1MitmasRequest)) {
      a830 = cugex1MitmasRequest.get("F1A830")

      if (a830.trim() == "30" || a830.trim() == "40")
        typeAppro = "E"
      if (a830.trim() == "20")
        typeAppro = "D"
      if (a830.trim() == "10")
        typeAppro = "S"

      refDroit = cugex1MitmasRequest.get("F1A330")
    }

    codeArticleRemplace = ""
    libelleArticleRemplace = ""
    DBAction mitaltQuery = database.table("MITALT").index("40").selection("MAITNO", "MASTDT").build()
    DBContainer mitaltRequest = mitaltQuery.getContainer()
    mitaltRequest.set("MACONO", currentCompany)
    mitaltRequest.set("MAALIT", sigma9)
    mitaltRequest.set("MARPTY", 3)

    // Retrieve MITALT
    Closure<?> mitaltReader = { DBContainer mitaltResult ->
      int stdt = mitaltResult.get("MASTDT")
      if (stdt <= currentDate) {
        codeArticleRemplace = mitaltResult.get("MAITNO")
        DBAction mitmasQuery2 = database.table("MITMAS").index("00").selection("MMFUDS").build()
        DBContainer mitmasRequest2 = mitmasQuery2.getContainer()
        mitmasRequest2.set("MMCONO", currentCompany)
        mitmasRequest2.set("MMITNO", codeArticleRemplace)
        if (mitmasQuery2.read(mitmasRequest2)) {
          libelleArticleRemplace = mitmasRequest2.get("MMFUDS")
        }
        if (lhcd.trim() != "FR") {
          DBAction mitladQuery = database.table("MITLAD").index("00").selection("MDFUDS").build()
          DBContainer mitlodRequest = mitladQuery.getContainer()
          mitlodRequest.set("MDCONO", currentCompany)
          mitlodRequest.set("MDITNO", codeArticleRemplace)
          mitlodRequest.set("MDLNCD", "GB")
          if (mitladQuery.read(mitlodRequest)) {
            libelleArticleRemplace = mitlodRequest.get("MDFUDS")
          }
        }
      }
    }
    if (mitaltQuery.readAll(mitaltRequest, 3, 10000, mitaltReader)) {
    }
    poidsBrut = ""
    volumeColis = ""
    hauteurColis = ""
    longueurColis = ""
    largeurColis = ""
    DBAction cugex1MitaunQuery = database.table("CUGEX1").index("00").selection("F1N096", "F1N196", "F1N296", "F1N396", "F1N496").build()
    DBContainer cugex1MitaunRequest = cugex1MitaunQuery.getContainer()
    cugex1MitaunRequest.set("F1CONO", currentCompany)
    cugex1MitaunRequest.set("F1FILE", "MITAUN")
    cugex1MitaunRequest.set("F1PK01", sigma9)
    cugex1MitaunRequest.set("F1PK02", "2")
    cugex1MitaunRequest.set("F1PK03", "COL")
    cugex1MitaunRequest.set("F1PK04", "")
    cugex1MitaunRequest.set("F1PK05", "")
    cugex1MitaunRequest.set("F1PK06", "")
    cugex1MitaunRequest.set("F1PK07", "")
    cugex1MitaunRequest.set("F1PK08", "")
    if (cugex1MitaunQuery.read(cugex1MitaunRequest)) {
      poidsBrut = cugex1MitaunRequest.get("F1N096")
      volumeColis = cugex1MitaunRequest.get("F1N196")
      hauteurColis = cugex1MitaunRequest.get("F1N296")
      largeurColis = cugex1MitaunRequest.get("F1N496")
      longueurColis = cugex1MitaunRequest.get("F1N396")
    }
    parCombien = ""
    nbCouchePalette = 0
    nbColisParCouche = 0
    nbColisParPalette = 0
    nbUVCParPalette = 0

    double cofaCOL = 0
    DBAction mitaunQuery = database.table("MITAUN").index("00").selection("MUCOFA").build()
    DBContainer mitaunRequest = mitaunQuery.getContainer()
    mitaunRequest.set("MUCONO", currentCompany)
    mitaunRequest.set("MUITNO", sigma9)
    mitaunRequest.set("MUAUTP", 1)
    mitaunRequest.set("MUALUN", "COL")
    if (mitaunQuery.read(mitaunRequest)) {
      cofaCOL = mitaunRequest.get("MUCOFA")
    }

    double cofaUCO = 0
    mitaunRequest.set("MUAUTP", 1)
    mitaunRequest.set("MUALUN", "UCO")
    if (mitaunQuery.read(mitaunRequest)) {
      cofaUCO = mitaunRequest.get("MUCOFA")
    }

    double cofaUPA = 0
    mitaunRequest.set("MUAUTP", 1)
    mitaunRequest.set("MUALUN", "UPA")
    if (mitaunQuery.read(mitaunRequest)) {
      cofaUPA = mitaunRequest.get("MUCOFA")
    }

    parCombien = cofaCOL as String
    if (cofaUCO != 0)
      nbCouchePalette = cofaUPA / cofaUCO
    if (cofaCOL != 0) {
      nbColisParCouche = cofaUCO / cofaCOL
      nbColisParPalette = cofaUPA / cofaCOL
    }
    nbUVCParPalette = cofaUPA


    popn = ""
    ean13 = ""
    ExpressionFactory mitpopExpression = database.getExpressionFactory("MITPOP")
    mitpopExpression = mitpopExpression.eq("MPREMK", "EA13")
    DBAction mitpopQuery = database.table("MITPOP").index("30").matching(mitpopExpression).selection("MPPOPN").build()
    DBContainer mitpopRequest = mitpopQuery.getContainer()
    mitpopRequest.set("MPCONO", currentCompany)
    mitpopRequest.set("MPALWT", 1)
    mitpopRequest.set("MPITNO", sigma9)
    if (mitpopQuery.readAll(mitpopRequest, 3, 1, mitPopReader)) {
      ean13 = popn
    }

    popn = ""
    fba = ""
    ExpressionFactory mitpopExpression2 = database.getExpressionFactory("MITPOP")
    mitpopExpression2 = mitpopExpression2.eq("MPREMK", "FBA")
    DBAction mitpopQuery2 = database.table("MITPOP").index("30").matching(mitpopExpression2).selection("MPPOPN").build()
    DBContainer mitpopRequest2 = mitpopQuery2.getContainer()
    mitpopRequest2.set("MPCONO", currentCompany)
    mitpopRequest2.set("MPALWT", 1)
    mitpopRequest2.set("MPITNO", sigma9)
    if (mitpopQuery2.readAll(mitpopRequest2, 3, 1, mitPopReader)) {
      fba = popn
    }

    popn = ""
    dun14 = ""
    ExpressionFactory mitpopExpression3 = database.getExpressionFactory("MITPOP")
    mitpopExpression3 = mitpopExpression3.eq("MPREMK", "DUN14")
    DBAction mitpopQuery3 = database.table("MITPOP").index("30").matching(mitpopExpression3).selection("MPPOPN").build()
    DBContainer mitpopRequest3 = mitpopQuery.getContainer()
    mitpopRequest3.set("MPCONO", currentCompany)
    mitpopRequest3.set("MPALWT", 1)
    mitpopRequest3.set("MPITNO", sigma9)
    if (mitpopQuery3.readAll(mitpopRequest3, 3, 1, mitPopReader)) {
      dun14 = popn
    }

    paysOrigine = ""
    nomPaysOrigine = ""
    codeDouanier = ""
    LocalDateTime timeOfCreation = LocalDateTime.now()
    DBAction mitfacQuery = database.table("MITFAC").index("00").selection("M9ORCO", "M9CSNO").build()
    DBContainer mitfacRequest = mitfacQuery.getContainer()
    mitfacRequest.set("M9CONO", currentCompany)
    mitfacRequest.set("M9FACI", "E10")
    mitfacRequest.set("M9ITNO", sigma9)
    if (mitfacQuery.read(mitfacRequest)) {
      paysOrigine = mitfacRequest.get("M9ORCO")
      if (paysOrigine.trim() != "") {
        DBAction csytabCscdQuery = database.table("CSYTAB").index("00").selection("CTTX40").build()
        DBContainer csytabRequest = csytabCscdQuery.getContainer()
        csytabRequest.set("CTCONO", currentCompany)
        csytabRequest.set("CTSTCO", "CSCD")
        csytabRequest.set("CTSTKY", paysOrigine)
        if (csytabCscdQuery.read(csytabRequest)) {
          nomPaysOrigine = csytabRequest.get("CTTX40")
        }
      }
      codeDouanier = mitfacRequest.get("M9CSNO")
    }

    nombreComposantBox = ""
    sumCNQT = 0
    DBAction mpdmatQuery = database.table("MPDMAT").index("00").selection("PMCNQT").build()
    DBContainer mpdmatRequest = mpdmatQuery.getContainer()
    mpdmatRequest.set("PMCONO", currentCompany)
    mpdmatRequest.set("PMFACI", "E10")
    mpdmatRequest.set("PMPRNO", sigma9)
    mpdmatRequest.set("PMSTRT", "KIT")

    // Retrieve MPDMAT
    Closure<?> mpdmatResult = { DBContainer mpdmatResult ->
      sumCNQT += mpdmatResult.get("PMCNQT") as double
    }

    if (mpdmatQuery.readAll(mpdmatRequest, 4, 10000, mpdmatResult)) {
    }
    nombreComposantBox = sumCNQT

    fournisseur = ""
    nomFournisseur = ""
    entrepot = ""
    rscl = ""
    assortimentLogistique = ""
    saisonnalite = ""
    diffusion = ""
    ExpressionFactory ext010Expression = database.getExpressionFactory("EXT010")
    ext010Expression = ext010Expression.le("EXFVDT", currentDate as String).and(ext010Expression.ge("EXLVDT", currentDate as String))
    DBAction ext010Query = database.table("EXT010").index("02").matching(ext010Expression).selection("EXSULE", "EXSULD", "EXRSCL", "EXASGD").build()
    DBContainer ext010Request = ext010Query.getContainer()
    ext010Request.set("EXCONO", currentCompany)
    ext010Request.set("EXCUNO", inCustomer)
    ext010Request.set("EXITNO", sigma9)
    if (ext010Query.readAll(ext010Request, 3, 1, ext010Reader3)) {
    }


    if (rscl.trim() == "999" ||
      rscl.trim() == "") {
      saisonnalite = "Permanent"
    }
    if (rscl.trim() == "998") {
      saisonnalite = "Saisonnalite"
    }

    if (rscl.trim().length() == 2) {
      try {
        int xxrscl = Integer.parseInt(rscl.trim())
        diffusion = "" + rscl
        saisonnalite = "Permanent"
      } catch (NumberFormatException e) {
      }
    }

    prixVente = "0"
    unitePrixVente = ""
    savedFromValidDate = 0
    currentFromValidDate = 0
    ExpressionFactory oprbasExpression = database.getExpressionFactory("OPRBAS")
    oprbasExpression = oprbasExpression.le("ODVFDT", currentDate as String).and(oprbasExpression.ge("ODLVDT", currentDate as String))
    oprbasExpression = oprbasExpression.and(oprbasExpression.eq("ODITNO", sigma9))
    oprbasExpression = oprbasExpression.and(oprbasExpression.eq("ODCUNO", inCustomer))
    DBAction oprbasQuery = database.table("OPRBAS").index("00").matching(oprbasExpression).selection("ODVFDT", "ODSAPR", "ODSPUN").build()
    DBContainer oprbasRequest = oprbasQuery.getContainer()
    oprbasRequest.set("ODCONO", currentCompany)
    oprbasRequest.set("ODPRRF", prrf)
    oprbasQuery.readAll(oprbasRequest, 2, 10000, oprbasReader)

    minimumCommande = ""
    DBAction ocusitQuery = database.table("OCUSIT").index("00").selection("ORD2QT").build()
    DBContainer ocusitRequest = ocusitQuery.getContainer()
    ocusitRequest.set("ORCONO", currentCompany)
    ocusitRequest.set("ORCUNO", "")
    ocusitRequest.set("ORITNO", sigma9)
    if (ocusitQuery.read(ocusitRequest)) {
      minimumCommande = ocusitRequest.get("ORD2QT")
    }

    nouveaute = ""
    prixCadencierPrecedent = "0"
    evolutionPrixCadencierPrecedent = ""

    DBAction ext040Query = database.table("EXT040").index("00").selection("EXSAPR").build()
    DBContainer ext040Reader = ext040Query.getContainer()
    ext040Reader.set("EXCONO", currentCompany)
    ext040Reader.set("EXCUNO", inCustomer)
    ext040Reader.set("EXCDNN", previousCalendar)
    ext040Reader.set("EXPOPN", sigma6)
    ext040Reader.set("EXITNO", sigma9)
    if (!ext040Query.read(ext040Reader)) {
      nouveaute = "N"
    } else {
      prixCadencierPrecedent = ext040Reader.get("EXSAPR") as String
    }

    if (prixVente != "0" && prixCadencierPrecedent != "0")
      evolutionPrixCadencierPrecedent = ((prixVente as double) - (prixCadencierPrecedent as double)) / ((prixCadencierPrecedent as double)) * 100

    if (prixCadencierPrecedent == "0" && prixVente != "0") {
      evolutionPrixCadencierPrecedent = "999"
    }
    if (prixVente == "0" && prixCadencierPrecedent != "0") {
      evolutionPrixCadencierPrecedent = "-100"
    }
  }


  // Execute EXT820MI.SubmitBatch
  private executeEXT820MISubmitBatch(String CONO, String JOID, String USID, String P001, String P002, String P003) {
    def parameters = ["CONO": CONO, "JOID": JOID, "USID": USID, "P001": P001, "P002": P002, "P003": P003, "EXVSN": "1"]
    Closure<?> handler = { Map<String, String> response ->

      if (response.error != null) {
        return response.error
      }
    }
    miCaller.call("EXT820MI", "SubmitBatch", parameters, handler)
  }
  // Retrieve OPRBAS
  Closure<?> oprbasReader = { DBContainer oprbasResult ->
    currentFromValidDate = oprbasResult.get("ODVFDT")
    if (currentFromValidDate > savedFromValidDate) {
      savedFromValidDate = oprbasResult.get("ODVFDT")
      prixVente = oprbasResult.get("ODSAPR")
      unitePrixVente = oprbasResult.get("ODSPUN")
    }
  }

  // Retrieve MITPOP
  Closure<?> mitPopReader = { DBContainer mitpopResult ->
    popn = mitpopResult.get("MPPOPN")
  }

  public void deleteTemporaryFiles() {
    // Delete EXT040  records for previous calendar
    ExpressionFactory ext040Expression = database.getExpressionFactory("EXT040")
    if (!isMainAssortment)
      ext040Expression = ext040Expression.ne("EXCDNN", previousCalendar)
    if (isMainAssortment)
      ext040Expression = ext040Expression.ne("EXCDNN", inCalendar).and(ext040Expression.ne("EXCDNN", previousCalendar))

    DBAction ext040Query = database.table("EXT040").index("00").matching(ext040Expression).build()
    DBContainer ext040Request = ext040Query.getContainer()
    ext040Request.set("EXCONO", currentCompany)
    ext040Request.set("EXCUNO", inCustomer)

    Closure<?> ext040MassDeleter = { DBContainer ext040Result ->
      // Delete EXT040
      Closure<?> ext040Updater = { LockedResult ext040lockedResult ->
        ext040lockedResult.delete()
      }
      ext040Query.readLock(ext040Result, ext040Updater)
    }

    //Manage loop on ext040 records we can have max 50 000 records per customer
    boolean doLoop = true
    int nbIteration = 0
    while (doLoop) {
      if (nbIteration > 0) {
        PositionKey position = ext040Query.getPositionKey()
        if (position instanceof PositionEmpty) {
          doLoop = false
          break
        } else {
          ext040Query = database.table("EXT040")
            .index("00")
            .matching(ext040Expression)
            .position(position)
            .build()
        }
      }
      if (!ext040Query.readAll(ext040Request, 2, 10000, ext040MassDeleter)) {
      }
      nbIteration++
      if (nbIteration > 5) {//max 50 0000 records
        logMessage("ERROR", "Nombre d'itération trop important cuno:${inCustomer} cdnn:${inCalendar}")
        break
      }
    }

    // Delete EXT041  records for previous calendar
    ExpressionFactory ext041Expression = database.getExpressionFactory("EXT041")
    if (!isMainAssortment)
      ext041Expression = ext041Expression.ne("EXCDNN", previousCalendar)
    if (isMainAssortment)
      ext041Expression = ext041Expression.ne("EXCDNN", inCalendar).and(ext041Expression.ne("EXCDNN", previousCalendar))

    DBAction ext041Query = database.table("EXT041").index("00").matching(ext041Expression).build()
    DBContainer ext041Request = ext041Query.getContainer()
    ext041Request.set("EXCONO", currentCompany)
    ext041Request.set("EXCUNO", inCustomer)

    Closure<?> ext041MassDeleter = { DBContainer ext041Result ->
      // Delete ext041
      Closure<?> ext041Updater = { LockedResult ext041lockedResult ->
        ext041lockedResult.delete()
      }
      ext041Query.readLock(ext041Result, ext041Updater)
    }

    //Manage loop on ext041 records we can have max 50 000 records per customer
    doLoop = true
    nbIteration = 0
    while (doLoop) {
      if (nbIteration > 0) {
        PositionKey position = ext041Query.getPositionKey()
        if (position instanceof PositionEmpty) {
          doLoop = false
          break
        } else {
          ext041Query = database.table("EXT041")
            .index("00")
            .matching(ext041Expression)
            .position(position)
            .build()
        }
      }
      if (!ext041Query.readAll(ext041Request, 2, 10000, ext041MassDeleter)) {
      }
      nbIteration++
      if (nbIteration > 5) {//max 50 0000 records
        logMessage("ERROR", "Nombre d'itération trop important cuno:${inCustomer} cdnn:${inCalendar}")
        break
      }
    }

    //DELETE EXT042 Records for previous calendar
    ExpressionFactory ext042Expression = database.getExpressionFactory("EXT042")
    if (!isMainAssortment)
      ext042Expression = ext042Expression.ne("EXCDNN", previousCalendar)
    if (isMainAssortment)
      ext042Expression = ext042Expression.ne("EXCDNN", inCalendar).and(ext042Expression.ne("EXCDNN", previousCalendar))
    DBAction ext042Query = database.table("EXT042").index("00").matching(ext042Expression).build()
    DBContainer ext042Request = ext042Query.getContainer()
    ext042Request.set("EXCONO", currentCompany)
    ext042Request.set("EXCUNO", inCustomer)

    Closure<?> ext042MassDeleter = { DBContainer ext042Result ->
      // Delete ext041
      Closure<?> ext042Updater = { LockedResult ext042lockedResult ->
        ext042lockedResult.delete()
      }
      ext042Query.readLock(ext042Result, ext042Updater)
    }
    if (!ext042Query.readAll(ext042Request, 2, 10000, ext042MassDeleter)) {
    }

    //DELETE EXT043 Records for previous calendar
    ExpressionFactory ext043Expression = database.getExpressionFactory("EXT043")
    if (!isMainAssortment)
      ext043Expression = ext043Expression.ne("EXCDNN", previousCalendar)
    if (isMainAssortment)
      ext043Expression = ext043Expression.ne("EXCDNN", inCalendar).and(ext043Expression.ne("EXCDNN", previousCalendar))
    DBAction ext043Query = database.table("EXT043").index("00").matching(ext043Expression).build()
    DBContainer ext043Request = ext043Query.getContainer()
    ext043Request.set("EXCONO", currentCompany)
    ext043Request.set("EXCUNO", inCustomer)

    Closure<?> ext043MassDeleter = { DBContainer ext043Result ->
      // Delete ext041
      Closure<?> ext043Updater = { LockedResult ext043lockedResult ->
        ext043lockedResult.delete()
      }
      ext043Query.readLock(ext043Result, ext043Updater)
    }
    if (!ext043Query.readAll(ext043Request, 2, 10000, ext043MassDeleter)) {
    }

    //DELETE EXT044 Records for previous calendar
    ExpressionFactory ext044Expression = database.getExpressionFactory("EXT044")
    if (!isMainAssortment)
      ext044Expression = ext044Expression.ne("EXCDNN", previousCalendar)
    if (isMainAssortment)
      ext044Expression = ext044Expression.ne("EXCDNN", inCalendar).and(ext044Expression.ne("EXCDNN", previousCalendar))
    DBAction ext044Query = database.table("EXT044").index("00").matching(ext044Expression).build()
    DBContainer ext044Request = ext044Query.getContainer()
    ext044Request.set("EXCONO", currentCompany)
    ext044Request.set("EXCUNO", inCustomer)

    Closure<?> ext044MassDeleter = { DBContainer ext044Result ->
      // Delete ext041
      Closure<?> ext044Updater = { LockedResult ext044lockedResult ->
        ext044lockedResult.delete()
      }
      ext044Query.readLock(ext044Result, ext044Updater)
    }
    if (!ext044Query.readAll(ext044Request, 2, 10000, ext044MassDeleter)) {
    }

    //DELETE EXT045 Records for previous calendar
    ExpressionFactory ext045Expression = database.getExpressionFactory("EXT045")
    if (!isMainAssortment)
      ext045Expression = ext045Expression.ne("EXCDNN", previousCalendar)
    if (isMainAssortment)
      ext045Expression = ext045Expression.ne("EXCDNN", inCalendar).and(ext045Expression.ne("EXCDNN", previousCalendar))
    DBAction ext045Query = database.table("EXT045").index("00").matching(ext045Expression).build()
    DBContainer ext045Request = ext045Query.getContainer()
    ext045Request.set("EXCONO", currentCompany)
    ext045Request.set("EXCUNO", inCustomer)

    Closure<?> ext045MassDeleter = { DBContainer ext045Result ->
      // Delete ext041
      Closure<?> ext045Updater = { LockedResult ext045lockedResult ->
        ext045lockedResult.delete()
      }
      ext045Query.readLock(ext045Result, ext045Updater)
    }
    if (!ext045Query.readAll(ext045Request, 2, 10000, ext045MassDeleter)) {
    }

    //DELETE EXT046 Records for previous calendar
    ExpressionFactory ext046Expression = database.getExpressionFactory("EXT046")
    if (!isMainAssortment)
      ext046Expression = ext046Expression.ne("EXCDNN", previousCalendar)
    if (isMainAssortment)
      ext046Expression = ext046Expression.ne("EXCDNN", inCalendar).and(ext046Expression.ne("EXCDNN", previousCalendar))
    DBAction ext046Query = database.table("EXT046").index("00").matching(ext046Expression).build()
    DBContainer ext046Request = ext046Query.getContainer()
    ext046Request.set("EXCONO", currentCompany)
    ext046Request.set("EXCUNO", inCustomer)

    Closure<?> ext046MassDeleter = { DBContainer ext046Result ->
      // Delete ext041
      Closure<?> ext046Updater = { LockedResult ext046lockedResult ->
        ext046lockedResult.delete()
      }
      ext046Query.readLock(ext046Result, ext046Updater)
    }
    if (!ext046Query.readAll(ext046Request, 2, 10000, ext046MassDeleter)) {
    }
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
    String parameter = ""
    beginIndex = endIndex + 1
    endIndex = rawDataLength - rawData.indexOf(";") - 1
    rawData = rawData.substring(beginIndex, rawDataLength)
    rawDataLength = rawData.length()
    beginIndex = 0
    endIndex = rawData.indexOf(";")
    // Get parameter
    parameter = rawData.substring(beginIndex, endIndex)
    return parameter
  }
  // Delete records related to the current job from EXTJOB table
  public void deleteEXTJOB() {
    LocalDateTime timeOfCreation = LocalDateTime.now()
    DBAction query = database.table("EXTJOB").index("00").build()
    DBContainer EXTJOB = query.getContainer()
    EXTJOB.set("EXRFID", batch.getReferenceId().get())
    // Delete EXTJOB
    Closure<?> updateCallBack_EXTJOB = { LockedResult lockedResult ->
      lockedResult.delete()
    }
    if (!query.readLock(EXTJOB, updateCallBack_EXTJOB)) {//todo check readll
    }
  }
  // Log message
  void writeInFile(String header, String line) {
    textFiles.open("FileImport/CadencierClient")
    if (header.trim() != "") {
      writeFile(header + "\r\n")
    }
    if (line.trim() != "") {
      writeFile(line)
    }
  }
  // Log
  void writeFile(String message) {
    IN60 = true
    //message = LocalDateTime.now().toString() + ";" + message
    Closure<?> consumer = { PrintWriter printWriter ->
      printWriter.print(message)
    }
    if (logFileName.endsWith("docNumber.xml")) {
      textFiles.write(logFileName, "UTF-8", false, consumer)
    } else {
      textFiles.write(logFileName, "UTF-8", true, consumer)
    }
  }

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
  /**
   * Initialize log management
   */
  private void initializeLogManagement() {
    logfile = program.getProgramName() + "." + "batch" + "." + jobNumber + ".log"
    logmessages = new LinkedList<String>()
    loglevel = getCRS881("", "EXTENC", "1", "ExtendM3", "I", program.getProgramName(), "LOGLEVEL", "", "", "TDTX15")
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
  private String getCRS881(String division, String mstd, String mvrs, String bmsg, String ibob, String elmp, String elmd, String elmc, String mbmc, String field) {
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
      DBAction queryMbmtrd = database.table("MBMTRD").index("00")
        .selection("TDMVXD", "TDTX15", "TDTX40")
        .build()
      DBContainer requestMbmtrd = queryMbmtrd.getContainer()
      requestMbmtrd.set("TDCONO", currentCompany)
      requestMbmtrd.set("TDDIVI", division)
      requestMbmtrd.set("TDIDTR", requestMbmtrn.get("TRIDTR"))
      // Retrieve MBTRND
      Closure<?> readerMbmtrd = { DBContainer resultMbmtrd ->
        mvxd = resultMbmtrd.get(field) as String
        mvxd = mvxd.trim()
      }
      if (queryMbmtrd.readAll(requestMbmtrd, 3, 1, readerMbmtrd)) {
      }
      return mvxd
    }
  }
}
