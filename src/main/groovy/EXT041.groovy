/****************************************************************************************
 Extension Name: EXT041
 Type: ExtendM3Transaction
 Script Author: PBEAUDOUIN
 Date: 2024-03-25
 Description:
 * Generates the ECOM_PRD_STO file

 Revision History:
 Name          Date        Version  Description of Changes
 PBEAUDOUIN    2024-03-25  1.0      INITIAL
 RENARN        2025-05-12  1.1      Taking into account INFOR standards
 RENARN        2025-05-13  1.1.2    Reading loops have been reduced
 FLEBARS       2025-05-28  1.1.4    Add expiration date in main method
 FLEBARS       2025-05-28  1.2      Delete EXTJOB record
 ******************************************************************************************/


import java.time.LocalDate
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import mvx.db.common.PositionKey
import mvx.db.common.PositionEmpty

public class EXT041 extends ExtendM3Batch {
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
  private String currentDateTime
  private Integer currentDate
  private String docnumber
  private String inCustomer
  private String inCalendar
  private Integer inAllContacts
  private Integer inSchedule
  private String sigma6
  private String sigma9
  private String sigma9DirectDelivery
  private String sigma9NoDirectDelivery
  private String sigma9DirectDeliveryAssortment
  private String sigma9NoDirectDeliveryAssortment
  private int chb7
  private String customerName
  private String creationDate
  private String creationTime
  private String header
  private String line
  private String lines
  private String assortment
  private int countLines
  private int countLineCancellationNo
  private int countLineCancellationYes
  private double currentCofa
  private double savedCofa
  private String UL
  private String libelleArticle
  private String prixVente
  private Integer savedFromValidDate
  private Integer currentFromValidDate
  private String unitePrixVente
  private String saisonnalite
  private String supplierCode
  private String vacode
  private String entrepot
  private String diffusion
  private String parCombien
  private String nbCouchePalette
  private String nbColisParCouche
  private String nbColisParPalette
  private String nbUVCParPalette
  private String rscl
  private String assortimentLogistique
  private String pltb
  private String lhcd
  private String fileJobNumber
  private String jobNumber
  private String previousCalendar
  private boolean isReedition
  private boolean foundSigma6
  private String referenceId

  public EXT041(LoggerAPI logger, DatabaseAPI database, ProgramAPI program, BatchAPI batch, MICallerAPI miCaller, TextFilesAPI textFiles, UtilityAPI utility) {
    this.logger = logger
    this.database = database
    this.program = program
    this.batch = batch
    this.miCaller = miCaller
    this.textFiles = textFiles
    this.utility = utility
  }

  public void main() {
    //Expiration Date for data correction extension
    if (LocalDate.now().isAfter(LocalDate.of(2025, 11, 30))) {
      logger.debug("Extension signature expired")
      return
    }

    currentCompany = (Integer) program.getLDAZD().CONO
    // Get job number
    LocalDateTime timeOfCreation = LocalDateTime.now()
    currentDateTime = timeOfCreation.format(DateTimeFormatter.ofPattern("yyyy-MM-dd")) + "T" + timeOfCreation.format(DateTimeFormatter.ofPattern("HH:mm:ss"))
    fileJobNumber = program.getJobNumber()
    jobNumber = program.getJobNumber() + timeOfCreation.format(DateTimeFormatter.ofPattern("yyMMdd")) + timeOfCreation.format(DateTimeFormatter.ofPattern("HHmmss"))

    //log management
    initializeLogManagement()

    logger.debug("fileJobNumber=${fileJobNumber}")
    if (batch.getReferenceId().isPresent()) {
      referenceId = batch.referenceId
      referenceId = referenceId.substring(9, 45)
      Optional<String> data = getJobData(batch.getReferenceId().get())
      performActualJob(data)
    } else {
      writeInFile("ERROR", "Job data for job ${batch.getJobId()} is missing")
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
    LocalDateTime timeOfCreation = LocalDateTime.now()
    currentDate = timeOfCreation.format(DateTimeFormatter.ofPattern("yyyyMMdd")) as Integer
    creationDate = timeOfCreation.format(DateTimeFormatter.ofPattern("yyyyMMdd")) as String
    creationTime = timeOfCreation.format(DateTimeFormatter.ofPattern("HHmmss")) as String

    logMessage("INFO", "Start catalogue B2B for customer:${inCustomer} calendar:${inCalendar} allContacts:${inAllContacts} schedule:${inSchedule}")
    createEcoPrdStoFile()
    writeGapLine()
    writeEndFile()
    deleteEXTJOB()
  }


  // Retrieve EXT010
  Closure<?> ext010Reader = { DBContainer ext010Result ->
    String suno = ""
    String sule = ext010Result.get("EXSULE")
    String suld = ext010Result.get("EXSULD")
    if (sule.trim() != "") {
      suno = sule
    } else {
      suno = suld
    }
    if (suno.trim() != "") {
      DBAction cidmasQuery = database.table("CIDMAS").index("00").selection("IDSUCO").build()
      DBContainer cidmasRequest = cidmasQuery.getContainer()
      cidmasRequest.set("IDCONO", currentCompany)
      cidmasRequest.set("IDSUNO", suno)
      if (cidmasQuery.read(cidmasRequest)) {
        supplierCode = cidmasRequest.getString("IDSUCO")
        DBAction cugex1CidmasQuery = database.table("CUGEX1").index("00").selection("F1N096").build()
        DBContainer cuges1CidmasRequest = cugex1CidmasQuery.getContainer()
        cuges1CidmasRequest.set("F1CONO", currentCompany)
        cuges1CidmasRequest.set("F1FILE", "CIDMAS")
        cuges1CidmasRequest.set("F1PK01", suno)
        cuges1CidmasRequest.set("F1PK02", "")
        cuges1CidmasRequest.set("F1PK03", "")
        cuges1CidmasRequest.set("F1PK04", "")
        cuges1CidmasRequest.set("F1PK05", "")
        cuges1CidmasRequest.set("F1PK06", "")
        cuges1CidmasRequest.set("F1PK07", "")
        cuges1CidmasRequest.set("F1PK08", "")
        if (cugex1CidmasQuery.read(cuges1CidmasRequest)) {
          vacode = cuges1CidmasRequest.get("F1N096")
          if (vacode != "") {
            Float f = Float.parseFloat(vacode)
            vacode = String.valueOf(f.intValue())
          }
        }
        if ("" != supplierCode) {
          supplierCode = supplierCode.replace("S", "")
          if (supplierCode.length() == 9) {
            supplierCode = supplierCode.substring(0, supplierCode.length() - 3)
          } else if (supplierCode.length() == 6) {
            supplierCode = supplierCode.substring(supplierCode.length() - 6)
          }
          supplierCode = supplierCode.replaceAll("^0+", "")
        }
      }
    }


    rscl = ext010Result.get("EXRSCL")
    assortimentLogistique = ext010Result.get("EXASGD")
  }
  // Retrieve EXT040
  Closure<?> ext040Reader = { DBContainer ext040Result ->
    foundSigma6 = true
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
        sigma9DirectDeliveryAssortment = ext040Result.get("EXASCD")
      } else {
        DBAction mitaunQuery = database.table("MITAUN").index("00").selection("MUCOFA").build()
        DBContainer mitaunRequest = mitaunQuery.getContainer()
        mitaunRequest.set("MUCONO", currentCompany)
        mitaunRequest.set("MUITNO", ext040Result.get("EXITNO"))
        mitaunRequest.set("MUAUTP", 1)
        mitaunRequest.set("MUALUN", "COL")
        if (mitaunQuery.read(mitaunRequest)) {
          currentCofa = mitaunRequest.get("MUCOFA")
          if (savedCofa == 0) {
            savedCofa = currentCofa
            sigma9NoDirectDelivery = ext040Result.get("EXITNO")
            sigma9NoDirectDeliveryAssortment = ext040Result.get("EXASCD")
          } else {
            if (currentCofa < savedCofa) {
              savedCofa = currentCofa
              sigma9NoDirectDelivery = ext040Result.get("EXITNO")
              sigma9NoDirectDeliveryAssortment = ext040Result.get("EXASCD")
            }
          }
        }
      }
    }
  }

  // Retrieve EXT041
  Closure<?> ext040ReaderWritePrdStoFile = { DBContainer ext040Result ->
    sigma6 = ext040Result.get("EXPOPN")
    sigma9DirectDelivery = ""
    sigma9NoDirectDelivery = ""
    sigma9DirectDeliveryAssortment = ""
    sigma9NoDirectDeliveryAssortment = ""
    savedCofa = 0

    foundSigma6 = true
    chb7 = 0
    DBAction mitmasCugex1Query = database.table("CUGEX1").index("00").selection("F1A830").build()
    DBContainer mitmasCugex1Request = mitmasCugex1Query.getContainer()
    mitmasCugex1Request.set("F1CONO", currentCompany)
    mitmasCugex1Request.set("F1FILE", "MITMAS")
    mitmasCugex1Request.set("F1PK01", ext040Result.get("EXITNO"))
    mitmasCugex1Request.set("F1PK02", "")
    mitmasCugex1Request.set("F1PK03", "")
    mitmasCugex1Request.set("F1PK04", "")
    mitmasCugex1Request.set("F1PK05", "")
    mitmasCugex1Request.set("F1PK06", "")
    mitmasCugex1Request.set("F1PK07", "")
    mitmasCugex1Request.set("F1PK08", "")
    if (mitmasCugex1Query.read(mitmasCugex1Request)) {
      if (mitmasCugex1Request.get("F1A830") == "20") {
        sigma9DirectDelivery = ext040Result.get("EXITNO")
        sigma9DirectDeliveryAssortment = ext040Result.get("EXASCD")
      } else {
        DBAction mitaunQuery = database.table("MITAUN").index("00").selection("MUCOFA").build()
        DBContainer mitaunRequest = mitaunQuery.getContainer()
        mitaunRequest.set("MUCONO", currentCompany)
        mitaunRequest.set("MUITNO", ext040Result.get("EXITNO"))
        mitaunRequest.set("MUAUTP", 1)
        mitaunRequest.set("MUALUN", "COL")
        if (mitaunQuery.read(mitaunRequest)) {
          currentCofa = mitaunRequest.get("MUCOFA")
          if (savedCofa == 0) {
            savedCofa = currentCofa
            sigma9NoDirectDelivery = ext040Result.get("EXITNO")
            sigma9NoDirectDeliveryAssortment = ext040Result.get("EXASCD")
          } else {
            if (currentCofa < savedCofa) {
              savedCofa = currentCofa
              sigma9NoDirectDelivery = ext040Result.get("EXITNO")
              sigma9NoDirectDeliveryAssortment = ext040Result.get("EXASCD")
            }
          }
        }
      }
    }
    writeEcoPrdSto()
  }


  // Write to ECOM_PRD_STO File
  public void createEcoPrdStoFile() {
    retrieveCustomerInfo()
    logFileName = "ECOM_PRD_STO_" + inCustomer + "_" + creationDate + "_" + creationTime + ".xml"
    header = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\r\n" +
      "<data>\r\n" +
      "<header>\r\n" +
      "<reference>ECOM_PRD_STO</reference>\r\n" +
      "<sequence>001</sequence>\r\n" +
      "<date>" + currentDateTime + "</date>\n" +
      "<issuer>METI</issuer>\r\n" +
      "<target>MWS</target>\r\n" +
      "<storeReference>" + inCustomer + "</storeReference>\r\n" +
      "<dependentStores/>\r\n" +
      "<transfertPriceCode>1</transfertPriceCode>\r\n" +
      "<mode>ADD_OVERWRITE</mode>\r\n" +
      "</header>\r\n" +
      "<products>"
    writeInFile(header, "")
    countLineCancellationNo = 0
    countLines = 0
    lines = ""

    // Read all POPNs in the current calendar
    DBAction ext040Query = database.table("EXT040").index("00").selection("EXPOPN").build()
    DBContainer ext040Request = ext040Query.getContainer()
    ext040Request.set("EXCONO", currentCompany)
    ext040Request.set("EXCUNO", inCustomer)
    ext040Request.set("EXCDNN", inCalendar)

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
            .position(position)
            .build()
        }
      }
      if (!ext040Query.readAll(ext040Request, 3, 10000, ext040ReaderWritePrdStoFile)) {
      }
      nbIteration++
      if (nbIteration > 5) {//max 50 0000 records
        logMessage("ERROR", "Nombre d'itération trop important cuno:${inCustomer} cdnn:${inCalendar}")
        break
      }
    }
// todo ici
    if (countLines > 0) {
      logger.debug("Nbligne Return from ext041 = " + countLines)
      writeInFile("", lines)
    }
  }

  /**
   * Write to ECOM_PRD_STO File
   */
  public void writeEcoPrdSto() {
    Closure<?> ext010Reader = { DBContainer EXT010 ->
      initLine()
      line = "<product>\r\n" +
        "<productReference>" + sigma6.trim() + "</productReference>\r\n" +
        "<cancellationMode>N</cancellationMode>\r\n" +
        "<range/>\r\n" +
        "<recommendedQuantity/>\r\n" +
        "<suppliers>\r\n" +
        "<supplier>\r\n" +
        "<supplierCode>" + supplierCode + "</supplierCode>\r\n" +
        "<variants>\r\n" +
        "<variant>\r\n" +
        "<vaCode>" + vacode + "</vaCode>\r\n" +
        "<mainVariant>N</mainVariant>\r\n" +
        "<logisticUnits>\r\n" +
        "<logisticUnit>\r\n" +
        "<luCode>" + UL.trim() + "</luCode>\n" +
        "<transfertPrice>" + prixVente + "</transfertPrice>\r\n" +
        "<transfertPriceDate>T</transfertPriceDate>\r\n" +
        "<levelledTransfertPrice/>\r\n" +
        "</logisticUnit>\r\n" +
        "</logisticUnits>\r\n" +
        "</variant>\r\n" +
        "</variants>\r\n" +
        "</supplier>\r\n" +
        "</suppliers>\r\n" +
        "</product>"
      lines += line + "\r\n"
      countLines++
      countLineCancellationNo++
      if (countLines == 5000) {
        writeInFile("", lines)
        countLines = 0
        lines = ""
      }
    }

    if (sigma9DirectDelivery.trim() != "") {
      sigma9 = sigma9DirectDelivery
      assortment = sigma9DirectDeliveryAssortment
      ExpressionFactory ext010Expression = database.getExpressionFactory("OPRBAS")
      ext010Expression = ext010Expression.le("EXFVDT", currentDate as String).and(ext010Expression.ge("EXLVDT", currentDate as String))
      DBAction ext010Query = database.table("EXT010").index("02").matching(ext010Expression).selection("EXCMDE").build()
      DBContainer ext010Request = ext010Query.getContainer()
      ext010Request.set("EXCONO", currentCompany)
      ext010Request.set("EXCUNO", inCustomer)
      ext010Request.set("EXITNO", sigma9DirectDelivery)
      if (ext010Query.readAll(ext010Request, 3, 1, ext010Reader)) {
      }
    }
    if (sigma9NoDirectDelivery.trim() != "") {
      sigma9 = sigma9NoDirectDelivery
      assortment = sigma9NoDirectDeliveryAssortment
      ExpressionFactory ext010Expression = database.getExpressionFactory("OPRBAS")
      ext010Expression = ext010Expression.le("EXFVDT", currentDate as String).and(ext010Expression.ge("EXLVDT", currentDate as String))
      DBAction ext010Query = database.table("EXT010").index("02").matching(ext010Expression).selection("EXCMDE").build()
      DBContainer ext010Request = ext010Query.getContainer()
      ext010Request.set("EXCONO", currentCompany)
      ext010Request.set("EXCUNO", inCustomer)
      ext010Request.set("EXITNO", sigma9NoDirectDelivery)
      if (ext010Query.readAll(ext010Request, 3, 1, ext010Reader)) {
      }
    }
  }

  /**
   * Retrieve customer information
   */
  private void retrieveCustomerInfo() {
    customerName = ""
    pltb = ""
    lhcd = ""
    DBAction ocusmaQuery = database.table("OCUSMA").index("00").selection("OKCUNM", "OKPLTB", "OKLHCD").build()
    DBContainer ocusmaRequest = ocusmaQuery.getContainer()
    ocusmaRequest.set("OKCONO", currentCompany)
    ocusmaRequest.set("OKCUNO", inCustomer)
    if (ocusmaQuery.read(ocusmaRequest)) {
      customerName = ocusmaRequest.get("OKCUNM")
      pltb = ocusmaRequest.get("OKPLTB")
      lhcd = ocusmaRequest.get("OKLHCD")
    }
  }


  // Init line informations
  public void initLine() {
    UL = ""
    libelleArticle = ""
    vacode = ""
    DBAction mitmasQuery = database.table("MITMAS").index("00").selection("MMECVE", "MMFUDS", "MMSPE2", "MMSPE1", "MMBUAR", "MMCFI1", "MMSPE3", "MMHIE1", "MMHIE2", "MMHIE3", "MMHIE4", "MMHIE5", "MMSUNO", "MMDIM1", "MMDIM2", "MMDIM3", "MMSPGV", "MMNEWE", "MMIHEI", "MMIWID", "MMILEN", "MMCFI2", "MMITCL").build()
    DBContainer mitmasRequest = mitmasQuery.getContainer()
    mitmasRequest.set("MMCONO", currentCompany)
    mitmasRequest.set("MMITNO", sigma9)

    if (mitmasQuery.read(mitmasRequest)) {
      UL = mitmasRequest.get("MMECVE")
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

    parCombien = ""
    nbCouchePalette = 0
    nbColisParCouche = 0
    nbColisParPalette = 0
    nbUVCParPalette = 0

    double cofaCol = 0
    DBAction mitaunQuery = database.table("MITAUN").index("00").selection("MUCOFA").build()
    DBContainer mitaunRequest = mitaunQuery.getContainer()
    mitaunRequest.set("MUCONO", currentCompany)
    mitaunRequest.set("MUITNO", sigma9)
    mitaunRequest.set("MUAUTP", 1)
    mitaunRequest.set("MUALUN", "COL")
    if (mitaunQuery.read(mitaunRequest)) {
      cofaCol = mitaunRequest.get("MUCOFA")
    }

    double cofaUco = 0
    mitaunRequest.set("MUAUTP", 1)
    mitaunRequest.set("MUALUN", "UCO")
    if (mitaunQuery.read(mitaunRequest)) {
      cofaUco = mitaunRequest.get("MUCOFA")
    }

    double cofaUpa = 0
    mitaunRequest.set("MUAUTP", 1)
    mitaunRequest.set("MUALUN", "UPA")
    if (mitaunQuery.read(mitaunRequest)) {
      cofaUpa = mitaunRequest.get("MUCOFA")
    }

    parCombien = cofaCol as String
    if (cofaUco != 0)
      nbCouchePalette = cofaUpa / cofaUco
    if (cofaCol != 0) {
      nbColisParCouche = cofaUco / cofaCol
      nbColisParPalette = cofaUpa / cofaCol
    }
    nbUVCParPalette = cofaUpa

    LocalDateTime timeOfCreation = LocalDateTime.now()

    vacode = ""
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
    if (ext010Query.readAll(ext010Request, 3, 1, ext010Reader)) {
    }


    prixVente = ext010Request.get("EXSAPR")
    unitePrixVente = ""
    savedFromValidDate = 0
    currentFromValidDate = 0
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
      ExpressionFactory oprbasExpression = database.getExpressionFactory("OPRBAS")
      oprbasExpression = oprbasExpression.le("ODVFDT", currentDate as String).and(oprbasExpression.ge("ODLVDT", currentDate as String))
      oprbasExpression = oprbasExpression.and(oprbasExpression.eq("ODITNO", sigma9))
      oprbasExpression = oprbasExpression.and(oprbasExpression.eq("ODCUNO", inCustomer))
      DBAction oprbasQuery = database.table("OPRBAS").index("00").matching(oprbasExpression).selection("ODVFDT", "ODSAPR", "ODSPUN").build()
      DBContainer oprbasRequest = oprbasQuery.getContainer()
      oprbasRequest.set("ODCONO", currentCompany)
      oprbasRequest.set("ODPRRF", oprmtxRequest.get("DXPRRF"))

      // Retrieve OPRBAS
      Closure<?> oprbasReader = { DBContainer oprbasResult ->
        currentFromValidDate = oprbasResult.get("ODVFDT")
        if (currentFromValidDate > savedFromValidDate) {
          savedFromValidDate = oprbasResult.get("ODVFDT")
          prixVente = oprbasResult.get("ODSAPR")

          if (prixVente != "" && prixVente != "0") {
            Double d = Double.parseDouble(prixVente)
            prixVente = String.format("%.4f", d)
          }
          unitePrixVente = oprbasResult.get("ODSPUN")
        }
      }
      oprbasQuery.readAll(oprbasRequest, 2, 10000, oprbasReader)
    }
  }

  /**
   * Write gap line
   */
  public void writeGapLine() {
    retrievePreviousCalendar()
    logger.debug("#PB Start Write GapLine new calendar = ${inCalendar} Previous Calendar ${previousCalendar}")
    countLines = 0
    countLineCancellationYes = 0
    lines = ""
    Closure<?> ext041Reader = { DBContainer ext041Result ->
      sigma6 = ext041Result.get("EXPOPN")
      sigma9 = ext041Result.get("EXITNO")

      foundSigma6 = false
      DBAction queryExt040 = database.table("EXT040").index("00").selection("EXITNO", "EXASCD", "EXITNO").build()
      DBContainer EXT040 = queryExt040.getContainer()
      EXT040.set("EXCONO", currentCompany)
      EXT040.set("EXCUNO", inCustomer)
      EXT040.set("EXCDNN", inCalendar)
      EXT040.set("EXPOPN", sigma6)
      if (!queryExt040.readAll(EXT040, 4, 1, ext040Reader)) {
        logger.debug("#PB Pas trouve in ext040 Calendar = ${inCalendar} Customer ${inCustomer} popn ${sigma6}")
        foundSigma6 = true
      }
      if (!foundSigma6) {
        initLine()
        line = "<product>\r\n" +
          "<productReference>" + sigma6.trim() + "</productReference>\r\n" +
          "<cancellationMode>O</cancellationMode>\r\n" +
          "<range/>\r\n" +
          "<recommendedQuantity/>\r\n" +
          "<suppliers>\r\n" +
          "<supplier>\r\n" +
          "<supplierCode>" + supplierCode + "</supplierCode>\r\n" +
          "<variants>\r\n" +
          "<variant>\r\n" +
          "<vaCode>" + vacode + "</vaCode>\r\n" +
          "<mainVariant>N</mainVariant>\r\n" +
          "<logisticUnits>\r\n" +
          "<logisticUnit>\r\n" +
          "<luCode>" + UL.trim() + "</luCode>\n" +
          "<transfertPrice>" + prixVente + "</transfertPrice>\r\n" +
          "<transfertPriceDate>T</transfertPriceDate>\r\n" +
          "<levelledTransfertPrice/>\r\n" +
          "</logisticUnit>\r\n" +
          "</logisticUnits>\r\n" +
          "</variant>\r\n" +
          "</variants>\r\n" +
          "</supplier>\r\n" +
          "</suppliers>\r\n" +
          "</product>"

        countLines++
        countLineCancellationYes++
        lines += line + (countLines < 5000 ? "\r\n" : "")
        if (countLines == 5000) {
          writeInFile("", lines)
          countLines = 0
          lines = ""
        }
      }
    }

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
            .position(position)
            .build()
        }
      }
      if (!ext041Query.readAll(ext041Request, 3, 10000, ext041Reader)) {
      }
      nbIteration++
      if (nbIteration > 3) {//max 50 0000 records
        logMessage("ERROR", "Nombre d'itération trop important cuno:${inCustomer} cdnn:${inCalendar}")
        break
      }
    }

    //Write last lines and close xml
    if (countLines > 0) {
      logger.debug("Nbligne Return from ext041 = " + countLines)
      lines += "</products>\n" +
        "</data>"
      writeInFile("", lines)

    } else {
      lines += "</products>\n" +
        "</data>"
      writeInFile("", lines)
    }
    logMessage("INFO", "End catalogue B2B for customer:${inCustomer} calendar:${inCalendar} nblignes actives:${countLineCancellationNo} nblignes annulees:${countLineCancellationYes}")
  }

  /**
   * Retrieve previous calendar
   */
  private void retrievePreviousCalendar() {
    // Retrieve EXT041
    Closure<?> ext041Reader = { DBContainer ext041Result ->
      previousCalendar = ext041Result.get("EXCDNN")
    }
    previousCalendar = ""

    DBAction ext041Query = database.table("EXT041").index("20").selection("EXCDNN").reverse().build()
    DBContainer ext041Request = ext041Query.getContainer()
    ext041Request.set("EXCONO", currentCompany)
    ext041Request.set("EXCUNO", inCustomer)
    if (!ext041Query.readAll(ext041Request, 2, 1, ext041Reader)) {
    }
    if (inCalendar == previousCalendar)
      isReedition = true
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

  // Log message
  void writeInFile(String header, String line, String directory = "") {
    if (directory == "") {
      textFiles.open("FileImport/COM04")
    } else {
      textFiles.open(directory)
    }
    //if (logFileName.endsWith("docNumber.xml"))
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

  /**
   * Get job data from EXTJOB
   * @param referenceId
   * @return Optional<String>
   */
  private Optional<String> getJobData(String referenceId) {
    DBAction query = database.table("EXTJOB").index("00").selection("EXDATA").build()
    DBContainer container = query.createContainer()
    container.set("EXRFID", referenceId)
    if (query.read(container)) {
      return Optional.of(container.getString("EXDATA"))
    } else {
    }
    return Optional.empty()
  }

/**
 * Write end file
 */
  public void writeEndFile() {
    logFileName = "ECOM_PRD_STO_" + inCustomer + "_" + creationDate + "_" + creationTime + "-" + "docNumber.xml"
    docnumber = "ECOM_PRD_STO_" + inCustomer + "_" + creationDate + "_" + creationTime
    header = "<?xml version='1.0' encoding='UTF-8' standalone='no'?><LoaddocumentNumber xmlns='http://schema.infor.com/InforOAGIS/2' xmlns:xsi='http://www.w3.org/2001/XMLSchema-instance' releaseID='9.2' versionID='2.14.6' xsi:schemaLocation='http://schema.infor.com/InforOAGIS/2 http://schema.infor.com/InforOAGIS/BODs/LoaddocumentNumber.xsd'><ApplicationArea><Sender><LogicalID>casino.com</LogicalID><ConfirmationCode>OnError</ConfirmationCode></Sender><CreationDateTime>2023-04-04</CreationDateTime></ApplicationArea><DataArea><Load><TenantID>CASINO_TST</TenantID><AccountingEntityID>100_</AccountingEntityID><ActionCriteria><ActionExpression actionCode='' expressionLanguage=''/></ActionCriteria></Load><documentNumber><DocumentType>ECOM_PRD_STO</DocumentType><DocumentNumber>${docnumber}.xml</DocumentNumber><DocumentPath>/FileImport/COM04/${docnumber}.xml</DocumentPath></documentNumber></DataArea></LoaddocumentNumber>"
    writeInFile(header, "")
  }

  /**
   * Log EXT875
   * @param rfid
   * @param jbnm
   * @param levl
   * @param tmsg
   */
  private void logEXT875(String rfid, String jbnm, String levl, String tmsg) {
    Map parameters = ["RFID": rfid, "JBNM": jbnm, "LEVL": levl, "TMSG": tmsg]
    Closure<?> handler = { Map<String, String> response ->
      if (response.error != null) {
        logger.debug("#PB  error EXT875MI:" + response.errorMessage)
      }
    }
    miCaller.call("EXT875MI", "AddLog", parameters, handler)
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
      DBAction queryMbmtrd = database.table("MBMTRD").index("00").selection("TDMVXD",  "TDTX15").build()
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
   * Delete records related to the current job from EXTJOB table
   * @return
   */
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
}

