/**
 * README
 *
 * Name : EXT041
 * Description : Generates the ECOM_PROD_STO file
 * Date         Changed By   Description
 * 20240325     PBEAUDOUIN   INITIAL
 */
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

public class EXT041 extends ExtendM3Batch {
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
  private String currentDateTime
  private Integer currentDate
  private String docnumber
  private String inCustomer
  private String inCalendar
  private Integer inAllContacts
  private Integer inSchedule
  private String sigma6
  private String sigma9
  private String sigma9_DirectDelivery
  private String sigma9_NoDirectDelivery
  private String sigma9_DirectDelivery_assortment
  private String sigma9_NoDirectDelivery_assortment
  private int chb7
  private String customerName
  private String creationDate
  private String creationTime
  private String header
  private String line
  private String lines
  private String assortment
  private int countLines
  private double currentCOFA
  private double savedCOFA
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
  private String nbCouche_palette
  private String nbColisParCouche
  private String nbColisParPalette
  private String nbUVCParPalette
  private String rscl
  private String assortimentLogistique
  private String pltb
  private String lhcd
  private String fileJobNumber

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
    // Get job number
    LocalDateTime timeOfCreation = LocalDateTime.now()
    currentDateTime = timeOfCreation.format(DateTimeFormatter.ofPattern("yy-MM-dd")) +"T"+ timeOfCreation.format(DateTimeFormatter.ofPattern("HH:mm:ss"))
    logger.debug("fileJobNumber=${fileJobNumber}")

    if(batch.getReferenceId().isPresent()){
      Optional<String> data = getJobData(batch.getReferenceId().get())
      performActualJob(data)
    } else {
      // No job data found
    }
  }
  // Perform actual job
  private performActualJob(Optional<String> data){
    if(!data.isPresent()){
      return
    }
    rawData = data.get()
    inCustomer = getFirstParameter()
    inCalendar = getNextParameter()
    inAllContacts = getNextParameter() as Integer
    inSchedule = 0
    try {
      inSchedule = getNextParameter() as Integer
    } catch (NumberFormatException e){

    }

    currentCompany = (Integer)program.getLDAZD().CONO

    LocalDateTime timeOfCreation = LocalDateTime.now()
    currentDate = timeOfCreation.format(DateTimeFormatter.ofPattern("yyyyMMdd")) as Integer
    creationDate = timeOfCreation.format(DateTimeFormatter.ofPattern("yyyyMMdd")) as String
    creationTime = timeOfCreation.format(DateTimeFormatter.ofPattern("HHmmss")) as String

    // Perform Job

    writeEcoPrdStoFile()
    writeEndFile()

  }

  // Retrieve EXT010
  Closure<?> outData_EXT010_2 = { DBContainer EXT010 ->
    initLine()
    //line = sigma6.trim()+";N;"+supplierCode.trim()+";"+vacode+";N;"+UL.trim()+";"+prixVente.trim()+";"+unitePrixVente.trim()+";20240325;"+parCombien.trim()+";"+nbCouche_palette.trim()+";"+nbColisParCouche.trim()+";"+nbColisParPalette.trim()+";"+nbUVCParPalette.trim()
    line = "<product>\r\n" +
      "<productReference>"+sigma6.trim()+"</productReference>\r\n" +
      "<cancellationMode>N</cancellationMode>\r\n" +
      "<range/>\r\n" +
      "<recommendedQuantity/>\r\n" +
      "<suppliers>\r\n" +
      "<supplier>\r\n" +
      "<supplierCode>"+supplierCode+"</supplierCode>\r\n" +
      "<variants>\r\n" +
      "<variant>\r\n" +
      "<vaCode>"+vacode+"</vaCode>\r\n" +
      "<mainVariant>N</mainVariant>\r\n" +
      "<logisticUnits>\r\n" +
      "<logisticUnit>\r\n" +
      "<luCode>"+UL.trim()+"</luCode>\n" +
      "<transfertPrice>"+prixVente+"</transfertPrice>\r\n" +
      "<transfertPriceDate>T</transfertPriceDate>\r\n" +
      "<levelledTransfertPrice/>\r\n" +
      "</logisticUnit>\r\n" +
      "</logisticUnits>\r\n" +
      "</variant>\r\n" +
      "</variants>\r\n" +
      "</supplier>\r\n" +
      "</suppliers>\r\n" +
      "</product>"
    //logMessage("", line)
    lines += line + "\r\n"
    countLines++
    if(countLines == 5000) {
      logMessage("", lines)
      countLines = 0
      lines = ""
    }
  }

  // Retrieve EXT010
  Closure<?> outData_EXT010_3 = { DBContainer EXT010 ->
    String suno = ""
    String sule = EXT010.get("EXSULE")
    String suld = EXT010.get("EXSULD")
    if(sule.trim()!= "") {
      suno = sule
    } else {
      suno = suld
    }
    if(suno.trim()!= "") {
      DBAction query_CIDMAS = database.table("CIDMAS").index("00").selection("IDSUCO").build()
      DBContainer CIDMAS = query_CIDMAS.getContainer()
      CIDMAS.set("IDCONO", currentCompany)
      CIDMAS.set("IDSUNO", suno)
      if (query_CIDMAS.read(CIDMAS)) {
        supplierCode = CIDMAS.getString("IDSUCO")
        DBAction query_CUGEX1_CIDMAS = database.table("CUGEX1").index("00").selection("F1N096").build()
        DBContainer CUGEX1_CIDMAS = query_CUGEX1_CIDMAS.getContainer()
        CUGEX1_CIDMAS.set("F1CONO", currentCompany)
        CUGEX1_CIDMAS.set("F1FILE", "CIDMAS")
        CUGEX1_CIDMAS.set("F1PK01", suno)
        CUGEX1_CIDMAS.set("F1PK02", "")
        CUGEX1_CIDMAS.set("F1PK03", "")
        CUGEX1_CIDMAS.set("F1PK04", "")
        CUGEX1_CIDMAS.set("F1PK05", "")
        CUGEX1_CIDMAS.set("F1PK06", "")
        CUGEX1_CIDMAS.set("F1PK07", "")
        CUGEX1_CIDMAS.set("F1PK08", "")
        if (query_CUGEX1_CIDMAS.read(CUGEX1_CIDMAS)) {
          vacode = CUGEX1_CIDMAS.get("F1N096")

          if (vacode !="") {
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



    rscl = EXT010.get("EXRSCL")
    assortimentLogistique = EXT010.get("EXASGD")
  }
  // Retrieve EXT040
  Closure<?> outData_EXT040 = { DBContainer EXT040 ->
    chb7 = 0
    DBAction query_CUGEX1_MITMAS = database.table("CUGEX1").index("00").selection("F1A830").build()
    DBContainer CUGEX1_MITMAS = query_CUGEX1_MITMAS.getContainer()
    CUGEX1_MITMAS.set("F1CONO", currentCompany)
    CUGEX1_MITMAS.set("F1FILE",  "MITMAS")
    CUGEX1_MITMAS.set("F1PK01",  EXT040.get("EXITNO"))
    CUGEX1_MITMAS.set("F1PK02",  "")
    CUGEX1_MITMAS.set("F1PK03",  "")
    CUGEX1_MITMAS.set("F1PK04",  "")
    CUGEX1_MITMAS.set("F1PK05",  "")
    CUGEX1_MITMAS.set("F1PK06",  "")
    CUGEX1_MITMAS.set("F1PK07",  "")
    CUGEX1_MITMAS.set("F1PK08",  "")
    if(query_CUGEX1_MITMAS.read(CUGEX1_MITMAS)){
      if(CUGEX1_MITMAS.get("F1A830") == "20") {
        sigma9_DirectDelivery = EXT040.get("EXITNO")
        sigma9_DirectDelivery_assortment = EXT040.get("EXASCD")
      } else {
        DBAction query_MITAUN = database.table("MITAUN").index("00").selection("MUCOFA").build()
        DBContainer MITAUN = query_MITAUN.getContainer()
        MITAUN.set("MUCONO", currentCompany)
        MITAUN.set("MUITNO", EXT040.get("EXITNO"))
        MITAUN.set("MUAUTP", 1)
        MITAUN.set("MUALUN", "COL")
        if(query_MITAUN.read(MITAUN)){
          currentCOFA = MITAUN.get("MUCOFA")
          if(savedCOFA == 0) {
            savedCOFA = currentCOFA
            sigma9_NoDirectDelivery = EXT040.get("EXITNO")
            sigma9_NoDirectDelivery_assortment = EXT040.get("EXASCD")
          } else {
            if(currentCOFA < savedCOFA) {
              savedCOFA = currentCOFA
              sigma9_NoDirectDelivery = EXT040.get("EXITNO")
              sigma9_NoDirectDelivery_assortment = EXT040.get("EXASCD")
            }
          }
        }
      }
    }
  }

  // Retrieve EXT041
  Closure<?> outData_EXT041_2 = { DBContainer EXT041 ->
    sigma6 = EXT041.get("EXPOPN")
    sigma9_DirectDelivery = ""
    sigma9_NoDirectDelivery = ""
    sigma9_DirectDelivery_assortment = ""
    sigma9_NoDirectDelivery_assortment = ""
    savedCOFA = 0
    // Read all ITNOs attached to the current POPN
    DBAction query_EXT040 = database.table("EXT040").index("00").selection("EXITNO", "EXASCD").build()
    DBContainer EXT040 = query_EXT040.getContainer()
    EXT040.set("EXCONO", currentCompany)
    EXT040.set("EXCUNO", inCustomer)
    EXT040.set("EXCDNN", inCalendar)
    EXT040.set("EXPOPN", sigma6)
    if(!query_EXT040.readAll(EXT040, 4, outData_EXT040)){}

    writeEcoPrdSto()

  }


  // Write to ECOM_PROD_STO File
  public void writeEcoPrdStoFile() {
    retrieveCustomerInfo()


    logFileName = "ECOM_PROD_STO_" +inCustomer +"_"+creationDate+"_"+creationTime+".xml"
    header = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\r\n" +
      "<data>\r\n" +
      "<header>\r\n" +
      "<reference>ECOM_PRD_STO</reference>\r\n" +
      "<sequence>001</sequence>\r\n" +
      "<date>"+currentDateTime+"</date>\n" +
      "<issuer>METI</issuer>\r\n" +
      "<target>MWS</target>\r\n" +
      "<storeReference>"+inCustomer+"</storeReference>\r\n" +
      "<dependentStores/>\r\n" +
      "<transfertPriceCode>1</transfertPriceCode>\r\n" +
      "<mode>ADD_OVERWRITE</mode>\r\n" +
      "</header>\r\n" +
      "<products>"
    logMessage(header, "")
    countLines = 0
    lines = ""

    // Read all POPNs in the current calendar
    DBAction query_EXT041 = database.table("EXT041").index("00").selection("EXPOPN").build()
    DBContainer EXT041 = query_EXT041.getContainer()
    EXT041.set("EXCONO", currentCompany)
    EXT041.set("EXCUNO", inCustomer)
    EXT041.set("EXCDNN", inCalendar)
    logger.debug("CONO /CUNO / EXCDNN = " + currentCompany + " / " + inCustomer + " / " + inCalendar )
    if(!query_EXT041.readAll(EXT041, 3, outData_EXT041_2)){}

    if(countLines > 0) {
      logger.debug("Nbligne Return from ext041 = " + countLines)
      lines += "</products>\n" +
        "</data>"
      logMessage("", lines)

    }

  }

  // Write to writeEcomPrdSto
  public void writeEcoPrdSto() {
    logger.debug("writeCalendar sigma9_DirectDelivery = " + sigma9_DirectDelivery)
    logger.debug("writeCalendar sigma9_NoDirectDelivery = " + sigma9_NoDirectDelivery)
    if(sigma9_DirectDelivery.trim() != "") {
      logger.debug("Ecriture sigma9_DirectDelivery 1 = " + sigma9_DirectDelivery)
      logger.debug("sigma9_DirectDelivery_assortment = " + sigma9_DirectDelivery_assortment)
      sigma9 = sigma9_DirectDelivery
      assortment = sigma9_DirectDelivery_assortment
      ExpressionFactory expression_EXT010 = database.getExpressionFactory("OPRBAS")
      expression_EXT010 = expression_EXT010.le("EXFVDT", currentDate as String).and(expression_EXT010.ge("EXLVDT", currentDate as String))
      DBAction query_EXT010 = database.table("EXT010").index("02").matching(expression_EXT010).selection("EXCMDE").build()
      DBContainer EXT010 = query_EXT010.getContainer()
      EXT010.set("EXCONO", currentCompany)
      EXT010.set("EXCUNO", inCustomer)
      EXT010.set("EXITNO", sigma9_DirectDelivery)
      if(query_EXT010.readAll(EXT010, 3, 1, outData_EXT010_2)){}
    }
    if(sigma9_NoDirectDelivery.trim() != "") {
      logger.debug("Ecriture sigma9_NoDirectDelivery 1 = " + sigma9_NoDirectDelivery)
      logger.debug("sigma9_NoDirectDelivery_assortment = " + sigma9_NoDirectDelivery_assortment)
      sigma9 = sigma9_NoDirectDelivery
      assortment = sigma9_NoDirectDelivery_assortment
      ExpressionFactory expression_EXT010 = database.getExpressionFactory("OPRBAS")
      expression_EXT010 = expression_EXT010.le("EXFVDT", currentDate as String).and(expression_EXT010.ge("EXLVDT", currentDate as String))
      DBAction query_EXT010 = database.table("EXT010").index("02").matching(expression_EXT010).selection("EXCMDE").build()
      DBContainer EXT010 = query_EXT010.getContainer()
      EXT010.set("EXCONO", currentCompany)
      EXT010.set("EXCUNO", inCustomer)
      EXT010.set("EXITNO", sigma9_NoDirectDelivery)
      if(query_EXT010.readAll(EXT010, 3, 1, outData_EXT010_2)){}
    }
  }
  private void retrieveCustomerInfo() {
    customerName = ""
    pltb = ""
    lhcd = ""
    DBAction query = database.table("OCUSMA").index("00").selection("OKCUNM", "OKPLTB", "OKLHCD").build()
    DBContainer OCUSMA = query.getContainer()
    OCUSMA.set("OKCONO", currentCompany)
    OCUSMA.set("OKCUNO", inCustomer)
    if (query.read(OCUSMA)) {
      customerName = OCUSMA.get("OKCUNM")
      pltb = OCUSMA.get("OKPLTB")
      lhcd = OCUSMA.get("OKLHCD")
    }
  }


  // Init line informations
  public void initLine() {
    UL = ""
    libelleArticle = ""
    vacode=""
    DBAction query_MITMAS = database.table("MITMAS").index("00").selection("MMECVE","MMFUDS","MMSPE2","MMSPE1", "MMBUAR","MMCFI1","MMSPE3","MMHIE1","MMHIE2","MMHIE3","MMHIE4","MMHIE5","MMSUNO","MMDIM1","MMDIM2","MMDIM3","MMSPGV","MMNEWE","MMIHEI","MMIWID","MMILEN","MMCFI2","MMITCL").build()
    DBContainer MITMAS = query_MITMAS.getContainer()
    MITMAS.set("MMCONO", currentCompany)
    MITMAS.set("MMITNO", sigma9)

    if(query_MITMAS.read(MITMAS)) {
      UL = MITMAS.get("MMECVE")
      libelleArticle = MITMAS.get("MMFUDS")
      if (lhcd.trim() != "FR") {
        DBAction query_MITLAD = database.table("MITLAD").index("00").selection("MDFUDS").build()
        DBContainer MITLAD = query_MITLAD.getContainer()
        MITLAD.set("MDCONO", currentCompany)
        MITLAD.set("MDITNO", sigma9)
        MITLAD.set("MDLNCD", "GB")
        if (query_MITLAD.read(MITLAD)) {
          libelleArticle = MITLAD.get("MDFUDS")
        }
      }
    }



    parCombien = ""
    nbCouche_palette = 0
    nbColisParCouche = 0
    nbColisParPalette = 0
    nbUVCParPalette = 0

    double cofaCOL = 0
    DBAction query_MITAUN = database.table("MITAUN").index("00").selection("MUCOFA").build()
    DBContainer MITAUN = query_MITAUN.getContainer()
    MITAUN.set("MUCONO", currentCompany)
    MITAUN.set("MUITNO", sigma9)
    MITAUN.set("MUAUTP", 1)
    MITAUN.set("MUALUN", "COL")
    if(query_MITAUN.read(MITAUN)){
      cofaCOL = MITAUN.get("MUCOFA")
    }

    double cofaUCO = 0
    MITAUN.set("MUAUTP", 1)
    MITAUN.set("MUALUN", "UCO")
    if(query_MITAUN.read(MITAUN)){
      cofaUCO = MITAUN.get("MUCOFA")
    }

    double cofaUPA = 0
    MITAUN.set("MUAUTP", 1)
    MITAUN.set("MUALUN", "UPA")
    if(query_MITAUN.read(MITAUN)){
      cofaUPA = MITAUN.get("MUCOFA")
    }

    parCombien = cofaCOL as String
    if(cofaUCO != 0)
      nbCouche_palette = cofaUPA/cofaUCO
    if(cofaCOL != 0) {
      nbColisParCouche = cofaUCO/cofaCOL
      nbColisParPalette = cofaUPA/cofaCOL
    }
    nbUVCParPalette = cofaUPA

    LocalDateTime timeOfCreation = LocalDateTime.now()


    vacode = ""
    entrepot = ""
    rscl = ""
    assortimentLogistique = ""
    saisonnalite = ""
    diffusion = ""
    ExpressionFactory expression_EXT010 = database.getExpressionFactory("EXT010")
    expression_EXT010 = expression_EXT010.le("EXFVDT", currentDate as String).and(expression_EXT010.ge("EXLVDT", currentDate as String))
    DBAction query_EXT010 = database.table("EXT010").index("02").matching(expression_EXT010).selection("EXSULE", "EXSULD", "EXRSCL","EXASGD").build()
    DBContainer EXT010 = query_EXT010.getContainer()
    EXT010.set("EXCONO", currentCompany)
    EXT010.set("EXCUNO", inCustomer)
    EXT010.set("EXITNO", sigma9)
    if(query_EXT010.readAll(EXT010, 3, 1, outData_EXT010_3)){}


    prixVente = "0"
    unitePrixVente = ""
    savedFromValidDate = 0
    currentFromValidDate = 0
    DBAction query_OPRMTX = database.table("OPRMTX").index("00").selection("DXPRRF").build()
    DBContainer OPRMTX = query_OPRMTX.getContainer()
    OPRMTX.set("DXCONO", currentCompany)
    OPRMTX.set("DXPLTB", pltb)
    OPRMTX.set("DXPREX", " 5")
    OPRMTX.set("DXOBV1", inCustomer)
    OPRMTX.set("DXOBV2", "")
    OPRMTX.set("DXOBV3", "")
    OPRMTX.set("DXOBV4", "")
    OPRMTX.set("DXOBV5", "")
    if (query_OPRMTX.read(OPRMTX)) {
      ExpressionFactory expression = database.getExpressionFactory("OPRBAS")
      expression = expression.le("ODVFDT", currentDate as String).and(expression.ge("ODLVDT", currentDate as String))
      expression = expression.and(expression.eq("ODITNO", sigma9))
      expression = expression.and(expression.eq("ODCUNO", inCustomer))
      DBAction query_OPRBAS = database.table("OPRBAS").index("00").matching(expression).selection("ODVFDT","ODSAPR","ODSPUN").build()
      DBContainer OPRBAS = query_OPRBAS.getContainer()
      OPRBAS.set("ODCONO", currentCompany)
      OPRBAS.set("ODPRRF", OPRMTX.get("DXPRRF"))
      query_OPRBAS.readAll(OPRBAS, 2, outData_OPRBAS)
    }

  }
  // Retrieve OPRBAS
  Closure<?> outData_OPRBAS = { DBContainer OPRBAS ->
    currentFromValidDate = OPRBAS.get("ODVFDT")
    if(currentFromValidDate > savedFromValidDate) {
      savedFromValidDate = OPRBAS.get("ODVFDT")
      prixVente = OPRBAS.get("ODSAPR")

      if (prixVente != "" && prixVente != "0") {
        Double d = Double.parseDouble(prixVente)
        prixVente = String.format("%.4f", d)
      }
      unitePrixVente = OPRBAS.get("ODSPUN")
    }
  }

  // Get first parameter
  private String getFirstParameter(){
    rawDataLength = rawData.length()
    beginIndex = 0
    endIndex = rawData.indexOf(";")
    // Get parameter
    String parameter = rawData.substring(beginIndex, endIndex)
    return parameter
  }
  // Get next parameter
  private String getNextParameter(){
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
  void logMessage(String header, String line) {
    textFiles.open("FileImport/COM04")

    //if (logFileName.endsWith("docNumber.xml"))
    if(header.trim() != "") {
      log(header + "\r\n")
    }
    if(line.trim() != "") {
      log(line)
    }
  }
  // Log
  void log(String message) {
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
  private Optional<String> getJobData(String referenceId){
    def query = database.table("EXTJOB").index("00").selection("EXDATA").build()
    def container = query.createContainer()
    container.set("EXRFID", referenceId)
    if (query.read(container)){
      return Optional.of(container.getString("EXDATA"))
    } else {
    }
    return Optional.empty()
  }

  public void writeEndFile() {
    //logFileName = fileJobNumber + "-" +inCustomer + "-" + inCalendar + "-" + "docNumber.xml"
    logFileName = "ECOM_PROD_STO_" +inCustomer +"_"+creationDate+"_"+creationTime+ "-" + "docNumber.xml"
    //docnumber = fileJobNumber + "-" + inCustomer + "-" + inCalendar
    docnumber ="ECOM_PROD_STO_" +inCustomer +"_"+creationDate+"_"+creationTime
    header = "<?xml version=\"1.0\" encoding=\"UTF-8\"?><Document>  <DocumentType>ECOM_PROD_STO</DocumentType>  <DocumentNumber>${docnumber}</DocumentNumber>  <DocumentPath>F:\\COM04\\</DocumentPath></Document>"
    logMessage(header, "")
  }
}
