/**
 * README
 * This extension is used by Mashup
 *
 * Name : EXT040
 * Description : Generates the calendar file and associated files
 * Date         Changed By   Description
 * 20230309     RENARN       COMX02 - Cadencier
 * 20231122     RENARN       New columns added. Columns heading rectified
 * 20240327     PATBEA       Correction Key to read table CCUCON. CCEMRE missing
 * 20240402     PATBEA       Execute EXT041 (ECOM_PROD_STO File) when assortiment is complet
 * 20240412     YVOYOU       Addition of delete EXT045,EXT046 and no line in calendar if sapr < pupr
 */
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

public class EXT040 extends ExtendM3Batch {
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
  private Integer currentDate
  private String inCustomer
  private String inCalendar
  private Integer inFormatTXT
  private Integer inFormatCSV
  private Integer inFormatXLSX
  private String formatTXT
  private String formatCSV
  private String formatXLSX
  private Integer inAllContacts
  private Integer inSchedule
  private String previousCalendar
  private String previousCalendarYearWeek
  private String previousCalendarSuffix
  private String previousCalendarNextSuffix
  private String sigma6
  private String sigma9
  private String sigma9_DirectDelivery
  private String sigma9_NoDirectDelivery
  private String sigma9_DirectDelivery_assortment
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
  private double currentCOFA
  private double savedCOFA
  public boolean gapFileExists
  private String UL
  private String EAN13
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
  private String DLGR
  private String DLC
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
  private String FBA
  private String DUN14
  private String minimumCommande
  private String parCombien
  private String nbCouche_palette
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
  private String deliveryMethodName
  private String temperature
  private double sumCNQT
  private String typeBox
  private String nombreComposantBox
  private int count_OASITN = 0
  private String fileJobNumber
  private boolean iscomplet = false

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
    LocalDateTime timeOfCreation = LocalDateTime.now()
    fileJobNumber = program.getJobNumber()
    jobNumber = program.getJobNumber() + timeOfCreation.format(DateTimeFormatter.ofPattern("yyMMdd")) + timeOfCreation.format(DateTimeFormatter.ofPattern("HHmmss"))
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
    //Output Type
    inFormatTXT = 0
    inFormatCSV = 0
    inFormatXLSX = 0
    DBAction query_EXT042_Type = database.table("EXT042").index("10").selection("EXASCD", "EXFLTX", "EXFLCS", "EXFLXL").build()
    DBContainer EXT042_Type = query_EXT042_Type.getContainer()
    EXT042_Type.set("EXCONO", currentCompany)
    EXT042_Type.set("EXCDNN", inCalendar)
    if(!query_EXT042_Type.readAll(EXT042_Type, 2, outData_EXT042_Type)){
    }



    LocalDateTime timeOfCreation = LocalDateTime.now()
    currentDate = timeOfCreation.format(DateTimeFormatter.ofPattern("yyyyMMdd")) as Integer

    creationDate = timeOfCreation.format(DateTimeFormatter.ofPattern("yyyyMMdd")) as String
    creationTime = timeOfCreation.format(DateTimeFormatter.ofPattern("HHmmss")) as String

    // Perform Job
    writeFormatFile()

    writeCustomerEmailFile()

    writeInternalEmailFile()

    writeEXT040_EXT041()

    writeCalendarFile()

    writeGapFile()

    writeAlertFile()

    writePriceControlFile()

    writeEndFile()
    if (iscomplet){
      executeEXT820MISubmitBatch(String.valueOf(currentCompany),"EXT041", program.getUser(),String.valueOf(inCustomer),String.valueOf(inCalendar),String.valueOf(inAllContacts))
    }
    deleteTemporaryFiles()

    deleteEXTJOB()
  }
  // Retrieve previous calendar name
  private void retrievePreviousCalendar () {
    previousCalendar = ""
    previousCalendarYearWeek = ""
    previousCalendarSuffix = ""
    previousCalendarNextSuffix = ""
    DBAction query = database.table("EXT042").index("10").selection("EXCDNN").reverse().build()
    DBContainer EXT042 = query.getContainer()
    EXT042.set("EXCONO", currentCompany)
    EXT042.set("EXCUNO", inCustomer)
    if(!query.readAll(EXT042, 2, 1, outData_EXT042)){}
    if(previousCalendar.trim() != "") {
      previousCalendarYearWeek = previousCalendar.substring(0,6)
      previousCalendarSuffix = previousCalendar.substring(6,9)
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
    DBAction query = database.table("OCUSMA").index("00").selection("OKCUNM", "OKPLTB", "OKLHCD", "OKMODL").build()
    DBContainer OCUSMA = query.getContainer()
    OCUSMA.set("OKCONO", currentCompany)
    OCUSMA.set("OKCUNO", inCustomer)
    if (query.read(OCUSMA)) {
      customerName = OCUSMA.get("OKCUNM")
      pltb = OCUSMA.get("OKPLTB")
      lhcd = OCUSMA.get("OKLHCD")
      modl = OCUSMA.get("OKMODL")
      if (modl.trim() != "") {
        DBAction query_CSYTAB = database.table("CSYTAB").index("00").selection("CTTX15").build()
        DBContainer CSYTAB = query_CSYTAB.getContainer()
        CSYTAB.set("CTCONO", currentCompany)
        CSYTAB.set("CTSTCO", "MODL")
        CSYTAB.set("CTSTKY", modl.trim())
        if(lhcd.trim() == "FR"){
          CSYTAB.set("CTLNCD", "FR")
        } else {
          CSYTAB.set("CTLNCD", "GB")
        }
        if (query_CSYTAB.read(CSYTAB)) {
          deliveryMethodName = CSYTAB.get("CTTX15")
        }
      }
    }
  }
  // Retrieve EXT010
  Closure<?> outData_EXT010 = { DBContainer EXT010 ->
    cmde = EXT010.get("EXCMDE")
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
      DBAction query = database.table("CIDVEN").index("00").selection ("IISUCL").build()
      DBContainer CIDVEN = query.getContainer()
      CIDVEN.set("IICONO", currentCompany)
      CIDVEN.set("IISUNO", suno)
      if (query.read(CIDVEN)) {
        String sucl = CIDVEN.get("IISUCL")
        if(sucl.trim() == "100" || sucl.trim() == "200") {
          DBAction query_CSYTAB = database.table("CSYTAB").index("00").selection("CTTX40").build()
          DBContainer CSYTAB = query_CSYTAB.getContainer()
          CSYTAB.set("CTCONO", currentCompany)
          CSYTAB.set("CTSTCO", "SUCL")
          CSYTAB.set("CTSTKY", CIDVEN.get("IISUCL"))
          if (query_CSYTAB.read(CSYTAB)) {
            fournisseur = CSYTAB.get("CTTX40")
          }
        }
        if(sucl.trim() == "200") {
          entrepot = sule
        }
      }
      DBAction query_CIDMAS = database.table("CIDMAS").index("00").selection("IDSUNM").build()
      DBContainer CIDMAS = query_CIDMAS.getContainer()
      CIDMAS.set("IDCONO", currentCompany)
      CIDMAS.set("IDSUNO",  suno)
      if (query_CIDMAS.read(CIDMAS)) {
        nomFournisseur = CIDMAS.getString("IDSUNM")
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
  // Retrieve outData_EXT042_Type
  Closure<?> outData_EXT042_Type = { DBContainer EXT042_Type ->
    inFormatTXT = EXT042_Type.get("EXFLTX")
    inFormatCSV = EXT042_Type.get("EXFLCS")
    inFormatXLSX = EXT042_Type.get("EXFLXL")
  }
  // Retrieve EXT040
  Closure<?> outData_EXT040_2 = { DBContainer EXT040 ->
    foundSigma6 = true
  }
  // Retrieve EXT041
  Closure<?> outData_EXT041 = { DBContainer EXT041 ->
    previousCalendar = EXT041.get("EXCDNN")
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
    writeCalendar()
  }
  // Retrieve EXT041
  Closure<?> outData_EXT041_3 = { DBContainer EXT041 ->
    sigma6 = EXT041.get("EXPOPN")
    libelleSigma6 = EXT041.get("EXFUDS")
    foundSigma6 = false
    DBAction query_EXT040 = database.table("EXT040").index("00").selection("EXITNO", "EXASCD").build()
    DBContainer EXT040 = query_EXT040.getContainer()
    EXT040.set("EXCONO", currentCompany)
    EXT040.set("EXCUNO", inCustomer)
    EXT040.set("EXCDNN", inCalendar)
    EXT040.set("EXPOPN", sigma6)
    if(!query_EXT040.readAll(EXT040, 4, outData_EXT040_2)){}
    if(!foundSigma6) {
      line = sigma6+";"+libelleSigma6+";"+"1"
      countLines++
      lines += line + (countLines < 5000 ? "\r\n" : "")
      if(countLines == 5000) {
        logMessage("", lines)
        countLines = 0
        lines = ""
      }
    }
  }
  // Retrieve EXT042
  Closure<?> outData_EXT042 = { DBContainer EXT042 ->
    assortment = EXT042.get("EXASCD")
    DBAction assortmentQuery = database.table("OASITN").index("00").selection("OIITNO", "OIASCD").build()
    DBContainer OASITN = assortmentQuery.getContainer()
    OASITN.set("OICONO", currentCompany)
    OASITN.set("OIASCD", EXT042.get("EXASCD"))
    if(!assortmentQuery.readAll(OASITN, 2, outData_OASITN)){}
  }
  // Retrieve EXT043
  Closure<?> outData_EXT043 = { DBContainer EXT043 ->
    line = EXT043.get("EXEMAL")
    countLines++
    lines += line + (countLines < 5000 ? "\r\n" : "")
    if(countLines == 5000) {
      logMessage("", lines)
      countLines = 0
      lines = ""
    }
  }
  // Retrieve CCUCON
  Closure<?> outData_CCUCON = { DBContainer CCUCON ->
    line = CCUCON.get("CCEMAL")
    countLines++
    lines += line + (countLines < 5000 ? "\r\n" : "")
    if(countLines == 5000) {
      logMessage("", lines)
      countLines = 0
      lines = ""
    }
  }
  // Retrieve EXT044
  Closure<?> outData_EXT044 = { DBContainer EXT044 ->
    line = EXT044.get("EXEMAL")
    countLines++
    lines += line + (countLines < 5000 ? "\r\n" : "")
    if(countLines == 5000) {
      logMessage("", lines)
      countLines = 0
      lines = ""
    }
  }
  // Retrieve EXT045
  Closure<?> outData_EXT045 = { DBContainer EXT045 ->
    sigma9 = EXT045.get("EXITNO")
    libelleArticle = ""
    DBAction query_MITMAS = database.table("MITMAS").index("00").selection("MMFUDS").build()
    DBContainer MITMAS = query_MITMAS.getContainer()
    MITMAS.set("MMCONO", currentCompany)
    MITMAS.set("MMITNO", sigma9)
    if(query_MITMAS.read(MITMAS)){
      libelleArticle = MITMAS.get("MMFUDS")
    }
    if(!lhcd.trim().equals("FR")) {
      DBAction query_MITLAD = database.table("MITLAD").index("00").selection("MDFUDS").build()
      DBContainer MITLAD = query_MITLAD.getContainer()
      MITLAD.set("MDCONO", currentCompany)
      MITLAD.set("MDITNO", sigma9)
      MITLAD.set("MDLNCD", "GB")
      if(query_MITLAD.read(MITLAD)){
        libelleArticle = MITLAD.get("MDFUDS")
      }
    }
    line = sigma9+";"+libelleArticle+";"+"1"
    countLines++
    lines += line + (countLines < 5000 ? "\r\n" : "")
    if(countLines == 5000) {
      logMessage("", lines)
      countLines = 0
      lines = ""
    }
  }
  // Retrieve EXT046
  Closure<?> outData_EXT046 = { DBContainer EXT046 ->
    sigma9 = EXT046.get("EXITNO")
    sapr = EXT046.get("EXSAPR")
    appr = EXT046.get("EXAPPR")
    libelleArticle = ""

    DBAction query_MITMAS = database.table("MITMAS").index("00").selection("MMFUDS").build()
    DBContainer MITMAS = query_MITMAS.getContainer()
    MITMAS.set("MMCONO", currentCompany)
    MITMAS.set("MMITNO", sigma9)
    if(query_MITMAS.read(MITMAS)){
      libelleArticle = MITMAS.get("MMFUDS")
    }
    if(lhcd.trim() != "FR") {
      DBAction query_MITLAD = database.table("MITLAD").index("00").selection("MDFUDS").build()
      DBContainer MITLAD = query_MITLAD.getContainer()
      MITLAD.set("MDCONO", currentCompany)
      MITLAD.set("MDITNO", sigma9)
      MITLAD.set("MDLNCD", "GB")
      if(query_MITLAD.read(MITLAD)){
        libelleArticle = MITLAD.get("MDFUDS")
      }
    }
    line = sigma9+";"+libelleArticle+";"+(sapr as String)+";"+(appr as String)
    lines += line + "\r\n"
    countLines++
    if(countLines == 5000) {
      logMessage("", lines)
      countLines = 0
      lines = ""
    }
  }

  // Retrieve OASCUS
  Closure<?> outData_OASITN = { DBContainer OASITN ->
    sigma9 = OASITN.get("OIITNO")
    assortment = OASITN.get("OIASCD")
    iscomplet = assortment.substring(assortment.length() - 1, assortment.length()) == "0"
    if(itemIsOK()) {
      retrieveSigma6()
      retrieveSalesPrice()
      writeEXT040()
      writeEXT041()
    }
  }
  // Retrieve MITPOP
  Closure<?> outData_MITPOP = { DBContainer MITPOP ->
    sigma6 = MITPOP.get("MPPOPN")
  }
  // Retrieve sigma6
  public void retrieveSigma6() {
    sigma6 = ""
    ExpressionFactory expression_MITPOP = database.getExpressionFactory("MITPOP")
    expression_MITPOP = expression_MITPOP.eq("MPREMK", "SIGMA6")
    DBAction query_MITPOP = database.table("MITPOP").index("30").matching(expression_MITPOP).selection("MPPOPN").build()
    DBContainer MITPOP = query_MITPOP.getContainer()
    MITPOP.set("MPCONO", currentCompany)
    MITPOP.set("MPALWT", 1)
    MITPOP.set("MPITNO", sigma9)
    if (!query_MITPOP.readAll(MITPOP, 3, outData_MITPOP)) {}
  }
  // Retrieve sales price
  public void retrieveSalesPrice() {
    prixVente = "0"
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
  // write EXT040 and EXT041
  public void writeEXT040_EXT041() {
    retrievePreviousCalendar()
    retrieveCustomerInformations()
    // Check customer assortment
    DBAction assortmentQuery = database.table("EXT042").index("00").selection("EXASCD").build()
    DBContainer EXT042 = assortmentQuery.getContainer()
    EXT042.set("EXCONO", currentCompany)
    EXT042.set("EXCUNO", inCustomer)
    EXT042.set("EXCDNN", inCalendar)
    if(!assortmentQuery.readAll(EXT042, 3, outData_EXT042)){}
  }
  // write EXT040
  public void writeEXT040() {
    LocalDateTime timeOfCreation = LocalDateTime.now()
    DBAction query = database.table("EXT040").index("00").build()
    DBContainer EXT040 = query.getContainer()
    EXT040.set("EXCONO", currentCompany)
    EXT040.set("EXCDNN", inCalendar)
    EXT040.set("EXCUNO", inCustomer)
    EXT040.set("EXPOPN", sigma6)
    EXT040.set("EXITNO", sigma9)
    if (!query.read(EXT040)) {
      EXT040.set("EXASCD", assortment)
      EXT040.set("EXSAPR", prixVente as double)
      EXT040.setInt("EXRGDT", timeOfCreation.format(DateTimeFormatter.ofPattern("yyyyMMdd")) as Integer)
      EXT040.setInt("EXRGTM", timeOfCreation.format(DateTimeFormatter.ofPattern("HHmmss")) as Integer)
      EXT040.setInt("EXLMDT", timeOfCreation.format(DateTimeFormatter.ofPattern("yyyyMMdd")) as Integer)
      EXT040.setInt("EXCHNO", 1)
      EXT040.set("EXCHID", program.getUser())
      query.insert(EXT040)
    }
  }
  // write EXT041
  public void writeEXT041() {
    LocalDateTime timeOfCreation = LocalDateTime.now()
    DBAction queryEXT041 = database.table("EXT041").index("00").build()
    DBContainer EXT041 = queryEXT041.getContainer()
    EXT041.set("EXCONO", currentCompany)
    EXT041.set("EXCDNN", inCalendar)
    EXT041.set("EXCUNO", inCustomer)
    EXT041.set("EXPOPN", sigma6)
    if (!queryEXT041.read(EXT041)) {
      EXT041.set("EXFUDS", libelleArticle)
      EXT041.setInt("EXRGDT", timeOfCreation.format(DateTimeFormatter.ofPattern("yyyyMMdd")) as Integer)
      EXT041.setInt("EXRGTM", timeOfCreation.format(DateTimeFormatter.ofPattern("HHmmss")) as Integer)
      EXT041.setInt("EXLMDT", timeOfCreation.format(DateTimeFormatter.ofPattern("yyyyMMdd")) as Integer)
      EXT041.setInt("EXCHNO", 1)
      EXT041.set("EXCHID", program.getUser())
      queryEXT041.insert(EXT041)
    }
  }
  // Check if item must be selected
  public boolean itemIsOK() {
    stat = ""
    libelleArticle = ""
    DBAction query_MITMAS = database.table("MITMAS").index("00").selection("MMSTAT", "MMFUDS").build()
    DBContainer MITMAS = query_MITMAS.getContainer()
    MITMAS.set("MMCONO", currentCompany)
    MITMAS.set("MMITNO", sigma9)
    if(query_MITMAS.read(MITMAS)){
      stat = MITMAS.get("MMSTAT")
      libelleArticle = MITMAS.get("MMFUDS")
      if(lhcd.trim() != "FR") {
        DBAction query_MITLAD = database.table("MITLAD").index("00").selection("MDFUDS").build()
        DBContainer MITLAD = query_MITLAD.getContainer()
        MITLAD.set("MDCONO", currentCompany)
        MITLAD.set("MDITNO", sigma9)
        MITLAD.set("MDLNCD", "GB")
        if(query_MITLAD.read(MITLAD)){
          libelleArticle = MITLAD.get("MDFUDS")
        }
      }
    }

    chb7 = 0
    DBAction query_CUGEX1_MITMAS = database.table("CUGEX1").index("00").selection("F1CHB7").build()
    DBContainer CUGEX1_MITMAS = query_CUGEX1_MITMAS.getContainer()
    CUGEX1_MITMAS.set("F1CONO", currentCompany)
    CUGEX1_MITMAS.set("F1FILE",  "MITMAS")
    CUGEX1_MITMAS.set("F1PK01",  sigma9)
    CUGEX1_MITMAS.set("F1PK02",  "")
    CUGEX1_MITMAS.set("F1PK03",  "")
    CUGEX1_MITMAS.set("F1PK04",  "")
    CUGEX1_MITMAS.set("F1PK05",  "")
    CUGEX1_MITMAS.set("F1PK06",  "")
    CUGEX1_MITMAS.set("F1PK07",  "")
    CUGEX1_MITMAS.set("F1PK08",  "")
    if(query_CUGEX1_MITMAS.read(CUGEX1_MITMAS)){
      chb7 = CUGEX1_MITMAS.get("F1CHB7")
    }

    cmde = ""
    ExpressionFactory expression_EXT010 = database.getExpressionFactory("EXT010")
    expression_EXT010 = expression_EXT010.le("EXFVDT", currentDate as String).and(expression_EXT010.ge("EXLVDT", currentDate as String))
    DBAction query_EXT010 = database.table("EXT010").index("02").matching(expression_EXT010).selection("EXCMDE").build()
    DBContainer EXT010 = query_EXT010.getContainer()
    EXT010.set("EXCONO", currentCompany)
    EXT010.set("EXCUNO", inCustomer)
    EXT010.set("EXITNO", sigma9)
    if(query_EXT010.readAll(EXT010, 3, 1, outData_EXT010)){}

    if(stat == "20" && chb7 == 0 && cmde == "1") {
      count_OASITN++
      return true
    } else {
      return false
    }
  }
  // Write to format file
  public void writeFormatFile() {
    if (inSchedule == 1) {
      inFormatTXT = 0
      inFormatCSV = 0
      inFormatXLSX = 0
      DBAction query_CUGEX1 = database.table("CUGEX1").index("00").selection("F1N296", "F1N396", "F1N496").build()
      DBContainer CUGEX1 = query_CUGEX1.getContainer()
      CUGEX1.set("F1CONO", currentCompany)
      CUGEX1.set("F1FILE", "OCUSMA")
      CUGEX1.set("F1PK01", inCustomer)
      if (query_CUGEX1.read(CUGEX1)) {
        inFormatTXT = CUGEX1.get("F1N296") as Integer
        inFormatCSV = CUGEX1.get("F1N396") as Integer
        inFormatXLSX = CUGEX1.get("F1N496") as Integer
        logger.debug("Fin format schedule : "+inFormatTXT + "-" + inFormatCSV)
      }
    }
    logFileName = fileJobNumber + "-" + inCustomer + "-" + inCalendar + "-" + "formats.txt"
    header = "CSV"+";"+"TXT"+";"+"XLSX"
    logMessage(header, "")
    line = inFormatCSV+";"+inFormatTXT+";"+inFormatXLSX
    logMessage("", line)
  }
  // Write to customer email file
  public void writeCustomerEmailFile() {
    logFileName = fileJobNumber + "-" + inCustomer + "-" + inCalendar + "-" + "mailsClients.txt"
    header = "Mail client"
    logMessage(header, "")
    countLines = 0
    lines = ""

    if(inAllContacts == 1) {
      DBAction customerEmailQuery = database.table("CCUCON").index("10").selection("CCEMAL").build()
      DBContainer CCUCON = customerEmailQuery.getContainer()
      CCUCON.set("CCCONO", currentCompany)
      CCUCON.set("CCERTP", 1)
      CCUCON.set("CCEMRE", inCustomer)
      if(!customerEmailQuery.readAll(CCUCON, 3, outData_CCUCON)){}
    } else {
      DBAction customerEmailQuery = database.table("EXT043").index("00").selection("EXEMAL").build()
      DBContainer EXT043 = customerEmailQuery.getContainer()
      EXT043.set("EXCONO", currentCompany)
      EXT043.set("EXCUNO", inCustomer)
      EXT043.set("EXCDNN", inCalendar)
      if(!customerEmailQuery.readAll(EXT043, 3, outData_EXT043)){}
    }

    if(countLines > 0) {
      logMessage("", lines)
    }
  }
  // Write to internal email file
  public void writeInternalEmailFile() {
    logFileName = fileJobNumber + "-" +inCustomer + "-" + inCalendar + "-" + "mailsInternes.txt"
    header = "Mail interne"
    logMessage(header, "")
    countLines = 0
    lines = ""

    if (inSchedule == 1) {
      ExpressionFactory expression_CCUCON = database.getExpressionFactory("CCUCON")
      expression_CCUCON = expression_CCUCON.eq("CCRFTP", "I-COM" )
      DBAction customerEmailQuery = database.table("CCUCON").index("10").matching(expression_CCUCON).selection("CCEMAL").build()
      DBContainer CCUCON = customerEmailQuery.getContainer()
      CCUCON.set("CCCONO", currentCompany)
      CCUCON.set("CCERTP", 0)
      CCUCON.set("CCEMRE", inCustomer)
      if(!customerEmailQuery.readAll(CCUCON, 3, outData_CCUCON)){}
    } else {
      DBAction customerEmailQuery = database.table("EXT044").index("00").selection("EXEMAL").build()
      DBContainer EXT044 = customerEmailQuery.getContainer()
      EXT044.set("EXCONO", currentCompany)
      EXT044.set("EXCUNO", inCustomer)
      EXT044.set("EXCDNN", inCalendar)
      if (!customerEmailQuery.readAll(EXT044, 3, outData_EXT044)) {
      }
    }

    if(countLines > 0) {
      logMessage("", lines)
    }
  }
  // write EXT045
  public void writeEXT045() {
    LocalDateTime timeOfCreation = LocalDateTime.now()
    DBAction query = database.table("EXT045").index("00").build()
    DBContainer EXT045 = query.getContainer()
    EXT045.set("EXCONO", currentCompany)
    EXT045.set("EXCDNN", inCalendar)
    EXT045.set("EXCUNO", inCustomer)
    EXT045.set("EXPOPN", sigma6)
    EXT045.set("EXITNO", sigma9)
    if (!query.read(EXT045)) {
      EXT045.setInt("EXRGDT", timeOfCreation.format(DateTimeFormatter.ofPattern("yyyyMMdd")) as Integer)
      EXT045.setInt("EXRGTM", timeOfCreation.format(DateTimeFormatter.ofPattern("HHmmss")) as Integer)
      EXT045.setInt("EXLMDT", timeOfCreation.format(DateTimeFormatter.ofPattern("yyyyMMdd")) as Integer)
      EXT045.setInt("EXCHNO", 1)
      EXT045.set("EXCHID", program.getUser())
      query.insert(EXT045)
    }
  }
  // write EXT046
  public void writeEXT046() {
    LocalDateTime timeOfCreation = LocalDateTime.now()
    DBAction query = database.table("EXT046").index("00").build()
    DBContainer EXT046 = query.getContainer()
    EXT046.set("EXCONO", currentCompany)
    EXT046.set("EXCDNN", inCalendar)
    EXT046.set("EXCUNO", inCustomer)
    EXT046.set("EXPOPN", sigma6)
    EXT046.set("EXITNO", sigma9)
    if (!query.read(EXT046)) {
      EXT046.set("EXSAPR", sapr)
      EXT046.set("EXAPPR", appr)
      EXT046.setInt("EXRGDT", timeOfCreation.format(DateTimeFormatter.ofPattern("yyyyMMdd")) as Integer)
      EXT046.setInt("EXRGTM", timeOfCreation.format(DateTimeFormatter.ofPattern("HHmmss")) as Integer)
      EXT046.setInt("EXLMDT", timeOfCreation.format(DateTimeFormatter.ofPattern("yyyyMMdd")) as Integer)
      EXT046.setInt("EXCHNO", 1)
      EXT046.set("EXCHID", program.getUser())
      query.insert(EXT046)
    }
  }
  // Write to calendar file
  public void writeCalendarFile() {
    if(lhcd.trim() == "FR") {
      logFileName = fileJobNumber + "-" +inCustomer + "-" + inCalendar + "-" + "cadencier.txt"
      header = "Numéro cadencier"+";"+"Date création"+";"+"Heure création"+";"+"Code magasin"+";"+"Nom magasin"+";"+"Mode de livraison"
      logMessage(header, "")
      line = inCalendar+";"+creationDate+";"+creationTime+";"+inCustomer+";"+customerName+";"+deliveryMethodName
      logMessage("", line + "\r\n")
      header = "SIGMA6"+";"+"UL"+";"+"Code article"+";"+"EAN13"+";"+"Libellé article"+";"+"Code article remplacé"+";"+"Libellé article remplacé"+";"+"Prix cadencier précédent"+";"+"Prix de vente"+";"+"Evolution prix cadencier précédent (%)"+";"+"Unité prix de vente"+";"+"DLC_Totale"+";"+"DLGR"+";"+"Code marque"+";"+"Libellé code marque"+";"+"Code marketing"+";"+"Libellé code marketing"+";"+"Saisonnalité"+";"+"Nouveauté"+";"+"Langue packaging"+";"+"Code département "+";"+"Libellé département"+";"+"Code rayon "+";"+"Libellé rayon"+";"+"Code famille"+";"+"Libellé famille"+";"+"Code sous famille"+";"+"Libellé Sous famille"+";"+"Unité de besoin"+";"+"Libellé unité de besoin"+";"+"Fournisseur (CASINO ou industriel)"+";"+"Nom du fournisseur"+";"+"Fournisseur d'origine"+";"+"Nom du fournisseur d'origine"+";"+"Pays d'origine"+";"+"Code douanier"+";"+"Type Appro"+";"+"Entrepôt Casino"+";"+"Taille"+";"+"Couleur"+";"+"Modèle"+";"+"Diffusion"+";"+"FBA"+";"+"DUN14"+";"+"Minimum de commande"+";"+"PCB"+";"+"Nbre couche / Palette"+";"+"Nbre colis par couche"+";"+"Nbre colis par palette"+";"+"Nbre UVC par palette"+";"+"Contenance (exprimée en L pour l'UC)"+";"+"Poids net (exprimé en Kg pour l'UC)"+";"+"Poids brut (exprimé en Kg pour le colis)"+";"+"Volume colis (exprimé en m3)"+";"+"Hauteur UVC (exprimé en m)"+";"+"Largeur UVC (exprimé en m)"+";"+"Longueur UVC (exprimé en m)"+";"+"Hauteur colis (exprimé en m)"+";"+"Largeur colis (exprimé en m)"+";"+"Longueur colis (exprimé en m)"+";"+"Degré d'alcool"+";"+"Réf en droit"+";"+"Type de box"+";"+"Nbre de composant box"+";"+"Température"+";"+"Assortiment logistique"
    } else {
      logFileName = fileJobNumber + "-" +inCustomer + "-" + inCalendar + "-" + "schedule.txt"
      header = "Schedule number"+";"+"Creation date"+";"+"Creation time"+";"+"Store"+";"+"Store name"+";"+"Default delivery method"
      logMessage(header, "")
      line = inCalendar+";"+creationDate+";"+creationTime+";"+inCustomer+";"+customerName+";"+deliveryMethodName
      logMessage("", line + "\r\n")
      header = "SIGMA6"+";"+"UL"+";"+"Item code"+";"+"Barcode"+";"+"Item designation"+";"+"Replaced item code"+";"+"Replaced item designation"+";"+"Previous unit invoiced price"+";"+"Unit invoiced price"+";"+"Price evolution"+";"+"Selling unit"+";"+"Total DLC"+";"+"Warehouse shelf life"+";"+"Brand code"+";"+"Brand code name"+";"+"Marketing code"+";"+"Marketing code name"+";"+"Seasonality (P = Permanent S = Seasonality)"+";"+"New"+";"+"Pack language"+";"+"Department Code"+";"+"Department"+";"+"Section code"+";"+"Section"+";"+"Family code"+";"+"Family"+";"+"Sub Family code"+";"+"Sub family"+";"+"Unit code"+";"+"Unit"+";"+"Supplier"+";"+"Supplier name"+";"+"Manufacturer"+";"+"Manufacturer name"+";"+"Origin"+";"+"Custom code"+";"+"Supply type"+";"+"Warehouse"+";"+"Size"+";"+"Color"+";"+"Box type"+";"+"Diffusion"+";"+"FBA"+";"+"DUN14"+";"+"MOQ"+";"+"SKU/Case"+";"+"Layer/Pallet"+";"+"Case/Layer"+";"+"Case/Pallet"+";"+"SKU/Pallet"+";"+"Containing"+";"+"Net weight"+";"+"Gross weight"+";"+"Case volume"+";"+"Unit height"+";"+"Unit width"+";"+"Unit length"+";"+"Case height"+";"+"Case width"+";"+"Case length"+";"+"Alcohol degree"+";"+"Legal reference"+";"+"Box type"+";"+"Number of box components"+";"+"Temperature"+";"+"Logistic assortment"
    }
    logMessage(header, "")
    countLines = 0
    lines = ""

    // Read all POPNs in the current calendar
    DBAction query_EXT041 = database.table("EXT041").index("00").selection("EXPOPN").build()
    DBContainer EXT041 = query_EXT041.getContainer()
    EXT041.set("EXCONO", currentCompany)
    EXT041.set("EXCUNO", inCustomer)
    EXT041.set("EXCDNN", inCalendar)
    if(!query_EXT041.readAll(EXT041, 3, outData_EXT041_2)){}

    if(countLines > 0) {
      logMessage("", lines)
    }
  }
  // Write to calendar
  public void writeCalendar() {
    if(sigma9_DirectDelivery.trim() != "") {
      sigma9 = sigma9_DirectDelivery
      assortment = sigma9_DirectDelivery_assortment
      ExpressionFactory expression_EXT010 = database.getExpressionFactory("EXT010")
      expression_EXT010 = expression_EXT010.le("EXFVDT", currentDate as String).and(expression_EXT010.ge("EXLVDT", currentDate as String))
      DBAction query_EXT010 = database.table("EXT010").index("02").matching(expression_EXT010).selection("EXCMDE").build()
      DBContainer EXT010 = query_EXT010.getContainer()
      EXT010.set("EXCONO", currentCompany)
      EXT010.set("EXCUNO", inCustomer)
      EXT010.set("EXITNO", sigma9_DirectDelivery)
      if(query_EXT010.readAll(EXT010, 3, 1, outData_EXT010_2)){}
    }
    if(sigma9_NoDirectDelivery.trim() != "") {
      sigma9 = sigma9_NoDirectDelivery
      assortment = sigma9_NoDirectDelivery_assortment
      ExpressionFactory expression_EXT010 = database.getExpressionFactory("EXT010")
      expression_EXT010 = expression_EXT010.le("EXFVDT", currentDate as String).and(expression_EXT010.ge("EXLVDT", currentDate as String))
      DBAction query_EXT010 = database.table("EXT010").index("02").matching(expression_EXT010).selection("EXCMDE").build()
      DBContainer EXT010 = query_EXT010.getContainer()
      EXT010.set("EXCONO", currentCompany)
      EXT010.set("EXCUNO", inCustomer)
      EXT010.set("EXITNO", sigma9_NoDirectDelivery)
      if(query_EXT010.readAll(EXT010, 3, 1, outData_EXT010_2)){}
    }
  }
  // Write to gap file
  public void writeGapFile() {
    logFileName = fileJobNumber + "-" + inCustomer + "-" + inCalendar + "-" + "ecarts.txt"
    header = "Numéro cadencier"+";"+"Date création"+";"+"Heure création"+";"+"Code magasin"+";"+"Nom magasin"
    logMessage(header, "")
    line = inCalendar+";"+creationDate+";"+creationTime+";"+inCustomer+";"+customerName
    logMessage("", line + "\r\n")
    header = "SIGMA6"+";"+"Nom du SIGMA6"+";"+"Supprimé"
    logMessage(header, "")
    countLines = 0
    lines = ""

    // Read all POPNs in the previous calendar
    DBAction query_EXT041 = database.table("EXT041").index("00").selection("EXPOPN", "EXFUDS").build()
    DBContainer EXT041 = query_EXT041.getContainer()
    EXT041.set("EXCONO", currentCompany)
    EXT041.set("EXCUNO", inCustomer)
    EXT041.set("EXCDNN", previousCalendar)
    if(!query_EXT041.readAll(EXT041, 3, outData_EXT041_3)){}

    if(countLines > 0) {
      logMessage("", lines)
    }
  }
  // Write to alert file
  public void writeAlertFile() {
    logFileName = fileJobNumber + "-" +inCustomer + "-" + inCalendar + "-" + "alertes.txt"
    header = "Numéro cadencier"+";"+"Date création"+";"+"Heure création"+";"+"Code magasin"+";"+"Nom magasin"
    logMessage(header, "")
    line = inCalendar+";"+creationDate+";"+creationTime+";"+inCustomer+";"+customerName
    logMessage("", line + "\r\n")
    header = "Code article"+";"+"Nom article"+";"+"Prix à 0"
    logMessage(header, "")
    countLines = 0
    lines = ""

    DBAction customerEmailQuery = database.table("EXT045").index("00").selection("EXITNO").build()
    DBContainer EXT045 = customerEmailQuery.getContainer()
    EXT045.set("EXCONO", currentCompany)
    EXT045.set("EXCUNO", inCustomer)
    EXT045.set("EXCDNN", inCalendar)
    if(!customerEmailQuery.readAll(EXT045, 3, outData_EXT045)){}

    if(countLines > 0) {
      logMessage("", lines)
    }
  }
  // Write to price control file
  public void writePriceControlFile() {
    logFileName = fileJobNumber + "-" + inCustomer + "-" + inCalendar + "-" + "controlePrix.txt"
    header = "Numéro cadencier"+";"+"Date création"+";"+"Heure création"+";"+"Code magasin"+";"+"Nom magasin"
    logMessage(header, "")
    line = inCalendar+";"+creationDate+";"+creationTime+";"+inCustomer+";"+customerName
    logMessage("", line + "\r\n")
    header = "Code article"+";"+"Nom article"+";"+"Prix de vente"+";"+"Prix de revient"
    logMessage(header, "")
    countLines = 0
    lines = ""

    DBAction customerEmailQuery = database.table("EXT046").index("00").selection("EXITNO", "EXSAPR", "EXAPPR").build()
    DBContainer EXT046 = customerEmailQuery.getContainer()
    EXT046.set("EXCONO", currentCompany)
    EXT046.set("EXCUNO", inCustomer)
    EXT046.set("EXCDNN", inCalendar)
    if(!customerEmailQuery.readAll(EXT046, 3, outData_EXT046)){}

    if(countLines > 0) {
      logMessage("", lines)
    }
  }
  // Write the file indicating the end of processing
  public void writeEndFile() {
    logFileName = fileJobNumber + "-" + inCustomer + "-" + inCalendar + "-" + "docNumber.xml"
    docnumber = fileJobNumber + "-" + inCustomer + "-" + inCalendar
    header = "<?xml version=\"1.0\" encoding=\"UTF-8\"?><Document>  <DocumentType>CADENCIER</DocumentType>  <DocumentNumber>${docnumber}</DocumentNumber>  <DocumentPath>F:\\CadencierClient\\</DocumentPath></Document>"
    logMessage(header, "")
  }
  // Retrieve EXT010
  Closure<?> outData_EXT010_2 = { DBContainer EXT010 ->
    initLine()

    if(prixVente == "0")
      writeEXT045()

    sapr = prixVente as double
    appr = 0
    LocalDateTime timeOfCreation = LocalDateTime.now()
    DBAction query = database.table("MITFAC").index("00").selection("M9APPR").build()
    DBContainer MITFAC = query.getContainer()
    MITFAC.set("M9CONO", currentCompany)
    MITFAC.set("M9FACI", "E10")
    MITFAC.set("M9ITNO", sigma9)
    if (query.read(MITFAC)) {
      appr = MITFAC.get("M9APPR")
    }
    if(sapr < appr)
      writeEXT046()

    if(sapr >= appr) {
      line = sigma6.trim()+";"+UL.trim()+";"+sigma9.trim()+";"+EAN13.trim()+";"+libelleArticle.trim()+";"+codeArticleRemplace.trim()+";"+libelleArticleRemplace.trim()+";"+prixCadencierPrecedent.trim()+";"+prixVente.trim()+";"+evolutionPrixCadencierPrecedent.trim()+";"+unitePrixVente.trim()+";"+DLC.trim()+";"+DLGR.trim()+";"+codeMarque.trim()+";"+libelleCodeMarque.trim()+";"+codeMarketing.trim()+";"+libelleCodeMarketing.trim()+";"+saisonnalite.trim()+";"+nouveaute.trim()+";"+languePackaging.trim()+";"+codeDepartement.trim()+";"+nomDepartement.trim()+";"+codeRayon.trim()+";"+nomRayon.trim()+";"+codeFamille.trim()+";"+nomFamille.trim()+";"+codeSousFamille.trim()+";"+nomSousFamille.trim()+";"+uniteBesoin.trim()+";"+nomUniteBesoin.trim()+";"+fournisseur.trim()+";"+nomFournisseur.trim()+";"+fournisseurOrigine.trim()+";"+nomFournisseurOrigine.trim()+";"+nomPaysOrigine.trim()+";"+codeDouanier.trim()+";"+typeAppro.trim()+";"+entrepot.trim()+";"+taille.trim()+";"+couleur.trim()+";"+modele.trim()+";"+diffusion.trim()+";"+FBA.trim()+";"+DUN14.trim()+";"+minimumCommande.trim()+";"+parCombien.trim()+";"+nbCouche_palette.trim()+";"+nbColisParCouche.trim()+";"+nbColisParPalette.trim()+";"+nbUVCParPalette.trim()+";"+contenance.trim()+";"+poidsNet.trim()+";"+poidsBrut.trim()+";"+volumeColis.trim()+";"+hauteurUVC.trim()+";"+largeurUVC.trim()+";"+longueurUVC.trim()+";"+hauteurColis.trim()+";"+largeurColis.trim()+";"+longueurColis.trim()+";"+degreAlcool.trim()+";"+refDroit.trim()+";"+typeBox.trim()+";"+nombreComposantBox.trim()+";"+temperature.trim()+";"+assortimentLogistique.trim()
      //logMessage("", line)
      lines += line + "\r\n"
      countLines++
      if(countLines == 5000) {
        logMessage("", lines)
        countLines = 0
        lines = ""
      }
    }
  }
  // Init line informations
  public void initLine() {
    UL = ""
    libelleArticle = ""
    DLGR = ""
    DLC = ""
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
      spe2 = MITMAS.get("MMSPE2")

      if (spe2.trim() != "") {
        //
        DLGR = spe2
      }

      spe1 = MITMAS.get("MMSPE1")
      if (spe1.trim() != "") {
        //
        DLC = spe1
      }
      codeMarque = MITMAS.get("MMBUAR")
      if (codeMarque.trim() != "") {
        DBAction query_CSYTAB = database.table("CSYTAB").index("00").selection("CTTX40").build()
        DBContainer CSYTAB = query_CSYTAB.getContainer()
        CSYTAB.set("CTCONO", currentCompany)
        CSYTAB.set("CTSTCO", "BUAR")
        CSYTAB.set("CTSTKY", codeMarque)
        if (query_CSYTAB.read(CSYTAB)) {
          libelleCodeMarque = CSYTAB.get("CTTX40")
        }
      }
      codeMarketing = MITMAS.get("MMCFI1")
      if (codeMarketing.trim() != "") {
        DBAction query_CSYTAB = database.table("CSYTAB").index("00").selection("CTTX40").build()
        DBContainer CSYTAB = query_CSYTAB.getContainer()
        CSYTAB.set("CTCONO", currentCompany)
        CSYTAB.set("CTSTCO", "CFI1")
        CSYTAB.set("CTSTKY", codeMarketing)
        if (query_CSYTAB.read(CSYTAB)) {
          libelleCodeMarketing = CSYTAB.get("CTTX40")
        }
      }

      languePackaging = MITMAS.get("MMSPE3")

      codeDepartement = MITMAS.get("MMHIE1")
      DBAction query = database.table("MITHRY").index("00").selection("HITX40").build()
      DBContainer MITHRY = query.getContainer()
      MITHRY.set("HICONO",currentCompany)
      MITHRY.set("HIHLVL",  1)
      MITHRY.set("HIHIE0", codeDepartement)
      if (query.read(MITHRY)) {
        nomDepartement = MITHRY.get("HITX40")
      }
      hie2 = MITMAS.get("MMHIE2")
      if(hie2.trim() != "")
        codeRayon = (hie2 as String).substring((hie2.trim().length()-2),hie2.trim().length())
      MITHRY.set("HIHLVL",  2)
      MITHRY.set("HIHIE0", hie2)
      if (query.read(MITHRY)) {
        nomRayon = MITHRY.get("HITX40")
      }
      hie3 = MITMAS.get("MMHIE3")
      if(hie3.trim() != "")
        codeFamille = (hie3 as String).substring((hie3.trim().length()-3),hie3.trim().length())
      MITHRY.set("HIHLVL",  3)
      MITHRY.set("HIHIE0", hie3)
      if (query.read(MITHRY)) {
        nomFamille = MITHRY.get("HITX40")
      }

      hie4 = MITMAS.get("MMHIE4")
      if(hie4.trim() != "")
        codeSousFamille = (hie4 as String).substring((hie4.trim().length()-2),hie4.trim().length())
      MITHRY.set("HIHLVL",  4)
      MITHRY.set("HIHIE0", hie4)
      if (query.read(MITHRY)) {
        nomSousFamille = MITHRY.get("HITX40")
      }

      hie5 = MITMAS.get("MMHIE5")
      if(hie5.trim() != "")
        uniteBesoin = (hie5 as String).substring((hie5.trim().length()-2),hie5.trim().length())
      MITHRY.set("HIHLVL",  5)
      MITHRY.set("HIHIE0", hie5)
      if (query.read(MITHRY)) {
        nomUniteBesoin = MITHRY.get("HITX40")
        nomUniteBesoin = nomUniteBesoin.replaceAll(/[<>&"\\]/, " ")
      }
      fournisseurOrigine = MITMAS.get("MMSUNO")
      DBAction query_CIDMAS = database.table("CIDMAS").index("00").selection("IDSUNM").build()
      DBContainer CIDMAS = query_CIDMAS.getContainer()
      CIDMAS.set("IDCONO", currentCompany)
      CIDMAS.set("IDSUNO",  fournisseurOrigine)
      if (query_CIDMAS.read(CIDMAS)) {
        nomFournisseurOrigine = CIDMAS.getString("IDSUNM")
      }

      taille = MITMAS.get("MMDIM2")
      couleur = MITMAS.get("MMDIM1")
      modele = MITMAS.get("MMDIM3")
      contenance = MITMAS.get("MMSPGV")
      poidsNet = MITMAS.get("MMNEWE")
      hauteurUVC = MITMAS.get("MMIHEI")
      largeurUVC = MITMAS.get("MMIWID")
      longueurUVC = MITMAS.get("MMILEN")
      degreAlcool = MITMAS.get("MMCFI2")
      temperature = MITMAS.get("MMITCL")
      typeBox = MITMAS.get("MMDIM3")
    }

    typeAppro = ""
    refDroit = ""
    String A830 = ""
    int CHB1 = 0
    DBAction query_CUGEX1_MITMAS = database.table("CUGEX1").index("00").selection("F1A330", "F1A830", "F1CHB1").build()
    DBContainer CUGEX1_MITMAS = query_CUGEX1_MITMAS.getContainer()
    CUGEX1_MITMAS.set("F1CONO", currentCompany)
    CUGEX1_MITMAS.set("F1FILE",  "MITMAS")
    CUGEX1_MITMAS.set("F1PK01",  sigma9)
    CUGEX1_MITMAS.set("F1PK02",  "")
    CUGEX1_MITMAS.set("F1PK03",  "")
    CUGEX1_MITMAS.set("F1PK04",  "")
    CUGEX1_MITMAS.set("F1PK05",  "")
    CUGEX1_MITMAS.set("F1PK06",  "")
    CUGEX1_MITMAS.set("F1PK07",  "")
    CUGEX1_MITMAS.set("F1PK08",  "")
    if(query_CUGEX1_MITMAS.read(CUGEX1_MITMAS)){
      A830 = CUGEX1_MITMAS.get("F1A830")
      CHB1 = CUGEX1_MITMAS.get("F1CHB1")

      if(A830.trim()== "30" || A830.trim()== "40")
        typeAppro = "E"
      if(A830.trim()== "20")
        typeAppro = "D"
      if(A830.trim()== "10")
        typeAppro = "S"

      refDroit = CUGEX1_MITMAS.get("F1A330")
    }

    codeArticleRemplace = ""
    libelleArticleRemplace = ""
    DBAction query_MITALT = database.table("MITALT").index("40").selection("MAITNO", "MASTDT").build()
    DBContainer MITALT = query_MITALT.getContainer()
    MITALT.set("MACONO", currentCompany)
    MITALT.set("MAALIT", sigma9)
    MITALT.set("MARPTY", 3)
    if(query_MITALT.readAll(MITALT, 3, outData_MITALT)){
    }
    poidsBrut = ""
    volumeColis = ""
    hauteurColis = ""
    longueurColis = ""
    largeurColis = ""
    DBAction query_CUGEX1_MITAUN = database.table("CUGEX1").index("00").selection("F1N096","F1N196","F1N296","F1N396","F1N496").build()
    DBContainer CUGEX1_MITAUN = query_CUGEX1_MITAUN.getContainer()
    CUGEX1_MITAUN.set("F1CONO", currentCompany)
    CUGEX1_MITAUN.set("F1FILE",  "MITAUN")
    CUGEX1_MITAUN.set("F1PK01",  sigma9)
    CUGEX1_MITAUN.set("F1PK02",  "2")
    CUGEX1_MITAUN.set("F1PK03",  "COL")
    CUGEX1_MITAUN.set("F1PK04",  "")
    CUGEX1_MITAUN.set("F1PK05",  "")
    CUGEX1_MITAUN.set("F1PK06",  "")
    CUGEX1_MITAUN.set("F1PK07",  "")
    CUGEX1_MITAUN.set("F1PK08",  "")
    if(query_CUGEX1_MITAUN.read(CUGEX1_MITAUN)){
      poidsBrut = CUGEX1_MITAUN.get("F1N096")
      volumeColis = CUGEX1_MITAUN.get("F1N196")
      hauteurColis = CUGEX1_MITAUN.get("F1N296")
      largeurColis = CUGEX1_MITAUN.get("F1N496")
      longueurColis = CUGEX1_MITAUN.get("F1N396")
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


    popn = ""
    EAN13 = ""
    ExpressionFactory expression_MITPOP = database.getExpressionFactory("MITPOP")
    expression_MITPOP = expression_MITPOP.eq("MPREMK", "EA13")
    DBAction query_MITPOP = database.table("MITPOP").index("30").matching(expression_MITPOP).selection("MPPOPN").build()
    DBContainer MITPOP = query_MITPOP.getContainer()
    MITPOP.set("MPCONO", currentCompany)
    MITPOP.set("MPALWT", 1)
    MITPOP.set("MPITNO", sigma9)
    if (!query_MITPOP.readAll(MITPOP, 3, outData_MITPOP2)) {}
    EAN13 = popn

    popn = ""
    FBA = ""
    ExpressionFactory expression_MITPOP2 = database.getExpressionFactory("MITPOP")
    expression_MITPOP2 = expression_MITPOP2.eq("MPREMK", "FBA")
    DBAction query_MITPOP2 = database.table("MITPOP").index("30").matching(expression_MITPOP2).selection("MPPOPN").build()
    DBContainer MITPOP2 = query_MITPOP2.getContainer()
    MITPOP2.set("MPCONO", currentCompany)
    MITPOP2.set("MPALWT", 1)
    MITPOP2.set("MPITNO", sigma9)
    if (!query_MITPOP2.readAll(MITPOP2, 3, outData_MITPOP2)) {}
    FBA = popn

    popn = ""
    DUN14 = ""
    ExpressionFactory expression_MITPOP3 = database.getExpressionFactory("MITPOP")
    expression_MITPOP3 = expression_MITPOP3.eq("MPREMK", "DUN14")
    DBAction query_MITPOP3 = database.table("MITPOP").index("30").matching(expression_MITPOP3).selection("MPPOPN").build()
    DBContainer MITPOP3 = query_MITPOP3.getContainer()
    MITPOP3.set("MPCONO", currentCompany)
    MITPOP3.set("MPALWT", 1)
    MITPOP3.set("MPITNO", sigma9)
    if (!query_MITPOP3.readAll(MITPOP3, 3, outData_MITPOP2)) {}
    DUN14 = popn

    paysOrigine = ""
    nomPaysOrigine = ""
    codeDouanier = ""
    LocalDateTime timeOfCreation = LocalDateTime.now()
    DBAction query = database.table("MITFAC").index("00").selection("M9ORCO", "M9CSNO").build()
    DBContainer MITFAC = query.getContainer()
    MITFAC.set("M9CONO", currentCompany)
    MITFAC.set("M9FACI", "E10")
    MITFAC.set("M9ITNO", sigma9)
    if (query.read(MITFAC)) {
      paysOrigine = MITFAC.get("M9ORCO")
      if(paysOrigine.trim() != "") {
        DBAction query_CSYTAB = database.table("CSYTAB").index("00").selection("CTTX40").build()
        DBContainer CSYTAB = query_CSYTAB.getContainer()
        CSYTAB.set("CTCONO", currentCompany)
        CSYTAB.set("CTSTCO", "CSCD")
        CSYTAB.set("CTSTKY", paysOrigine)
        if (query_CSYTAB.read(CSYTAB)) {
          nomPaysOrigine = CSYTAB.get("CTTX40")
        }
      }

      codeDouanier = MITFAC.get("M9CSNO")
    }

    nombreComposantBox = ""
    sumCNQT = 0
    DBAction query_MPDMAT = database.table("MPDMAT").index("00").selection("PMCNQT").build()
    DBContainer MPDMAT = query_MPDMAT.getContainer()
    MPDMAT.set("PMCONO", currentCompany)
    MPDMAT.set("PMFACI", "E10")
    MPDMAT.set("PMPRNO", sigma9)
    MPDMAT.set("PMSTRT", "KIT")
    if(query_MPDMAT.readAll(MPDMAT, 4, outData_MPDMAT)){
    }
    nombreComposantBox = sumCNQT

    fournisseur = ""
    nomFournisseur = ""
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

    /*
    if(rscl.trim() == "999" ||
        rscl.trim() == "" ||
        (utility.call("NumberUtil","isValidInteger", rscl.trim()) && rscl.trim().length() == 2)) {
      saisonnalite = "Permanent"
    }
    if(rscl.trim() == "998") {
      saisonnalite = "Saisonnalite"
    }

    if(utility.call("NumberUtil","isValidInteger", rscl.trim()) && rscl.trim().length() == 2) {
      diffusion = rscl
    }
    */
    if(rscl.trim() == "999" ||
      rscl.trim() == "") {
      saisonnalite = "Permanent"
    }
    if(rscl.trim() == "998") {
      saisonnalite = "Saisonnalite"
    }

    if(rscl.trim().length() == 2) {
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

    minimumCommande = ""
    DBAction query_OCUSIT = database.table("OCUSIT").index("00").selection("ORD2QT").build()
    DBContainer OCUSIT = query_OCUSIT.getContainer()
    OCUSIT.set("ORCONO", currentCompany)
    OCUSIT.set("ORCUNO", "")
    OCUSIT.set("ORITNO", sigma9)
    if (query_OCUSIT.read(OCUSIT)) {
      minimumCommande = OCUSIT.get("ORD2QT")
    }

    nouveaute = ""
    prixCadencierPrecedent = "0"
    evolutionPrixCadencierPrecedent = ""

    DBAction query_EXT040 = database.table("EXT040").index("00").selection("EXSAPR").build()
    DBContainer EXT040 = query_EXT040.getContainer()
    EXT040.set("EXCONO", currentCompany)
    EXT040.set("EXCUNO", inCustomer)
    EXT040.set("EXCDNN", previousCalendar)
    EXT040.set("EXPOPN", sigma6)
    EXT040.set("EXITNO", sigma9)
    if(!query_EXT040.read(EXT040)){
      nouveaute = "N"
    } else {
      prixCadencierPrecedent = EXT040.get("EXSAPR") as String
    }

    if(prixVente != "0" && prixCadencierPrecedent != "0")
      evolutionPrixCadencierPrecedent = ((prixVente as double) - (prixCadencierPrecedent as double))/((prixCadencierPrecedent as double))*100

    if(prixCadencierPrecedent == "0" && prixVente != "0") {
      evolutionPrixCadencierPrecedent = "999"
    }
    if (prixVente =="0" && prixCadencierPrecedent != "0") {
      evolutionPrixCadencierPrecedent = "-100"
    }
  }


  // Execute EXT820MI.SubmitBatch
  private executeEXT820MISubmitBatch(String CONO, String JOID , String USID, String P001, String P002, String P003){
    def parameters = ["CONO": CONO, "JOID": JOID, "USID" : USID, "P001" : P001 , "P002" : P002, "P003" : P003 ,"EXVSN" : "1"]
    Closure<?> handler = { Map<String, String> response ->

      if (response.error != null) {
        return response.error
      }
    }
    miCaller.call("EXT820MI", "SubmitBatch", parameters, handler)
  }
  // Retrieve OPRBAS
  Closure<?> outData_OPRBAS = { DBContainer OPRBAS ->
    currentFromValidDate = OPRBAS.get("ODVFDT")
    if(currentFromValidDate > savedFromValidDate) {
      savedFromValidDate = OPRBAS.get("ODVFDT")
      prixVente = OPRBAS.get("ODSAPR")
      unitePrixVente = OPRBAS.get("ODSPUN")
    }
  }
  // Retrieve MITALT
  Closure<?> outData_MITALT = { DBContainer MITALT ->
    int stdt = MITALT.get("MASTDT")
    if(stdt <= currentDate) {
      codeArticleRemplace = MITALT.get("MAITNO")
      DBAction query_MITMAS = database.table("MITMAS").index("00").selection("MMFUDS").build()
      DBContainer MITMAS = query_MITMAS.getContainer()
      MITMAS.set("MMCONO", currentCompany)
      MITMAS.set("MMITNO", codeArticleRemplace)
      if(query_MITMAS.read(MITMAS)){
        libelleArticleRemplace = MITMAS.get("MMFUDS")
      }
      if(lhcd.trim() != "FR") {
        DBAction query_MITLAD = database.table("MITLAD").index("00").selection("MDFUDS").build()
        DBContainer MITLAD = query_MITLAD.getContainer()
        MITLAD.set("MDCONO", currentCompany)
        MITLAD.set("MDITNO", codeArticleRemplace)
        MITLAD.set("MDLNCD", "GB")
        if(query_MITLAD.read(MITLAD)){
          libelleArticleRemplace = MITLAD.get("MDFUDS")
        }
      }
    }
  }
  // Retrieve MPDMAT
  Closure<?> outData_MPDMAT = { DBContainer MPDMAT ->
    sumCNQT += MPDMAT.get("PMCNQT") as double
  }
  // Retrieve MITPOP
  Closure<?> outData_MITPOP2 = { DBContainer MITPOP ->
    popn = MITPOP.get("MPPOPN")
  }
  // Delete EXT040 and EXT041 records for previous calendar
  public void deleteTemporaryFiles(){
    ExpressionFactory expressionEXT040 = database.getExpressionFactory("EXT040")
    expressionEXT040 = expressionEXT040.ne("EXCDNN", inCalendar).and(expressionEXT040.ne("EXCDNN", previousCalendar))
    DBAction query_EXT040 = database.table("EXT040").index("00").matching(expressionEXT040).build()
    DBContainer EXT040 = query_EXT040.getContainer()
    EXT040.set("EXCONO", currentCompany)
    EXT040.set("EXCUNO", inCustomer)
    if(!query_EXT040.readAllLock(EXT040, 2, updateCallBack_EXT040)){
    }
    ExpressionFactory expressionEXT041 = database.getExpressionFactory("EXT041")
    expressionEXT041 = expressionEXT041.ne("EXCDNN", inCalendar).and(expressionEXT041.ne("EXCDNN", previousCalendar))
    DBAction query_EXT041 = database.table("EXT041").index("00").matching(expressionEXT041).build()
    DBContainer EXT041 = query_EXT041.getContainer()
    EXT041.set("EXCONO", currentCompany)
    EXT041.set("EXCUNO", inCustomer)
    //EXT041.set("EXCDNN", previousCalendar)
    if(!query_EXT041.readAllLock(EXT041, 2, updateCallBack_EXT041)){
    }

    ExpressionFactory expressionEXT042 = database.getExpressionFactory("EXT042")
    expressionEXT042 = expressionEXT042.ne("EXCDNN", inCalendar).and(expressionEXT042.ne("EXCDNN", previousCalendar))
    DBAction query_EXT042 = database.table("EXT042").index("00").matching(expressionEXT042).build()
    DBContainer EXT042 = query_EXT042.getContainer()
    EXT042.set("EXCONO", currentCompany)
    EXT042.set("EXCUNO", inCustomer)
    if(!query_EXT042.readAllLock(EXT042, 2, updateCallBack_EXT042)){
    }

    ExpressionFactory expressionEXT043 = database.getExpressionFactory("EXT043")
    expressionEXT043 = expressionEXT043.ne("EXCDNN", inCalendar).and(expressionEXT043.ne("EXCDNN", previousCalendar))
    DBAction query_EXT043 = database.table("EXT043").index("00").matching(expressionEXT043).build()
    DBContainer EXT043 = query_EXT043.getContainer()
    EXT043.set("EXCONO", currentCompany)
    EXT043.set("EXCUNO", inCustomer)
    if(!query_EXT043.readAllLock(EXT043, 2, updateCallBack_EXT043)){
    }

    ExpressionFactory expressionEXT044 = database.getExpressionFactory("EXT044")
    expressionEXT044 = expressionEXT044.ne("EXCDNN", inCalendar).and(expressionEXT044.ne("EXCDNN", previousCalendar))
    DBAction query_EXT044 = database.table("EXT044").index("00").matching(expressionEXT044).build()
    DBContainer EXT044 = query_EXT044.getContainer()
    EXT044.set("EXCONO", currentCompany)
    EXT044.set("EXCUNO", inCustomer)
    if(!query_EXT044.readAllLock(EXT044, 2, updateCallBack_EXT044)){
    }
    ExpressionFactory expressionEXT045 = database.getExpressionFactory("EXT045")
    expressionEXT045 = expressionEXT045.ne("EXCDNN", inCalendar).and(expressionEXT045.ne("EXCDNN", previousCalendar))
    DBAction query_EXT045 = database.table("EXT045").index("00").matching(expressionEXT045).build()
    DBContainer EXT045 = query_EXT045.getContainer()
    EXT045.set("EXCONO", currentCompany)
    EXT045.set("EXCUNO", inCustomer)
    if(!query_EXT045.readAllLock(EXT045, 2, updateCallBack_EXT045)){
    }

    ExpressionFactory expressionEXT046 = database.getExpressionFactory("EXT046")
    expressionEXT046 = expressionEXT046.ne("EXCDNN", inCalendar).and(expressionEXT046.ne("EXCDNN", previousCalendar))
    DBAction query_EXT046 = database.table("EXT046").index("00").matching(expressionEXT046).build()
    DBContainer EXT046 = query_EXT046.getContainer()
    EXT046.set("EXCONO", currentCompany)
    EXT046.set("EXCUNO", inCustomer)
    if(!query_EXT046.readAllLock(EXT046, 2, updateCallBack_EXT046)){
    }
  }
  // Delete EXT040
  Closure<?> updateCallBack_EXT040 = { LockedResult lockedResult ->
    lockedResult.delete()
  }
  // Delete EXT041
  Closure<?> updateCallBack_EXT041 = { LockedResult lockedResult ->
    lockedResult.delete()
  }
  // Delete EXT042
  Closure<?> updateCallBack_EXT042 = { LockedResult lockedResult ->
    lockedResult.delete()
  }
  // Delete EXT043
  Closure<?> updateCallBack_EXT043 = { LockedResult lockedResult ->
    lockedResult.delete()
  }
  // Delete EXT044
  Closure<?> updateCallBack_EXT044 = { LockedResult lockedResult ->
    lockedResult.delete()
  }
  // Delete EXT045
  Closure<?> updateCallBack_EXT045 = { LockedResult lockedResult ->
    lockedResult.delete()
  }
  // Delete EXT046
  Closure<?> updateCallBack_EXT046 = { LockedResult lockedResult ->
    lockedResult.delete()
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
  void logMessage(String header, String line) {
    textFiles.open("FileImport/CadencierClient")

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
}
