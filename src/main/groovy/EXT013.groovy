/**
 * README
 * This extension is used by Mashup
 *
 * Name : EXT013
 * Description : Generates sales order integration report
 * Date         Changed By  Version   Description
 * 20231129     SEAR        1.0       CMD08 - Rapport d'intégration de demande
 * 20240521     PBEAUDOUIN  1.1       Si pas trouvé dans alors lecture
 * 20250410     ARENARD     1.2       Extension has been fixed
 * 20250725     FLEBARS     1.3       Add automail process
 */
import java.math.RoundingMode
import java.time.DateTimeException
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

public class EXT013 extends ExtendM3Batch {
  private final LoggerAPI logger
  private final DatabaseAPI database
  private final ProgramAPI program
  private final BatchAPI batch
  private final MICallerAPI miCaller
  private final TextFilesAPI textFiles
  private final UtilityAPI utility
  private Integer currentCompany
  private String rawData
  private String logFileName
  private boolean IN60
  private boolean orderExist
  private String jobNumber
  private String creationDate
  private String creationTime
  private String inOrderNumber
  private String autoMailI
  private String autoMailE
  private String confOrderNumber
  private Integer currentDate
  private int rawDataLength
  private int beginIndex
  private int endIndex
  private String header
  private String blocline
  private String bloclines
  private String nonBlocline
  private String nonBloclines
  private String replacedBlocline
  private String replacedBloclines
  private String roundedline
  private String roundedlines
  private String rayonline
  private String rayonlines
  private String ssTotline
  private String ssTotlines
  private String totIntegre
  private String totIntegres
  private String totRejete
  private String totRejetes
  private String totline
  private String totlines
  private String lines
  private String mailLine
  private String mailLines
  private int countLines
  private String docnumber
  private boolean isConfOrder
  private String numMagasin
  private String numCommande
  private String numDemande
  private String dateIntegration
  private String modeTransport
  private String codeFournisseur
  private String datePosit
  private String rotation
  private String typeAppro
  private String temperature
  private String numTransitaire
  private String incoterm
  private String cuno
  private String cunm
  private String modl
  private String lncd
  private String whlo
  private String ortp
  private String uca4
  private String uca5
  private String uca6
  private String tedl
  private String fwno
  private String ccud
  private String cucd
  private Integer conn
  private Long dlix
  private Integer totLignesRecues
  private Integer totLignesintegrees
  private Integer totLignesRejeteesBloquantes
  private Integer totLignesErreursInformation
  private Integer totLignesSubstituees
  private String rayon
  private String librayon
  private Integer totligneRayon
  private Integer totSsligneError
  private double volTot
  private Integer totCol
  private Double totEquivPal
  private double totBrut
  private double totNet
  private double totVal
  private double volTotRej
  private Integer totColRej
  private Double totEquivPalRej
  private double totBrutRej
  private double totNetRej
  private double totValRej
  private Integer ponr
  private Integer posx
  private String itno
  private String itnonok
  private String titn
  private String fitn
  private double orqt
  private double lnam
  private double nepr
  private double udn6
  private String repi
  private String ean13
  private String itds
  private String itdsnok
  private String rscd
  private String rsc1
  private double grweItem
  private double neweItem
  private double vol3Item
  private double grwe
  private double newe
  private double vol3
  private double ntam
  private double pcb
  private String errorCode
  private String typeError
  private String commItem
  private String fileJobNumber
  private Map<String, String> rayonMap
  private Map<String, String> soustotMap
  private String server
  private String share
  private String path
  private Integer nbMaxRecord = 10000

  public EXT013(LoggerAPI logger, DatabaseAPI database, ProgramAPI program, BatchAPI batch, MICallerAPI miCaller, TextFilesAPI textFiles, UtilityAPI utility) {
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
    fileJobNumber = program.getJobNumber()
    if (batch.getReferenceId().isPresent()) {
      Optional<String> data = getJobData(batch.getReferenceId().get())
      performActualJob(data)
    }

  }
  /**
   * Perform the actual job
   * @param jobNumber
   * @return
   */
  private performActualJob(Optional<String> data) {
    if (!data.isPresent()) {
      return
    }
    rawData = data.get()
    inOrderNumber = getFirstParameter()
    autoMailI = getNextParameter()
    autoMailE = getNextParameter()

    LocalDateTime timeOfCreation = LocalDateTime.now()
    currentDate = timeOfCreation.format(DateTimeFormatter.ofPattern("yyyyMMdd")) as Integer

    creationDate = timeOfCreation.format(DateTimeFormatter.ofPattern("yyyyMMdd")) as String
    creationTime = timeOfCreation.format(DateTimeFormatter.ofPattern("HHmmss")) as String

    rayonMap = new LinkedHashMap<String, String>()
    soustotMap = new LinkedHashMap<String, String>()

    server = getCRS881("", "EXTENC", "1", "ExtendM3", "I", "Generic", "Server", "", "")
    path = getCRS881("", "EXTENC", "1", "ExtendM3", "I", "RapportIntegration", "Path", "", "")
    share = "\\\\${server}\\${path}\\"
    logger.debug("#PB share = " + share)

    logger.debug("#PB share = " + share)
    //clear var head
    confOrderNumber = ""
    numMagasin = ""
    numCommande = ""
    numDemande = ""
    dateIntegration = ""
    isConfOrder = false
    modeTransport = ""
    codeFournisseur = ""
    datePosit = ""
    rotation = ""
    typeAppro = ""
    temperature = ""
    numTransitaire = ""
    incoterm = ""
    cuno = ""
    cunm = ""
    modl = ""
    lncd = ""
    whlo = ""
    ortp = ""
    uca4 = ""
    uca5 = ""
    uca6 = ""
    tedl = ""
    orderExist = true
    // retrouver le mail du user
    mailLine = ""
    mailLines = ""

    // open directory
    textFiles.open(path)

    // get confirm Order if temp Order

    ExpressionFactory oxcntrExpression = database.getExpressionFactory("OXCNTR")
    oxcntrExpression = oxcntrExpression.eq("EVBQLY", "A")
    oxcntrExpression = oxcntrExpression.and(oxcntrExpression.eq("EVID01", "M3BOD"))
    oxcntrExpression = oxcntrExpression.and(oxcntrExpression.eq("EVSTAT", "90"))

    DBAction queryOXCNTR = database.table("OXCNTR").index("00")
      .matching(oxcntrExpression)
      .selection(
        "EVORNO",
        "EVORNR",
        "EVSTAT",
        "EVBQLY",
        "EVID01"
      ).build()
    DBContainer OXCNTR = queryOXCNTR.getContainer()
    OXCNTR.set("EVCONO", currentCompany)
    OXCNTR.set("EVORNO", inOrderNumber)

    // Retrieve OXCNTR
    boolean found = false
    Closure<?> outdataOxcntr = { DBContainer oxcntrResult ->
      String stat = oxcntrResult.get("EVSTAT")
      String bqly = oxcntrResult.get("EVBQLY")
      String id01 = oxcntrResult.getString("EVID01").trim()

      if ("90".equalsIgnoreCase(stat) && bqly == "A" && id01 == "M3BOD") {
        found = true
      }

      if ("90".equalsIgnoreCase(stat)) {
        confOrderNumber = oxcntrResult.get("EVORNR")
      }
      numDemande = oxcntrResult.get("EVORNR")
      if (!orderExist)
        inOrderNumber = oxcntrResult.getString("EVORNO")
    }

    if (!queryOXCNTR.readAll(OXCNTR, 2, nbMaxRecord, outdataOxcntr)) {
      orderExist = false
    }

    if (orderExist == false) {
      queryOXCNTR = database.table("OXCNTR")
        .matching(oxcntrExpression)
        .index("50")
        .selection(
          "EVORNO",
          "EVORNR",
          "EVSTAT",
          "EVBQLY",
          "EVID01").build()
      OXCNTR = queryOXCNTR.getContainer()
      OXCNTR.set("EVCONO", currentCompany)
      OXCNTR.set("EVORNR", inOrderNumber)
      if (!queryOXCNTR.readAll(OXCNTR, 2, 1, outdataOxcntr)) {
        orderExist = false
      }
    }
    logger.debug("found : ${found} - inOrderNumber : ${inOrderNumber} confOrderNumber : ${confOrderNumber}")
    if (!found)
      return

    // OXHEAD informations
    DBAction queryOXHEAD = database.table("OXHEAD").index("00").selection(
      "OAORNO",
      "OACUNO",
      "OACUOR",
      "OAORDT",
      "OAWHLO",
      "OAMODL",
      "OAORTP",
      "OATEDL",
      "OAVOL3",
      "OANEWE",
      "OAGRWE",
      "OANTAM",
      "OAUCA4",
      "OAUCA5",
      "OAUCA6",
      "OACUCD").build()
    DBContainer OXHEAD = queryOXHEAD.getContainer()
    OXHEAD.set("OACONO", currentCompany)
    OXHEAD.set("OAORNO", inOrderNumber)
    if (queryOXHEAD.read(OXHEAD)) {
      numMagasin = OXHEAD.get("OACUNO").toString().trim()
      numDemande = inOrderNumber
      numCommande = OXHEAD.get("OACUOR").toString().trim()
      dateIntegration = OXHEAD.get("OAORDT").toString().trim()
      cuno = OXHEAD.get("OACUNO").toString().trim()
      modl = OXHEAD.get("OAMODL").toString().trim()
      whlo = OXHEAD.get("OAWHLO").toString().trim()
      ortp = OXHEAD.get("OAORTP").toString().trim()
      tedl = OXHEAD.get("OATEDL").toString().trim()
      cucd = OXHEAD.get("OACUCD").toString().trim()
      newe = OXHEAD.getDouble("OANEWE")
      grwe = OXHEAD.getDouble("OAGRWE")
      vol3 = OXHEAD.getDouble("OAVOL3")
      executecrs610MIGetBasicData(String.valueOf(currentCompany), cuno)
    }

    //OOHEAD informations
    DBAction queryOOHEAD = database.table("OOHEAD").index("00").selection(
      "OAORNO",
      "OACUNO",
      "OACUOR",
      "OAORDT",
      "OAWHLO",
      "OAMODL",
      "OAORTP",
      "OATEDL",
      "OAVOL3",
      "OANEWE",
      "OAGRWE",
      "OANTAM",
      "OAUCA4",
      "OAUCA5",
      "OAUCA6").build()
    DBContainer OOHEAD = queryOOHEAD.getContainer()
    OOHEAD.set("OACONO", currentCompany)
    OOHEAD.set("OAORNO", confOrderNumber)
    if (queryOOHEAD.read(OOHEAD)) {
      isConfOrder = true
      numDemande = confOrderNumber
      numCommande = OOHEAD.get("OACUOR").toString().trim()
      dateIntegration = OOHEAD.get("OAORDT").toString().trim()
      numMagasin = OOHEAD.get("OACUNO").toString().trim()
      cuno = OOHEAD.get("OACUNO").toString().trim()
      modl = OOHEAD.get("OAMODL").toString().trim()
      whlo = OOHEAD.get("OAWHLO").toString().trim()
      ortp = OOHEAD.get("OAORTP").toString().trim()
      uca4 = OOHEAD.get("OAUCA4").toString().trim()
      uca5 = OOHEAD.get("OAUCA5").toString().trim()
      uca6 = OOHEAD.get("OAUCA6").toString().trim()
      tedl = OOHEAD.get("OATEDL").toString().trim()
      newe = OOHEAD.getDouble("OANEWE")
      grwe = OOHEAD.getDouble("OAGRWE")
      vol3 = OOHEAD.getDouble("OAVOL3")
      ntam = OOHEAD.getDouble("OANTAM")
      executecrs610MIGetBasicData(String.valueOf(currentCompany), cuno)
    }
    if (!"1".equals(autoMailI) && !"1".equals(autoMailE)) {
      getMailUser()
    } else {
      getMails(autoMailI, autoMailE)
    }
    getHeadData()

    getLinesData()

    boolean firstLine = true
    for (key in rayonMap.keySet()) {
      String value = rayonMap.get(key)
      String[] vt = value.split("#")
      String mapRayon = vt[0]
      String mapLibRayon = vt[1]
      String nbLines = vt[2]
      rayonline = mapRayon.trim() + ";" + mapLibRayon.trim() + ";" + nbLines.trim()
      if (firstLine) {
        firstLine = false
        rayonlines = rayonline
      } else {
        rayonlines = rayonlines += "\r\n" + rayonline
      }
    }

    boolean firstLineM = true
    for (key in soustotMap.keySet()) {
      String value = soustotMap.get(key)
      String[] vt = value.split("#")
      String mapSsTot = vt[0]
      String mapLibSsTot = vt[1]
      String nbLines = vt[2]
      ssTotline = mapSsTot.trim() + ";" + mapLibSsTot.trim() + ";" + nbLines.trim()
      if (firstLineM) {
        firstLineM = false
        ssTotlines = ssTotline
      } else {
        ssTotlines = ssTotlines + "\r\n" + ssTotline
      }
    }

    // récap des lignes
    totline = "Total Lignes Recues;" + totLignesRecues
    totlines = totlines += totline + "\r\n"
    totline = "Total Lignes Intégrées;" + totLignesintegrees
    totlines = totlines += totline + "\r\n"
    totline = "Total lignes rejetées bloquantes;" + totLignesRejeteesBloquantes
    totlines = totlines += totline + "\r\n"
    totline = "Total lignes erreur d'information;" + totLignesErreursInformation
    totlines = totlines += totline + "\r\n"
    totline = "Total lignes substituées;" + totLignesSubstituees
    totlines = totlines += totline

    if (ntam > 0) {
      totVal = ntam
    }

    // récap totaux intégrés
    double dVol3 = new BigDecimal(vol3).setScale(6, RoundingMode.HALF_EVEN).doubleValue()
    String sVol3 = String.format("%.6f", dVol3)
    double dGrwe = new BigDecimal(grwe).setScale(3, RoundingMode.HALF_EVEN).doubleValue()
    String sGrwe = String.format("%.3f", dGrwe)
    double dNewe = new BigDecimal(newe).setScale(3, RoundingMode.HALF_EVEN).doubleValue()
    String sNewe = String.format("%.3f", dNewe)
    double dTotVal = new BigDecimal(totVal).setScale(3, RoundingMode.HALF_EVEN).doubleValue()
    String sTotVal = String.format("%.3f", dTotVal)
    Double dtotEquivPal = totEquivPal
    String stotEquivPal = String.format("%.2f", dtotEquivPal)
    totIntegre = "Volume total;" + sVol3
    totIntegres = totIntegres += totIntegre + "\r\n"
    totIntegre = "Total Colis;" + totCol
    totIntegres = totIntegres += totIntegre + "\r\n"
    totIntegre = "Total palette reconstituée;" + stotEquivPal
    totIntegres = totIntegres += totIntegre + "\r\n"
    totIntegre = "Total poids brut;" + sGrwe
    totIntegres = totIntegres += totIntegre + "\r\n"
    totIntegre = "Total poids net;" + sNewe
    totIntegres = totIntegres += totIntegre + "\r\n"
    totIntegre = "Total Valeur;" + sTotVal + ";" + cucd
    totIntegres = totIntegres += totIntegre

    // récap totaux rejetés
    double dVolTotRej = new BigDecimal(volTotRej).setScale(6, RoundingMode.HALF_EVEN).doubleValue()
    String sVolTotRej = String.format("%.6f", dVolTotRej)
    double dTotBrutRej = new BigDecimal(totBrutRej).setScale(3, RoundingMode.HALF_EVEN).doubleValue()
    String sTotBrutRej = String.format("%.3f", dTotBrutRej)
    double dTotNetRej = new BigDecimal(totNetRej).setScale(3, RoundingMode.HALF_EVEN).doubleValue()
    String sTotNetRej = String.format("%.3f", dTotNetRej)
    double dTotValRej = new BigDecimal(totValRej).setScale(3, RoundingMode.HALF_EVEN).doubleValue()
    String sTotValRej = String.format("%.3f", dTotValRej)
    Double dTotEquivPalRej = totEquivPalRej
    totRejete = "Volume total;" + sVolTotRej
    totRejetes = totRejetes += totRejete + "\r\n"
    totRejete = "Total Colis;" + totColRej
    totRejetes = totRejetes += totRejete + "\r\n"
    totRejete = "Total Palette reconstituée;" + dTotEquivPalRej
    totRejetes = totRejetes += totRejete + "\r\n"
    totRejete = "Total poids brut;" + sTotBrutRej
    totRejetes = totRejetes += totRejete + "\r\n"
    totRejete = "Total poids net;" + sTotNetRej
    totRejetes = totRejetes += totRejete + "\r\n"
    totRejete = "Total Valeur;" + sTotValRej + ";" + cucd
    totRejetes = totRejetes += totRejete

    // Perform Job

    writeRapportHeaderFile()

    if (bloclines != "")
      writeBlokingAnomalyLineFile()

    if (nonBloclines != "")
      writeNonBlokingAnomalyLineFile()

    if (replacedBloclines != "")
      writeReplacedLineFile()

    if (roundedlines != "")
      writeRoundedLineFile()

    if (rayonlines != "")
      writeRayonLineFile()

    if (totlines != "")
      writeTotlinesFile()

    if (ssTotlines != "")
      writeSsTotlinesFile()

    if (totIntegres != "")
      writeTotIntegreFile()

    if (totRejetes != "")
      writeTotRejeteFile()

    writeEndFile()
    writeMailFile()

    deleteEXTJOB()
  }


  /**
   * Get lines data
   */
  public void getLinesData() {
    logger.debug("getLinesData")
    // clear var lines
    ponr = 0
    posx = 0
    itno = ""
    fitn = ""
    orqt = 0
    lnam = 0
    nepr = 0
    udn6 = 0
    repi = ""
    rscd = ""
    rsc1 = ""

    blocline = ""
    bloclines = ""
    nonBlocline = ""
    nonBloclines = ""
    replacedBlocline = ""
    replacedBloclines = ""
    roundedline = ""
    roundedlines = ""
    rayonline = ""
    rayonlines = ""
    totline = ""
    totlines = ""
    ssTotline = ""
    ssTotlines = ""
    totIntegre = ""
    totIntegres = ""
    totRejete = ""
    totRejetes = ""

    totLignesRecues = 0
    totLignesintegrees = 0
    totLignesRejeteesBloquantes = 0
    totLignesErreursInformation = 0
    totLignesSubstituees = 0

    totligneRayon = 0
    totSsligneError = 0
    volTot = 0
    totCol = 0
    totEquivPal = 0
    totBrut = 0
    totNet = 0
    totVal = 0
    volTotRej = 0
    totColRej = 0
    totEquivPalRej = 0
    totBrutRej = 0
    totNetRej = 0
    totValRej = 0

    // Retrieve CONN
    conn = 0
    dlix = 0
    Closure<?> outdataOxline = { DBContainer OXLINE ->
      ponr = OXLINE.getInt("OBPONR")
      posx = OXLINE.getInt("OBPOSX")
      itno = OXLINE.getString("OBITNO").trim()
      orqt = OXLINE.getDouble("OBORQA")
      lnam = OXLINE.getDouble("OBLNAM")
      udn6 = OXLINE.getDouble("OBUDN6")
      repi = OXLINE.getString("OBREPI").trim()
      nepr = OXLINE.getDouble("OBNEPR")
      rscd = OXLINE.getString("OBRSCD").trim()

      DBAction queryOOLINE = database.table("OOLINE").index("00").selection(
        "OBORNO",
        "OBPONR",
        "OBPOSX",
        "OBORQT",
        "OBNEPR",
        "OBRSCD",
        "OBRSC1").build()
      DBContainer OOLINE = queryOOLINE.getContainer()
      OOLINE.set("OBCONO", currentCompany)
      OOLINE.set("OBORNO", confOrderNumber)
      OOLINE.set("OBPONR", ponr)
      OOLINE.set("OBPOSX", posx)
      if (queryOOLINE.read(OOLINE)) {
        rscd = OOLINE.getString("OBRSCD").trim()
        rsc1 = OOLINE.getString("OBRSC1").trim()
        orqt = OOLINE.getDouble("OBORQT")
        nepr = OOLINE.getDouble("OBNEPR")
      }

      errorCode = ""
      typeError = ""
      commItem = ""
      rayon = ""
      librayon = ""
      pcb = 0
      ean13 = ""
      itds = ""
      itdsnok = ""
      itnonok = ""

      totLignesRecues++
      totLignesintegrees++
      logger.debug("outdataOxline ${totLignesintegrees} + ${totLignesRecues} + ${ponr}")
      errorCode = retrieveError(inOrderNumber, ponr, posx)
      errorCode = errorCode.trim()
      logger.debug("outdataOxline retrieveError return errorCode  ${errorCode}")
      if (errorCode == "") {
        errorCode = rscd
        if (rsc1 != "")
          errorCode = rsc1
      }
      if (errorCode == "")
        errorCode = "0"

      typeError = getErrorDescription(cuno, errorCode)

      Closure<?> outdatamitmasnok = { DBContainer mitmasnok ->
        logger.debug("#PB NOK SIGMA9 + "+ itnonok +" POUR "+itno)
        itnonok = mitmasnok.getString("MMITNO")
        itds = mitmasnok.getString("MMITDS")
        logger.debug("#PB NOK SIGMA9 + "+ itnonok +" POUR "+itno)

        ean13 = getEAN(itnonok)
      }

      if (itno.startsWith("NOK-S")){
        itno = fitn
        titn = fitn+"9999"
        ExpressionFactory expressionMitmasNok = database.getExpressionFactory("MITMAS")
        expressionMitmasNok = expressionMitmasNok.gt("MMITNO", itno)
        expressionMitmasNok = expressionMitmasNok.and(expressionMitmasNok.lt("MMITNO", titn))
        DBAction queryMitmas00nok = database.table("MITMAS").index("00").matching(expressionMitmasNok).selection(
          "MMITNO","MMITDS").build()

        DBContainer mitmasnok = queryMitmas00nok.getContainer()
        mitmasnok.set("MMCONO", currentCompany)
        logger.debug("#PB ITNO START WITH NOK"+ itno +" jusqua "+titn)

        if (!queryMitmas00nok.readAll(mitmasnok, 1, 1, outdatamitmasnok)) {
          logger.debug("#PB NOK SIGMA9 COMMENCE PAR "+ itno +" jusqua "+titn)
        }
      }else{
        ean13 = getEAN(itno)
        pcb = getItemPCB(itno)
      }






      executeOIS320MIGetPriceLine(cuno, itno, dateIntegration, "UVC", orqt, ortp)
      getItemInfos(itno)
      getLevelItemInfo(rayon)

      commItem = getItemDataFromCUGEX1MITMAS(itno)

      // map Rayon
      if (rayon != "") {
        totligneRayon = 1
        String key = rayon
        String value = rayon
        value += "#" + librayon
        value += "#" + String.valueOf(totligneRayon)
        if (!rayonMap.containsKey(key)) {
          rayonMap.put(key, value)
        } else {
          String[] ks = rayonMap.get(key).split("#")
          String keyRayon = ks[0]
          String keylibRayon = ks[1]
          totligneRayon = ks[2] as int
          totligneRayon++
          String valueMap = keyRayon
          valueMap += "#" + keylibRayon
          valueMap += "#" + String.valueOf(totligneRayon)
          rayonMap.put(keyRayon, valueMap)
        }
      }

      // todo parametre dans CRS881
      boolean blockingError = false
      int errorBloc = Integer.parseInt(errorCode)
      if (errorBloc != 0 && errorBloc != 49 && errorBloc != 50 && errorBloc != 51 && errorBloc != 52 && errorBloc != 53)
        blockingError = true

      // map repartition sous totaux par type d'erreurs
      if (errorCode != "0" && errorCode != "75" && blockingError) {
        totSsligneError = 1
        String key = errorCode
        String value = errorCode
        value += "#" + typeError
        value += "#" + String.valueOf(totSsligneError)
        if (!soustotMap.containsKey(key)) {
          soustotMap.put(key, value)
        } else {
          String[] ks = soustotMap.get(key).split("#")
          String keySsTot = ks[0]
          String keylibSsTot = ks[1]
          totSsligneError = ks[2] as int
          totSsligneError++
          String valueMap = keySsTot
          valueMap += "#" + keylibSsTot
          valueMap += "#" + String.valueOf(totSsligneError)
          soustotMap.put(keySsTot, valueMap)
        }
      }

      int IntOrqt = (int) Math.round(orqt)
      int IntUdn6 = (int) Math.round(udn6)
      int IntPcb = (int) Math.round(pcb)
      double volLine = new BigDecimal(orqt * vol3Item).setScale(6, RoundingMode.HALF_UP).doubleValue()
      String svolLine = String.format("%.6f", volLine)
      logger.debug("svolLine ${svolLine}")

      if (errorCode != "75" && blockingError) {
        double valeur = new BigDecimal(nepr * orqt).setScale(3, RoundingMode.HALF_UP).doubleValue()
        String svaleur = String.format("%.3f", valeur)
        double dequivPal = new BigDecimal(orqt / getItemCoef(itno, "UPA")).setScale(2, RoundingMode.HALF_UP).doubleValue()

        logger.debug("blocline ponr:${ponr} itno:${itno}")
        itno = itno.padRight(6)
        blocline = itno.substring(0, 6) + ";" + ean13 + ";" + itds + ";" + IntOrqt + ";" + "0" + ";" + svolLine + ";" + IntPcb + ";" + svaleur + ";" + typeError + ";" + commItem
        logger.debug("blocline svolLine ${svolLine}")
        bloclines += blocline + "\r\n"
        totLignesintegrees--
        totLignesRejeteesBloquantes++
        totEquivPalRej = totEquivPalRej + 0
        totValRej = totValRej + 0
        double equivCol = new BigDecimal(orqt / getItemCoef(itno, "COL")).setScale(2, RoundingMode.HALF_UP).doubleValue()
        int equivColArrondie = (int) Math.round(equivCol)
        totColRej = totColRej + 0
        double brutLine = orqt * grweItem
        double netLine = orqt * neweItem
        totBrutRej = totBrutRej + 0
        totNetRej = totNetRej + 0
        volTotRej = volTotRej + 0
      } else {
        double valeur = new BigDecimal(lnam).setScale(3, RoundingMode.HALF_UP).doubleValue()
        String svaleur = String.format("%.3f", valeur)
        double dequivPal = new BigDecimal(orqt / getItemCoef(itno, "UPA")).setScale(2, RoundingMode.HALF_EVEN).doubleValue()

        logger.debug("nonBlocline ponr:${ponr} itno:${itno} errorCode:${errorCode}")

        if (errorCode != "0" && errorCode != "75") {
          logger.debug("nonBlocline errorCode " + errorCode)
          nonBlocline = itno.substring(0, 6) + ";" + ean13 + ";" + itds + ";" + IntUdn6 + ";" + IntOrqt + ";" + svolLine + ";" + IntPcb + ";" + svaleur + ";" + String.format("%.2f", dequivPal) + ";" + typeError + ";" + commItem
          logger.debug("nonBlocline svolLine ${svolLine}")
          nonBloclines += nonBlocline + "\r\n"
          totLignesErreursInformation++
        }
        totEquivPal = totEquivPal + dequivPal
        totVal = totVal + valeur
        double equivCol = new BigDecimal(orqt / getItemCoef(itno, "COL")).setScale(2, RoundingMode.HALF_UP).doubleValue()
        int equivColArrondie = (int) Math.round(equivCol)
        totCol = totCol + equivColArrondie
      }

      if (repi != "") {
        double valeur = new BigDecimal(lnam).setScale(3, RoundingMode.HALF_UP).doubleValue()
        String svaleur = String.format("%.3f", valeur)
        double dequivPal = new BigDecimal(orqt / getItemCoef(itno, "UPA")).setScale(2, RoundingMode.HALF_EVEN).doubleValue()

        replacedBlocline = itno.substring(0, 6) + ";" + ean13 + ";" + itds + ";" + IntUdn6 + ";" + IntOrqt + ";" + svolLine + ";" + IntPcb + ";" + svaleur + ";" + typeError + ";" + commItem
        logger.debug("svolLine ${svolLine}")
        replacedBloclines += replacedBlocline + "\r\n"
        totLignesSubstituees++
        totEquivPal = totEquivPal + dequivPal
        double equivCol = new BigDecimal(orqt / getItemCoef(itno, "COL")).setScale(2, RoundingMode.HALF_EVEN).doubleValue()
        int equivColArrondie = (int) Math.round(equivCol)
        totCol = totCol + equivColArrondie
      }

      if (udn6 != orqt && udn6 != 0 && orqt != 0) {
        double qteArrondie = new BigDecimal((orqt / udn6) * 100).setScale(2, RoundingMode.HALF_EVEN).doubleValue()
        int pourcentageArrondie = (int) Math.round(qteArrondie)
        roundedline = itno.substring(0, 6) + ";" + ean13 + ";" + itds + ";" + IntUdn6 + ";" + IntOrqt + ";" + svolLine + ";" + +IntPcb + ";" + pourcentageArrondie
        logger.debug("roundedline svolLine ${svolLine}")
        roundedlines += roundedline + "\r\n"
      }

    }

    DBAction queryOXLINE = database.table("OXLINE").index("00").selection(
      "OBORNO",
      "OBPONR",
      "OBPOSX",
      "OBITNO",
      "OBORQA",
      "OBLNAM",
      "OBUDN6",
      "OBNEPR",
      "OBREPI",
      "OBRSCD").build()
    DBContainer OXLINE = queryOXLINE.getContainer()
    OXLINE.set("OBCONO", currentCompany)
    OXLINE.set("OBORNO", inOrderNumber)
    if (!queryOXLINE.readAll(OXLINE, 2, nbMaxRecord, outdataOxline)) {
    }

  }

  /**
   * Get header data
   */
  public void getHeadData() {

    typeAppro = ""
    temperature = ""
    codeFournisseur = ""

    DBAction querySupplierCUGEX1 = database.table("CUGEX1").index("00").selection("F1A030", "F1A130").build()
    DBContainer SupplierCUGEX1 = querySupplierCUGEX1.getContainer()
    SupplierCUGEX1.set("F1CONO", currentCompany)
    SupplierCUGEX1.set("F1FILE", "OOTYPE")
    SupplierCUGEX1.set("F1PK01", ortp)
    SupplierCUGEX1.set("F1PK02", "")
    SupplierCUGEX1.set("F1PK03", "")
    SupplierCUGEX1.set("F1PK04", "")
    SupplierCUGEX1.set("F1PK05", "")
    SupplierCUGEX1.set("F1PK06", "")
    SupplierCUGEX1.set("F1PK07", "")
    SupplierCUGEX1.set("F1PK08", "")
    if (querySupplierCUGEX1.read(SupplierCUGEX1)) {
      typeAppro = getTempCUGEVM("F1A030", SupplierCUGEX1.getString("F1A030").trim())
      temperature = getTempCUGEVM("F1A130", SupplierCUGEX1.getString("F1A130").trim())
      if ("20".equals(typeAppro)) codeFournisseur = whlo.trim()
    }

    modeTransport = getTransport(cuno, modl)
    rotation = uca4 + "-" + uca5 + "-" + uca6

    // Retrieve CONN
    conn = 0
    dlix = 0
    Closure<?> outdataMhdish = { DBContainer MHDISH ->
      conn = MHDISH.getInt("OQCONN")
      dlix = MHDISH.getLong("OQDLIX")
      return
    }

    ExpressionFactory expressionMHDISH = database.getExpressionFactory("MHDISH")
    expressionMHDISH = expressionMHDISH.eq("OQRIDN", numDemande)
    expressionMHDISH = expressionMHDISH.and(expressionMHDISH.eq("OQRORC", "3"))
    DBAction queryMHDISH = database.table("MHDISH").index("00").matching(expressionMHDISH).selection("OQCONN", "OQDLIX").build()
    DBContainer MHDISH = queryMHDISH.getContainer()
    MHDISH.set("OQCONO", currentCompany)
    MHDISH.set("OQINOU", 1)
    if (!queryMHDISH.readAll(MHDISH, 2, nbMaxRecord, outdataMhdish)) {
      return
    }

    //DCONSI informations
    fwno = ""
    DBAction queryDCONSI = database.table("DCONSI").index("00").selection("DAFWNO").build()
    DBContainer DCONSI = queryDCONSI.getContainer()
    DCONSI.set("DACONO", currentCompany)
    DCONSI.set("DACONN", conn)
    if (queryDCONSI.read(DCONSI)) {
      fwno = DCONSI.getString("DAFWNO").trim()
    }

    //DRADTR informations
    ccud = ""
    DBAction queryDRADTR = database.table("DRADTR").index("00").selection("DRFWNO").build()
    DBContainer DRADTR = queryDRADTR.getContainer()
    DRADTR.set("DRCONO", currentCompany)
    DRADTR.set("DRCONN", conn)
    DRADTR.set("DRTLVL", 2)
    DRADTR.set("DRDLIX", dlix)
    if (queryDRADTR.read(DRADTR)) {
      ccud = DRADTR.getString("DRFWNO").trim()
    }

    numTransitaire = fwno
    datePosit = ccud
    incoterm = tedl + " " + getIncoterm(cuno, tedl)
  }
  /**
   * Get transport mode
   * @param codeClient
   */
  public String getTransport(String codeClient, String TransportMode) {
    lncd = getcustomerLang(codeClient)
    return getTextCSYTAB("MODL", lncd, TransportMode)
  }

  /**
   * Get Incoterm
   * @param codeClient
   */
  public String getIncoterm(String codeClient, String Incoterm) {
    lncd = getcustomerLang(codeClient)
    return getDescriptionCSYTAB("TEDL", lncd, Incoterm)
  }

  /**
   * Get error description
   * @param codeClient
   */
  public String getErrorDescription(String codeClient, String errorCode) {
    lncd = getcustomerLang(codeClient)
    return getDescriptionCSYTAB("RSCD", lncd, errorCode)
  }

  /**
   * Get EAN
   * @param itno
   */
  public String getEAN(String ITNO) {
    String EAN13 = ""
    //init query on MITPOP
    ExpressionFactory expressionMITPOP = database.getExpressionFactory("MITPOP")
    expressionMITPOP = expressionMITPOP.eq("MPREMK", "EA13")
    DBAction queryMITPOP = database.table("MITPOP").index("00").matching(expressionMITPOP).selection("MPPOPN").build()
    DBContainer containerMITPOP = queryMITPOP.getContainer()
    containerMITPOP.set("MPCONO", currentCompany)
    containerMITPOP.set("MPALWT", 1)
    containerMITPOP.set("MPALWQ", "")
    containerMITPOP.set("MPITNO", ITNO)

    /**
     * Read MITPOP records
     */
    logger.debug("#PB GETean itno = "+ITNO  )
    Closure<?> readMITPOP = { DBContainer resultMITPOP ->
      EAN13 = resultMITPOP.getString("MPPOPN").trim()
      logger.debug("#PB GETean EAN + "+EAN13  )
    }
    queryMITPOP.readAll(containerMITPOP, 4, nbMaxRecord, readMITPOP)

    return EAN13
  }

  /**
   * Retrieve error
   * @param itno
   */
  public String retrieveError(String ORNO, Integer PONR, Integer POSX) {
    int saveMSCD = 0
    int mscd = 0
    String errorCODE = ""
    String mscdCODE = ""
    Closure<?> ext013Reader = { DBContainer ext013Result ->
      fitn = ext013Result.getString("EXFITN").trim()
      mscdCODE = ext013Result.getString("EXMSCD").trim()
      if (mscdCODE != "") {
        mscd = Integer.parseInt(mscdCODE)
      } else {
        mscd = 0
      }
      if (saveMSCD == 0 || mscd > saveMSCD) {
        saveMSCD = mscd
        errorCODE = ext013Result.getString("EXMSCD").trim()
      }
    }

    DBAction ext013Query = database.table("EXT013").index("00").selection(
      "EXORNO",
      "EXPONR",
      "EXFITN",
      "EXMSCD",
      "EXREMK").build()
    DBContainer ext013Request = ext013Query.getContainer()
    ext013Request.set("EXCONO", currentCompany)
    ext013Request.set("EXORNO", ORNO)
    ext013Request.set("EXPONR", PONR)
    ext013Request.set("EXPOSX", POSX)
    ext013Query.readAll(ext013Request, 4, nbMaxRecord, ext013Reader)
    return errorCODE
  }
  /**
   * Get Item Data from CUGEX1.MITMAS
   * @param itno
   */
  private String getItemDataFromCUGEX1MITMAS(String ITNO) {
    //Define return object structure
    String COMM = ""

    DBAction queryCUGEX100 = database.table("CUGEX1").index("00").selection("F1CONO",
      "F1FILE",
      "F1PK01",
      "F1PK02",
      "F1PK03",
      "F1PK04",
      "F1PK05",
      "F1PK06",
      "F1PK07",
      "F1PK08",
      "F1A121"
    ).build()

    DBContainer containerCUGEX1 = queryCUGEX100.getContainer()
    containerCUGEX1.set("F1CONO", currentCompany)
    containerCUGEX1.set("F1FILE", "MITMAS")
    containerCUGEX1.set("F1PK01", ITNO)

    if (queryCUGEX100.read(containerCUGEX1)) {
      COMM = containerCUGEX1.getString("F1A121").trim()
    }
    return COMM
  }

  /**
   * Get Item Data from CUGEX1.MITMAS
   * @param itno
   */
  private String getTempCUGEVM(String FLDI, String A130) {
    //Define return object structure
    String TX40 = ""
    DBAction queryCUGEVM = database.table("CUGEVM").index("00").selection("F3CONO",
      "F3FILE",
      "F3CUER",
      "F3FLDI",
      "F3AL30",
      "F3SEQN",
      "F3TX40"
    ).build()

    DBContainer containerCUGEVM = queryCUGEVM.getContainer()
    containerCUGEVM.set("F3CONO", currentCompany)
    containerCUGEVM.set("F3FILE", "OOTYPE")
    containerCUGEVM.set("F3FLDI", FLDI.trim())
    containerCUGEVM.set("F3CUER", "")
    containerCUGEVM.set("F3AL30", A130.trim())
    containerCUGEVM.set("F3SEQN", Integer.parseInt(A130.trim()))

    if (queryCUGEVM.read(containerCUGEVM)) {
      TX40 = containerCUGEVM.getString("F3TX40").trim()
    }
    return TX40
  }

  /**
   * Get Item Coefficient
   * @param itno
   */
  private Double getItemCoef(String ITNO, String ALUN) {

    double cofa = 1
    DBAction queryMITAUN = database.table("MITAUN").index("00").selection("MUCOFA").build()
    DBContainer MITAUN = queryMITAUN.getContainer()
    MITAUN.set("MUCONO", currentCompany)
    MITAUN.set("MUITNO", ITNO)
    MITAUN.set("MUAUTP", 1)
    MITAUN.set("MUALUN", ALUN)
    if (queryMITAUN.read(MITAUN)) {
      cofa = MITAUN.get("MUCOFA") as Double
    }

    return cofa
  }

  /**
   * Get text CSYTAB
   * @param codeClient
   */
  public String getTextCSYTAB(String constant, String lang, String key) {
    String TX15 = ""
    DBAction queryCSYTAB = database.table("CSYTAB").index("00").selection("CTTX15").build()
    DBContainer CSYTAB = queryCSYTAB.getContainer()
    CSYTAB.set("CTCONO", currentCompany)
    CSYTAB.set("CTLNCD", lang)
    CSYTAB.set("CTSTCO", constant)
    CSYTAB.set("CTSTKY", key)
    if (queryCSYTAB.read(CSYTAB)) {
      TX15 = CSYTAB.getString("CTTX15").trim()
    } else {
      DBAction queryCSYTAB1 = database.table("CSYTAB").index("00").selection("CTTX15").build()
      DBContainer CSYTAB1 = queryCSYTAB1.getContainer()
      CSYTAB1.set("CTCONO", currentCompany)
      CSYTAB1.set("CTLNCD", "")
      CSYTAB1.set("CTSTCO", constant)
      CSYTAB1.set("CTSTKY", key)
      if (queryCSYTAB1.read(CSYTAB1)) {
        TX15 = CSYTAB1.getString("CTTX15").trim()
      }
    }
    return TX15
  }

  /**
   * Get description CSYTAB
   * @param codeClient
   */
  public String getDescriptionCSYTAB(String constant, String lang, String key) {
    String CTPARM = ""
    DBAction queryCSYTAB = database.table("CSYTAB").index("00").selection("CTPARM", "CTTX40").build()
    DBContainer CSYTAB = queryCSYTAB.getContainer()
    CSYTAB.set("CTCONO", currentCompany)
    CSYTAB.set("CTLNCD", lang)
    CSYTAB.set("CTSTCO", constant)
    CSYTAB.set("CTSTKY", key)
    if (queryCSYTAB.read(CSYTAB)) {
      if ("TEDL".equalsIgnoreCase(constant)) {
        CTPARM = CSYTAB.getString("CTPARM").trim().substring(0, 36)
      } else {
        CTPARM = CSYTAB.getString("CTTX40").trim()
        logger.debug("found CSYTAB PARM : " + CSYTAB.getString("CTTX40").trim() + "item : " + itno + " line : " + ponr)
      }
    } else {
      DBAction queryCSYTAB1 = database.table("CSYTAB").index("00").selection("CTPARM", "CTTX40").build()
      DBContainer CSYTAB1 = queryCSYTAB1.getContainer()
      CSYTAB1.set("CTCONO", currentCompany)
      CSYTAB1.set("CTLNCD", "")
      CSYTAB1.set("CTSTCO", constant)
      CSYTAB1.set("CTSTKY", key)
      if (queryCSYTAB1.read(CSYTAB1)) {
        if ("TEDL".equalsIgnoreCase(constant)) {
          CTPARM = CSYTAB1.getString("CTPARM").trim().substring(0, 36)
        } else {
          CTPARM = CSYTAB1.getString("CTTX40").trim()
        }
      }
    }
    return CTPARM
  }

  /**
   * Get Item informations
   * @param itno
   */
  public void getItemInfos(String ITNO) {

    grweItem = 0
    neweItem = 0
    vol3Item = 0
    rayon = ""
    DBAction queryMITMAS = database.table("MITMAS").index("00").selection(
      "MMITDS",
      "MMGRWE",
      "MMVOL3",
      "MMNEWE",
      "MMHIE2").build()
    DBContainer MITMAS = queryMITMAS.getContainer()
    MITMAS.set("MMCONO", currentCompany)
    MITMAS.set("MMITNO", ITNO.trim())
    if (queryMITMAS.read(MITMAS)) {
      itds = MITMAS.getString("MMITDS").trim()
      grweItem = MITMAS.getDouble("MMGRWE")
      neweItem = MITMAS.getDouble("MMNEWE")
      vol3Item = MITMAS.getDouble("MMVOL3")
      rayon = MITMAS.getString("MMHIE2").trim()
    }

  }

  /**
   * Get Item level information
   * @param itno
   */
  public void getLevelItemInfo(String HIE2) {
    librayon = ""
    DBAction queryMITHRY = database.table("MITHRY").index("00").selection("HITX40").build()
    DBContainer MITHRY = queryMITHRY.getContainer()
    MITHRY.set("HICONO", currentCompany)
    MITHRY.set("HIHLVL", 2)
    MITHRY.set("HIHIE0", HIE2.trim())
    if (queryMITHRY.read(MITHRY)) {
      librayon = MITHRY.getString("HITX40").trim()
    }

  }

  /**
   * Get Item PCB
   * @param itno
   */
  public double getItemPCB(String ITNO) {
    double ItemPCB = 0
    DBAction queryMPACIT = database.table("MPACIT").index("00").selection("ECD1QT").build()
    DBContainer MPACIT = queryMPACIT.getContainer()
    MPACIT.set("ECCONO", currentCompany)
    MPACIT.set("ECWHLO", "")
    MPACIT.set("ECDXIT", 1)
    MPACIT.set("ECITNO", ITNO.trim())
    MPACIT.set("ECTEPA", "")
    MPACIT.set("ECCUNO", "")
    MPACIT.set("ECADID", "")
    MPACIT.set("ECTRQT", 9999999)
    if (queryMPACIT.read(MPACIT)) {
      ItemPCB = MPACIT.getDouble("ECD1QT")
    }
    return ItemPCB
  }

  /**
   * Get customer language
   * @param customer
   */
  public String getcustomerLang(String customer) {
    String customerLang = ""
    DBAction queryOCUSMA = database.table("OCUSMA").index("00").selection("OKLHCD").build()
    DBContainer OCUSMA = queryOCUSMA.getContainer()
    OCUSMA.set("OKCONO", currentCompany)
    OCUSMA.set("OKCUNO", customer.trim())
    if (queryOCUSMA.read(OCUSMA)) {
      customerLang = OCUSMA.getString("OKLHCD").trim()
    }
    return customerLang
  }

  /**
   * Execute OIS320MI GetPriceLine
   * @param CUNO
   * @param ITNO
   * @param ORDT
   * @param ALUN
   * @param ORQA
   * @param ORTP
   */
  private executeOIS320MIGetPriceLine(String CUNO, String ITNO, String ORDT, String ALUN, double ORQA, String ORTP) {
    Map<String, String> parameters = [
      "CUNO": CUNO,
      "ITNO": ITNO,
      "ORQA": "" + ORQA,
      "ALUN": ALUN,
      "ORDT": ORDT,
      "ORTP": ORTP]
    Closure<?> handler = { Map<String, String> response ->
      if (response.error != null) {
      } else {
        lnam = response.LNAM as double
        nepr = response.NETP as double
      }
    }


    miCaller.call("OIS320MI", "GetPriceLine", parameters, handler)
  }

  /**
   * Execute CRS610MI GetBasicData
   * @param CONO
   * @param CUNO
   */
  private executecrs610MIGetBasicData(String CONO, String CUNO) {
    Map<String, String> parameters = [
      "CONO": CONO,
      "CUNO": CUNO]
    Closure<?> handler = { Map<String, String> response ->
      if (response.error != null) {
      } else {
        cunm = response.CUNM.trim()
      }
    }
    miCaller.call("CRS610MI", "GetBasicData", parameters, handler)
  }

  private void getMails(String mailI, String mailE) {

    logger.debug("getMails mailI: " + mailI + " mailE: " + mailE)


    DBAction ccuconQuery = database.table("CCUCON")
      .index("10")
      .selection(
        "CCEMAL",
        "CCRFTP",
        "CCSTAT")
      .build()
    DBContainer ccuconRequest = ccuconQuery.getContainer()
    ccuconRequest.set("CCCONO", currentCompany)
    ccuconRequest.set("CCERTP", 0)
    ccuconRequest.set("CCEMRE", cuno)

    Closure<?> ccuconReader = { DBContainer ccuconResult ->
      String rftp = ccuconResult.getString("CCRFTP").trim()
      String emal = ccuconResult.getString("CCEMAL").trim()
      String stat = ccuconResult.getString("CCSTAT").trim()

      if ("20".equals(stat)){
        if ("1".equals(mailI) && ("I-ADV".equals(rftp) || "I-COM".equals(rftp))){
          mailLines += emal + "\r\n"
          logger.debug("mail " + emal)
        }
        if ("1".equals(mailE) && ("E-RDI".equals(rftp))){
          mailLines += emal + "\r\n"
          logger.debug("mail " + emal)
        }
      }
    }



    if (!ccuconQuery.readAll(ccuconRequest, 3, 100, ccuconReader)) {
    }
  }

  /**
   * Get mail user
   */
  private void getMailUser() {
    //Define return object structure
    DBAction queryCEMAIL = database.table("CEMAIL").index("00").selection("CBEMAL").build()
    DBContainer containerCEMAIL = queryCEMAIL.getContainer()
    containerCEMAIL.set("CBCONO", currentCompany)
    containerCEMAIL.set("CBEMTP", "04")
    containerCEMAIL.set("CBEMKY", program.getUser())

    Closure<?> readCEMAIL = { DBContainer resultCEMAIL ->
      mailLines = resultCEMAIL.getString("CBEMAL").trim()
    }
    queryCEMAIL.readAll(containerCEMAIL, 3, nbMaxRecord, readCEMAIL)
    logger.debug("user : " + program.getUser())
  }

  /**
   * Write bloking line file
   * @param header
   * @param message
   */
  public void writeBlokingAnomalyLineFile() {
    logFileName = fileJobNumber + "-" + confOrderNumber + "-" + "BlokingLines" + "-" + "rapport.txt"
    header = "Article" + ";" + "Code EAN" + ";" + "Libellé article" + ";" + "Quantité commandée" + ";" + "Quantité intégrée" + ";" + "Volume commandé" + ";" + "PCB" + ";" + "Valeur" + ";"+ "Type erreur" +";" + "Commentaire Article"
    logMessage(header, bloclines)
  }

  /**
   * Write non bloking Lines file
   * @param header
   * @param message
   */
  public void writeNonBlokingAnomalyLineFile() {
    logFileName = fileJobNumber + "-" + confOrderNumber + "-" + "NonBlokingLines" + "-" + "rapport.txt"
    header = "Article" + ";" + "Code EAN" + ";" + "Libellé article" + ";" + "Quantité commandée" + ";" + "Quantité intégrée" + ";" + "Volume commandé" + ";" + "PCB" + ";" + "Valeur" + ";" + "Palette reconstituée" + ";" + "Type erreur" + ";" + "Commentaire Article"
    logMessage(header, nonBloclines)
  }

  /**
   * Write replaced Lines file
   * @param header
   * @param message
   */
  public void writeReplacedLineFile() {
    logFileName = fileJobNumber + "-" + confOrderNumber + "-" + "ReplacedLine" + "-" + "rapport.txt"
    header = "Article" + ";" + "Code EAN" + ";" + "Libellé article" + ";" + "Quantité commandée" + ";" + "Quantité intégrée" + ";" + "Volume commandé" + ";" + "PCB" + ";" + "Valeur" + ";" + "Palette reconstituée" + ";" + "Type erreur" + ";" + "Commentaire Article"
    logMessage(header, replacedBloclines)
  }

  /**
   * Write rounded Lines file
   * @param header
   * @param message
   */
  public void writeRoundedLineFile() {
    logFileName = fileJobNumber + "-" + confOrderNumber + "-" + "RoundedLine" + "-" + "rapport.txt"
    header = "Article" + ";" + "Code EAN" + ";" + "Libellé article" + ";" + "Quantité commandée" + ";" + "Qté intégrée"+ ";" + "Volume intégré" + ";" + "PCB" + ";" + "taux arrondi %"
    logMessage(header, roundedlines)
  }

  /**
   * Write rayon Lines file
   * @param header
   * @param message
   */
  public void writeRayonLineFile() {
    logFileName = fileJobNumber + "-" + confOrderNumber + "-" + "RayonLine" + "-" + "rapport.txt"
    header = "Rayon" + ";" + "Libellé Rayon" + ";" + "Nb de Lignes"
    logMessage(header, rayonlines)
  }

  /**
   * Write sous total Lines file
   * @param header
   * @param message
   */
  public void writeTotlinesFile() {
    logFileName = fileJobNumber + "-" + confOrderNumber + "-" + "totlines" + "-" + "rapport.txt"
    header = "Sous total lignes par type d'erreur" + ";" + "Nb de Lignes"
    logMessage(header, totlines)
  }

  /**
   * Write sous total Lines file
   * @param header
   * @param message
   */
  public void writeSsTotlinesFile() {
    logFileName = fileJobNumber + "-" + confOrderNumber + "-" + "ssTotlines" + "-" + "rapport.txt"
    header = "Code Erreur" + ";" + "Libellé Erreur" + ";" + "Nb de Lignes"
    logMessage(header, ssTotlines)
  }

  /**
   * Write total lines file
   * @param header
   * @param message
   */
  public void writeTotIntegreFile() {
    logFileName = fileJobNumber + "-" + confOrderNumber + "-" + "TotIntegre" + "-" + "rapport.txt"
    header = "Total Integré" + ";" + "valeur"
    logMessage(header, totIntegres)
  }

  /**
   * Write total reject file
   * @param header
   * @param message
   */
  public void writeTotRejeteFile() {
    logFileName = fileJobNumber + "-" + confOrderNumber + "-" + "TotRejete" + "-" + "rapport.txt"
    header = "Total Rejeté" + ";" + "valeur"
    logMessage(header, totRejetes)
  }

  /**
   * Write rapport header file
   * @param header
   * @param message
   */
  public void writeRapportHeaderFile() {
    logFileName = fileJobNumber + "-" + confOrderNumber + "-" + "Header" + "-" + "rapport.txt"
    header = "N° Magasin" + ";" + "Mode de transport" + ";" + "N° Demande" + ";" + "N° commande" + ";" + "Date Intégration" + ";" + "Code Fournisseur" + ";" + "Date Pos" + ";" + "Rotation" + ";" + "Type d'approvisionnement" + ";" + "Température" + ";" + "N° Transitaire" + ";" + "Incoterm" + ";" + "Nom Client"
    logMessage(header, "")
    countLines = 0

    DateTimeFormatter inputFormatter = DateTimeFormatter.ofPattern("yyyyMMdd")
    DateTimeFormatter outputFormatter = DateTimeFormatter.ofPattern("dd/MM/yyyy")

    dateIntegration = convertDate(dateIntegration)
    datePosit = convertDate(datePosit)
    lines = numMagasin + ";" + modeTransport + ";" + numDemande + ";" + numCommande + ";" + dateIntegration + ";" + codeFournisseur + ";" + datePosit + ";" + rotation + ";" + typeAppro + ";" + temperature + ";" + numTransitaire + ";" + incoterm + ";" + cunm
    logMessage("", lines)
  }

  /**
   * Write mail file
   * @param header
   * @param message
   */
  public void writeMailFile() {
    logFileName = fileJobNumber + "-" + confOrderNumber + "-" + "MailFile.txt"
    header = "Adresses Mail"
    logMessage(header, mailLines)
  }

  /**
   * Write end file
   * @param header
   * @param message
   */
  public void writeEndFile() {
    logFileName = fileJobNumber + "-" + confOrderNumber + "-" + "docNumber.xml"
    docnumber = fileJobNumber + "-" + confOrderNumber

    header = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
    header += "<Document>"
    header += "<DocumentType>RAPPORTINTEGRATION</DocumentType>"
    header += "<DocumentNumber>${docnumber}</DocumentNumber>"
    header += "<DocumentPath>${share}</DocumentPath>"
    header += "</Document>"

    logger.debug("#PB Docnumber =" + header)
    logMessage(header, "")
  }

  /**
   * Write log message
   * @param header
   * @param message
   */
  void logMessage(String header, String line) {

    if (logFileName.endsWith("docNumber.xml"))

      logger.debug("line = " + line)
    if (header.trim() != "") {
      log(header)
    }
    if (line.trim() != "") {
      log(line)
    }
  }

  /**
   * Write log
   * @param message
   */
  void log(String message) {
    IN60 = true
    Closure<?> consumer = { PrintWriter printWriter ->
      printWriter.println(message)
    }
    if (logFileName.endsWith("docNumber.xml")) {
      textFiles.write(logFileName, "UTF-8", false, consumer)
    } else {
      textFiles.write(logFileName, "UTF-8", true, consumer)
    }
  }
  /**
   * Get first parameter
   */
  private String getFirstParameter() {
    rawDataLength = rawData.length()
    beginIndex = 0
    endIndex = rawData.indexOf(";")
    // Get parameter
    String parameter = rawData.substring(beginIndex, endIndex)
    return parameter
  }
  /**
   * Convert date from yyyyMMdd to dd/MM/yyyy
   * @param inputDate
   * @return
   */
  public String convertDate(String inputDate) {
    DateTimeFormatter inputFormatter = DateTimeFormatter.ofPattern("yyyyMMdd")
    DateTimeFormatter outputFormatter = DateTimeFormatter.ofPattern("dd/MM/yyyy")
    try {
      LocalDate date = LocalDate.parse(inputDate, inputFormatter)
      return date.format(outputFormatter)
    } catch (DateTimeException e) {
      return ""
    }

  }
  /**
   * Get next parameter
   */
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

  /**
   * Delete the job data from EXTJOB table
   * @param referenceId
   */
  public void deleteEXTJOB() {
    LocalDateTime timeOfCreation = LocalDateTime.now()
    DBAction query = database.table("EXTJOB").index("00").build()
    DBContainer EXTJOB = query.getContainer()
    EXTJOB.set("EXRFID", batch.getReferenceId().get())
    if (!query.readLock(EXTJOB, updatecallbackExtjob)) {
    }
  }

  /**
   * Callback to delete the job data from EXTJOB table
   * @param lockedResult
   */
  Closure<?> updatecallbackExtjob = { LockedResult lockedResult ->
    lockedResult.delete()
  }

  /**
   * Get the job data from EXTJOB table
   * @param referenceId
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
