/**
 * README
 * This extension is used by Mashup
 *
 * Name : EXT013
 * Description : Generates sales order integration report
 * Date         Changed By   Description
 * 20231129     SEAR         CMD008 - Rapport d'intégration de demande
 */

import java.math.RoundingMode
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.concurrent.TimeUnit
import java.util.Calendar
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
  private String jobNumber
  private String creationDate
  private String creationTime
  private String inOrderNumber
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
  private String MailLine
  private String MailLines
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
  private Integer ordt
  private Integer totLignesRecues
  private Integer totLignesintegrees
  private Integer totLignesRejeteesBloquantes
  private Integer totLignesErreursInformation
  private Integer totLignesSubstituees
  private String rayon
  private String librayon
  private Integer totligneRayon
  private Integer totSsligneError
  private double VolTot
  private Integer totCol
  private double totEquivPal
  private double totBrut
  private double totNet
  private double totVal
  private double VolTotRej
  private Integer totColRej
  private double totEquivPalRej
  private double totBrutRej
  private double totNetRej
  private double totValRej
  private Integer ponr
  private Integer posx
  private String itno
  private String fitn
  private double orqt
  private double lnam
  private double nepr
  private double udn6
  private String repi
  private String ean13
  private String itds
  private String rscd
  private String rsc1
  private double cofa
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
    LocalDateTime timeOfCreation = LocalDateTime.now()
    jobNumber = program.getJobNumber() + timeOfCreation.format(DateTimeFormatter.ofPattern("yyMMdd")) + timeOfCreation.format(DateTimeFormatter.ofPattern("HHmmss"))
    fileJobNumber = program.getJobNumber()
    if (batch.getReferenceId().isPresent()) {
      Optional<String> data = getJobData(batch.getReferenceId().get())
      performActualJob(data)
    } else {
      // No job data found
    }

  }

  // Perform actual job
  private performActualJob(Optional<String> data) {
    if (!data.isPresent()) {
      //logger.debug("Job reference Id ${batch.getReferenceId().get()} is passed but data was not found")
      return
    }
    rawData = data.get()
    //logger.debug("Début performActualJob")
    inOrderNumber = getFirstParameter()

    currentCompany = (Integer) program.getLDAZD().CONO

    LocalDateTime timeOfCreation = LocalDateTime.now()
    currentDate = timeOfCreation.format(DateTimeFormatter.ofPattern("yyyyMMdd")) as Integer

    creationDate = timeOfCreation.format(DateTimeFormatter.ofPattern("yyyyMMdd")) as String
    creationTime = timeOfCreation.format(DateTimeFormatter.ofPattern("HHmmss")) as String

    rayonMap = new LinkedHashMap<String, String>()
    soustotMap = new LinkedHashMap<String, String>()

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
    modl = ""
    lncd = ""
    whlo = ""
    ortp = ""
    uca4 = ""
    uca5 = ""
    uca6 = ""
    tedl = ""

    // retrouver le mail du user
    MailLine = ""
    MailLines = ""
    getMailUser()

    // open directory
    textFiles.open("FileImport/RapportIntegration")

    // get confirm Order if temp Order
    DBAction queryOXCNTR = database.table("OXCNTR").index("00").selection("EVORNO", "EVORNR", "EVSTAT").build()
    DBContainer OXCNTR = queryOXCNTR.getContainer()
    OXCNTR.set("EVCONO", currentCompany)
    OXCNTR.set("EVORNO", inOrderNumber)
    if (!queryOXCNTR.readAll(OXCNTR, 2, outData_OXCNTR)) {
      logger.debug("pas de commande trouvée")
    }

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
      uca4 = OXHEAD.get("OAUCA4").toString().trim()
      uca5 = OXHEAD.get("OAUCA5").toString().trim()
      uca6 = OXHEAD.get("OAUCA6").toString().trim()
      tedl = OXHEAD.get("OATEDL").toString().trim()
      cucd = OXHEAD.get("OACUCD").toString().trim()
      newe = OXHEAD.getDouble("OANEWE")
      grwe = OXHEAD.getDouble("OAGRWE")
      vol3 = OXHEAD.getDouble("OAVOL3")
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
    }

    getHeadData()

    if (!(isConfOrder)) {
      getLinesData()
    } else {
      getLinesData()
    }

    boolean first_line = true
    for (key in rayonMap.keySet()) {
      String value = rayonMap.get(key)
      String[] vt = value.split("#")
      String mapRayon = vt[0]
      String mapLibRayon = vt[1]
      String nbLines = vt[2]
      rayonline = mapRayon.trim() + ";" + mapLibRayon.trim() + ";" + nbLines.trim()
      if (first_line) {
        first_line = false
        rayonlines =  rayonline
      } else {
        rayonlines = rayonlines += "\r\n" + rayonline
      }
    }

    boolean first_lineM = true
    for (key in soustotMap.keySet()) {
      String value = soustotMap.get(key)
      String[] vt = value.split("#")
      String mapSsTot = vt[0]
      String mapLibSsTot = vt[1]
      String nbLines = vt[2]
      ssTotline = mapSsTot.trim() + ";" + mapLibSsTot.trim() + ";" + nbLines.trim()
      if (first_lineM) {
        first_lineM = false
        ssTotlines = ssTotline
      } else {
        ssTotlines = ssTotlines += "\r\n" + ssTotline
      }
    }

    // récap des lignes
    totline ="total Lignes Recues;" + totLignesRecues
    totlines = totlines += totline + "\r\n"
    totline ="total Lignes Intégrées;" + totLignesintegrees
    totlines = totlines += totline + "\r\n"
    totline ="Total lignes rejetées bloquantes;" +  totLignesRejeteesBloquantes
    totlines = totlines += totline + "\r\n"
    totline ="Total lignes erreur d'information;" + totLignesErreursInformation
    totlines = totlines += totline + "\r\n"
    totline ="Total lignes substituées;" + totLignesSubstituees
    totlines = totlines += totline

    if (ntam > 0) {
      totVal = ntam
    }

    // récap totaux intégrés
    double dVol3 = new BigDecimal(vol3).setScale(3, RoundingMode.HALF_EVEN).doubleValue()
    double dGrwe = new BigDecimal(grwe).setScale(3, RoundingMode.HALF_EVEN).doubleValue()
    double dNewe = new BigDecimal(newe).setScale(3, RoundingMode.HALF_EVEN).doubleValue()
    double dTotVal = new BigDecimal(totVal).setScale(3, RoundingMode.HALF_EVEN).doubleValue()
    double dtotEquivPal = new BigDecimal(totEquivPal).setScale(3, RoundingMode.HALF_EVEN).doubleValue()
    totIntegre = "Volume total;" + dVol3
    totIntegres = totIntegres += totIntegre + "\r\n"
    totIntegre = "Total Colis;" + totCol
    totIntegres = totIntegres += totIntegre + "\r\n"
    totIntegre = "Total equivalent palette;" + dtotEquivPal
    totIntegres = totIntegres += totIntegre + "\r\n"
    totIntegre = "Total poids brut;" + dGrwe
    totIntegres = totIntegres += totIntegre + "\r\n"
    totIntegre = "Total poids net;" + dNewe
    totIntegres = totIntegres += totIntegre + "\r\n"
    totIntegre = "Total Valeur;" + dTotVal + ";" + cucd
    totIntegres = totIntegres += totIntegre

    // récap totaux rejetés
    double dVolTotRej = new BigDecimal(VolTotRej).setScale(3, RoundingMode.HALF_EVEN).doubleValue()
    double dTotBrutRej = new BigDecimal(totBrutRej).setScale(3, RoundingMode.HALF_EVEN).doubleValue()
    double dTotNetRej = new BigDecimal(totNetRej).setScale(3, RoundingMode.HALF_EVEN).doubleValue()
    double dTotValRej = new BigDecimal(totValRej).setScale(3, RoundingMode.HALF_EVEN).doubleValue()
    double dtotEquivPalRej = new BigDecimal(totEquivPalRej).setScale(3, RoundingMode.HALF_EVEN).doubleValue()
    totRejete = "Volume total;" + dVolTotRej
    totRejetes = totRejetes += totRejete + "\r\n"
    totRejete = "Total Colis;" + totColRej
    totRejetes = totRejetes += totRejete + "\r\n"
    totRejete = "Total equivalent palette;" + dtotEquivPalRej
    totRejetes = totRejetes += totRejete + "\r\n"
    totRejete = "Total poids brut;" + dTotBrutRej
    totRejetes = totRejetes += totRejete + "\r\n"
    totRejete = "Total poids net;" +  dTotNetRej
    totRejetes = totRejetes += totRejete + "\r\n"
    totRejete = "Total Valeur;" + dTotValRej + ";" + cucd
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

  // Retrieve OXCNTR
  Closure<?> outData_OXCNTR = { DBContainer OXCNTR ->
    String stat = OXCNTR.get("EVSTAT")
    if ("90".equalsIgnoreCase(stat)) {
      confOrderNumber = OXCNTR.get("EVORNR")
    }
    numDemande = OXCNTR.get("EVORNR")
  }
  // Write date from order head
  public void getLinesData() {

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
    VolTot = 0
    totCol = 0
    totEquivPal = 0
    totBrut = 0
    totNet = 0
    totVal = 0
    VolTotRej = 0
    totColRej = 0
    totEquivPalRej = 0
    totBrutRej = 0
    totNetRej = 0
    totValRej = 0


    // Retrieve CONN
    conn = 0
    dlix = 0
    Closure<?> outdata_OXLINE = { DBContainer OXLINE ->
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
        logger.debug("rsc1 : " + rsc1)
      }

      errorCode = ""
      typeError = ""
      commItem = ""
      rayon = ""
      librayon = ""
      pcb = 0
      ean13 =""
      itds = ""

      totLignesRecues++
      totLignesintegrees++

      errorCode = retrieveError(inOrderNumber, ponr, posx)
      if (errorCode == "") errorCode = rscd
      if(rsc1 != "") {
        errorCode = rsc1
      }
      if (errorCode == "") errorCode = "0"
      logger.debug("errorCode : " + errorCode)

      // logger.debug("errorCode : " + errorCode)
      typeError = getErrorDescription(cuno, errorCode)

      if (itno.startsWith("NOK-S6")) itno = fitn
      ean13 = getEAN(itno)
      pcb = getItemPCB(itno)

      executeOIS320MIGetPriceLine(cuno, itno, dateIntegration, "UVC", orqt, ortp)
      getItemInfos(itno)
      getLevelItemInfo(rayon)

      commItem = getItemDataFromCUGEX1MITMAS(itno)

      // map Rayon
      if (rayon != "") {
        totligneRayon = 1
        String key = rayon
        String value = rayon
        value += "#" +  librayon
        value += "#" +  String.valueOf(totligneRayon)
        if (!rayonMap.containsKey(key)){
          rayonMap.put(key, value)
          logger.debug("rayon map key=${key}")
        } else {
          String[] ks = rayonMap.get(key).split("#")
          String keyRayon = ks[0]
          String keylibRayon = ks[1]
          totligneRayon = ks[2] as int
          totligneRayon++
          String valueMap = keyRayon
          valueMap += "#" +  keylibRayon
          valueMap += "#" +  String.valueOf(totligneRayon)
          rayonMap.put(keyRayon, valueMap)
          logger.debug("update rayon map key=${key}")
        }
      }

      // map repartition sous totaux par type d'erreurs
      if (errorCode != "0") {
        totSsligneError = 1
        String key = errorCode
        String value = errorCode
        value += "#" +  typeError
        value += "#" +  String.valueOf(totSsligneError)
        logger.debug("Add document_EXT036 key=${key}")
        if (!soustotMap.containsKey(key)){
          soustotMap.put(key, value)
        } else {
          String[] ks = soustotMap.get(key).split("#")
          String keySsTot = ks[0]
          String keylibSsTot = ks[1]
          totSsligneError = ks[2] as int
          totSsligneError++
          String valueMap = keySsTot
          valueMap += "#" +  keylibSsTot
          valueMap += "#" +  String.valueOf(totSsligneError)
          soustotMap.put(keySsTot, valueMap)
        }
      }

      int errorBloc = Integer.parseInt(errorCode)
      int IntOrqt = (int) Math.round(orqt)
      int IntUdn6 = (int) Math.round(udn6)
      int IntPcb = (int) Math.round(pcb)

      boolean dlcBloc = false
      if (errorBloc != 0 && errorBloc != 49 && errorBloc != 50 && errorBloc != 51 && errorBloc != 52) dlcBloc = true
      if (dlcBloc) {
        double valeur = new BigDecimal(nepr * orqt).setScale(3, RoundingMode.HALF_EVEN).doubleValue()
        double equivPal = new BigDecimal(orqt / getItemCoef(itno, "UPA")).setScale(2, RoundingMode.HALF_EVEN).doubleValue()
        blocline = itno + ";" + ean13 + ";" + itds + ";" + IntOrqt + ";"  + "0" + ";" + IntPcb + ";" + valeur  + ";" + equivPal + ";" + typeError + ";" + commItem
        bloclines += blocline + "\r\n"
        totLignesintegrees--
        totLignesRejeteesBloquantes++
        totEquivPalRej = totEquivPalRej + equivPal
        totValRej = totValRej + valeur
        double equivCol = new BigDecimal(orqt / getItemCoef(itno, "COL")).setScale(2, RoundingMode.HALF_EVEN).doubleValue()
        int equivColArrondie = (int) Math.round(equivCol)
        totColRej = totColRej + equivColArrondie
        double brutLine = orqt * grweItem
        double netLine = orqt * neweItem
        double volLine = orqt * vol3Item
        totBrutRej = totBrutRej + brutLine
        totNetRej = totNetRej + netLine
        VolTotRej = VolTotRej + volLine
      } else {
        double valeur = new BigDecimal(lnam).setScale(3, RoundingMode.HALF_EVEN).doubleValue()
        double equivPal = new BigDecimal(orqt / getItemCoef(itno, "UPA")).setScale(2, RoundingMode.HALF_EVEN).doubleValue()
        nonBlocline = itno + ";" + ean13 + ";" + itds + ";" + IntUdn6 + ";"  + IntOrqt + ";" + IntPcb + ";" + valeur  + ";" + equivPal + ";" + typeError + ";" + commItem
        nonBloclines += nonBlocline + "\r\n"
        totLignesErreursInformation++
        totEquivPal = totEquivPal + equivPal
        totVal = totVal + valeur
        double equivCol = new BigDecimal(orqt / getItemCoef(itno, "COL")).setScale(2, RoundingMode.HALF_EVEN).doubleValue()
        int equivColArrondie = (int) Math.round(equivCol)
        totCol = totCol + equivColArrondie
      }

      if (repi != "") {
        double valeur = new BigDecimal(lnam).setScale(3, RoundingMode.HALF_EVEN).doubleValue()
        double equivPal = new BigDecimal(orqt / getItemCoef(itno, "UPA")).setScale(2, RoundingMode.HALF_EVEN).doubleValue()
        replacedBlocline = itno + ";" + ean13 + ";" + itds + ";" + IntUdn6 + ";"  + IntOrqt + ";" + IntPcb + ";" + valeur  + ";" + equivPal + ";" + typeError + ";" + commItem
        replacedBloclines += replacedBlocline + "\r\n"
        totLignesSubstituees++
        totEquivPal = totEquivPal + equivPal
        double equivCol = new BigDecimal(orqt / getItemCoef(itno, "COL")).setScale(2, RoundingMode.HALF_EVEN).doubleValue()
        int equivColArrondie = (int) Math.round(equivCol)
        totCol = totCol + equivColArrondie
      }

      if (udn6 != orqt) {
        double qteArrondie = new BigDecimal((orqt / udn6) * 100).setScale(2, RoundingMode.HALF_EVEN).doubleValue()
        int pourcentageArrondie = (int) Math.round(qteArrondie)
        roundedline = itno + ";" + ean13 + ";" + itds + ";" + IntOrqt + ";"  + IntUdn6 + ";" + IntPcb + ";" + pourcentageArrondie
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
    if (!queryOXLINE.readAll(OXLINE, 2, outdata_OXLINE)) {
      logger.debug("pas de ligne de commande trouvée")
    }

  }

  // Write date from order head
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
    Closure<?> outData_MHDISH = { DBContainer MHDISH ->
      logger.debug("found MHDISH")
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
    if (!queryMHDISH.readAll(MHDISH, 2, outData_MHDISH)) {
      logger.debug("Index de livraison n'existe pas")
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
      logger.debug("found DCONSI")
    }

    //DRADTR informations
    ccud = ""
    DBAction queryDRADTR = database.table("DRADTR").index("00").selection("DRCCUD").build()
    DBContainer DRADTR = queryDRADTR.getContainer()
    DRADTR.set("DRCONO", currentCompany)
    DRADTR.set("DRCONN", conn)
    DRADTR.set("DRTLVL", 2)
    DRADTR.set("DRDLIX", dlix)
    if (queryDRADTR.read(DRADTR)) {
      logger.debug("found DRADTR")
      ccud = DRADTR.getString("DRFWNO").trim()
    }

    numTransitaire = fwno
    datePosit = ccud
    incoterm = tedl + " " + getIncoterm(cuno, tedl)
  }
  // get transport mode
  public String getTransport(String codeClient, String TransportMode) {
    lncd = getcustomerLang(codeClient)
    return getTextCSYTAB("MODL", lncd, TransportMode)
  }

  // get incoterm
  public String getIncoterm(String codeClient, String Incoterm) {
    lncd = getcustomerLang(codeClient)
    return getDescriptionCSYTAB("TEDL", lncd, Incoterm)
  }

  // get transport mode
  public String getErrorDescription(String codeClient, String errorCode) {
    lncd = getcustomerLang(codeClient)
    return getDescriptionCSYTAB("RSCD", lncd, errorCode)
  }

  // Get EAN
  public String getEAN(String ITNO) {
    String EAN13 = ""
    //init query on MITPOP
    ExpressionFactory expressionMITPOP = database.getExpressionFactory("MITPOP")
    expressionMITPOP = expressionMITPOP.eq("MPREMK", "EA13")
    DBAction queryMITPOP = database.table("MITPOP").index("00").matching(expressionMITPOP).selection("MPPOPN").build()
    DBContainer containerMITPOP = queryMITPOP.getContainer()
    containerMITPOP.set("MPCONO",currentCompany)
    containerMITPOP.set("MPALWT",1)
    containerMITPOP.set("MPALWQ","")
    containerMITPOP.set("MPITNO", ITNO)

    //loop on MITPOP records
    Closure<?> readMITPOP = { DBContainer resultMITPOP ->
      EAN13 = resultMITPOP.getString("MPPOPN").trim()
    }
    queryMITPOP.readAll(containerMITPOP, 4, readMITPOP)

    return EAN13
  }

  // Retrieve Error List
  public String retrieveError(String ORNO, Integer PONR, Integer POSX) {
    int saveMSCD = 0
    int mscd = 0
    String errorCODE = ""
    String mscdCODE = ""
    Closure<?> outData_EXT013 = { DBContainer EXT013 ->
      fitn = EXT013.getString("EXFITN").trim()
      mscdCODE = EXT013.getString("EXMSCD").trim()
      if (mscdCODE != "") {
        mscd = Integer.parseInt(mscdCODE)
      } else {
        mscd = 0
      }
      logger.debug("found MSCD : " + itno + " line : " + ponr + " and MSCD :" + mscd)
      if (saveMSCD == 0 || mscd > saveMSCD) {
        saveMSCD = mscd
        errorCODE = EXT013.getString("EXMSCD").trim()
        logger.debug("found Saved MSCD : " + itno + " line : " + ponr + " and savedMSCD :" + saveMSCD)
      }
    }

    DBAction orderRemark = database.table("EXT013").index("00").selection(
      "EXORNO",
      "EXPONR",
      "EXFITN",
      "EXMSCD",
      "EXREMK").build()
    DBContainer EXT013 = orderRemark.getContainer()
    EXT013.set("EXCONO", currentCompany)
    EXT013.set("EXORNO", ORNO)
    EXT013.set("EXPONR", PONR)
    EXT013.set("EXPOSX", POSX)
    orderRemark.readAll(EXT013, 4, outData_EXT013)
    logger.debug("errorCode :" + errorCODE)
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
      logger.debug("found commentaire A121 : " + containerCUGEX1.getString("F1A121").trim() + " article "+  itno + " line : " + ponr)
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
    logger.debug("A130  : " + A130)
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
    containerCUGEVM.set("F3SEQN",Integer.parseInt(A130.trim()))

    if (queryCUGEVM.read(containerCUGEVM)) {
      TX40 = containerCUGEVM.getString("F3TX40").trim()
      logger.debug("found libelle temperature  : " + containerCUGEVM.getString("F3TX40").trim())
    }
    return TX40
  }

  // get Item coef
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
      logger.debug("found MITAUN : " + itno + " line : " + ponr + " and COFA :" + cofa)
    }

    return cofa
  }

  // get texte 15 CSYTAB
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

  // get parm CSYTAB
  public String getDescriptionCSYTAB(String constant, String lang, String key) {
    String CTPARM = ""
    DBAction queryCSYTAB = database.table("CSYTAB").index("00").selection("CTPARM","CTTX40").build()
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
        logger.debug("CTPARM" + CTPARM)
        logger.debug("found CSYTAB PARM : " + CSYTAB.getString("CTTX40").trim() + "item : "+ itno + " line : " + ponr)
      }
    } else {
      DBAction queryCSYTAB1 = database.table("CSYTAB").index("00").selection("CTPARM","CTTX40").build()
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
          logger.debug("CTPARM" + CTPARM)
        }
      }
    }
    return CTPARM
  }

  // get Item informations
  public void getItemInfos(String ITNO) {
    itds = ""
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
      logger.debug("found MITMAS : " + itno + " line : " + ponr)
      itds = MITMAS.getString("MMITDS").trim()
      grweItem = MITMAS.getDouble("MMGRWE")
      neweItem = MITMAS.getDouble("MMNEWE")
      vol3Item = MITMAS.getDouble("MMVOL3")
      rayon = MITMAS.getString("MMHIE2").trim()
      logger.debug("rayon : " + rayon)
    }

  }

  // get Item informations
  public void getLevelItemInfo(String HIE2) {
    librayon = ""
    DBAction queryMITHRY = database.table("MITHRY").index("00").selection("HITX40").build()
    DBContainer MITHRY = queryMITHRY.getContainer()
    MITHRY.set("HICONO", currentCompany)
    MITHRY.set("HIHLVL", 2)
    MITHRY.set("HIHIE0", HIE2.trim())
    if (queryMITHRY.read(MITHRY)) {
      logger.debug("librayon : " + librayon)
      librayon = MITHRY.getString("HITX40").trim()
    }

  }

  // get Item PCB
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
      logger.debug("found MPACIT : " + itno + " line : " + ponr)
    }
    return ItemPCB
  }

  // get customer lang
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


  private executeOIS320MIGetPriceLine(String CUNO, String ITNO, String ORDT, String ALUN, double ORQA, String ORTP) {
    def parameters = [
      "CUNO": CUNO,
      "ITNO": ITNO,
      "ORQA": "" + ORQA,
      "ALUN": ALUN,
      "ORDT": ORDT,
      "ORTP": ORTP]
    Closure<?> handler = { Map<String, String> response ->
      logger.debug("OIS320MI.GetPriceLine " + response)
      if (response.error != null) {
        logger.debug("Failed OIS320MI.GetPriceLine: " + response.errorMessage)
      } else {
        lnam = response.LNAM as double
        nepr = response.NETP as double
        logger.debug("OIS100.AddOrderLine " + lnam)
      }
    }
    miCaller.call("OIS320MI", "GetPriceLine", parameters, handler)
  }

  /* Get mail user
  */
  private void getMailUser() {
    //Define return object structure
    DBAction queryCEMAIL = database.table("CEMAIL").index("00").selection("CBEMAL").build()
    DBContainer containerCEMAIL = queryCEMAIL.getContainer()
    containerCEMAIL.set("CBCONO", currentCompany)
    containerCEMAIL.set("CBEMTP", "04")
    containerCEMAIL.set("CBEMKY", program.getUser())

    Closure<?> readCEMAIL = { DBContainer resultCEMAIL ->
      MailLines = resultCEMAIL.getString("CBEMAL").trim()
      logger.debug("Mail trouvé" + MailLines)
    }
    queryCEMAIL.readAll(containerCEMAIL, 3, readCEMAIL)
    logger.debug("Mail : " + MailLines)
    logger.debug("user : " + program.getUser())

  }

  // Write bloking Lines file
  public void writeBlokingAnomalyLineFile() {
    logFileName = fileJobNumber + "-" + inOrderNumber + "-" + "BlokingLines" + "-" + "rapport.txt"
    logger.debug("writeRapportFile ${logFileName}")
    header = "Article" + ";" + "Code EAN" + ";" + "Libellé article" + ";" + "Qté cdées par le client"+ ";" + "Qtés intégrées" + ";" + "PCB"+ ";" + "Equiv Pal" + ";" + "Valeur" + ";" +  "Type erreur" + ";" + "Commentaire Article"
    logMessage(header, bloclines)
  }

  // Write non bloking Lines file
  public void writeNonBlokingAnomalyLineFile() {
    logFileName = fileJobNumber + "-" + inOrderNumber + "-" + "NonBlokingLines" + "-" + "rapport.txt"
    logger.debug("writeRapportFile ${logFileName}")
    header = "Article" + ";" + "Code EAN" + ";" + "Libellé article" + ";" + "Qté cdées par le client"+ ";" + "Qtés intégrées" + ";" + "PCB"+ ";" + "Equiv Pal" + ";" + "Valeur" + ";" +  "Type erreur" + ";" + "Commentaire Article"
    logMessage(header, nonBloclines)
  }

  // Write Replaced Lines file
  public void writeReplacedLineFile() {
    logFileName = fileJobNumber + "-" + inOrderNumber + "-" + "ReplacedLine" + "-" + "rapport.txt"
    logger.debug("writeRapportFile ${logFileName}")
    header = "Article" + ";" + "Code EAN" + ";" + "Libellé article" + ";" + "Qté cdées par le client"+ ";" + "Qtés intégrées" + ";" + "PCB"+ ";" + "Equiv Pal" + ";" + "Valeur" + ";" +  "Type erreur" + ";" + "Commentaire Article"
    logMessage(header, replacedBloclines)
  }

  // Write Rounded Lines file
  public void writeRoundedLineFile() {
    logFileName = fileJobNumber + "-" + inOrderNumber + "-" + "RoundedLine" + "-" + "rapport.txt"
    logger.debug("writeRapportFile ${logFileName}")
    header = "Article" + ";" + "Code EAN" + ";" + "Libellé article" + ";" + "Qté fichier"+ ";" + "Qté Cdée" + ";" + "PCB"+ ";" + "taux arrondi"
    logMessage(header, roundedlines)
  }

  // Write Rayon Lines file
  public void writeRayonLineFile() {
    logFileName = fileJobNumber + "-" + inOrderNumber + "-" + "RayonLine" + "-" + "rapport.txt"
    logger.debug("writeRapportFile ${logFileName}")
    header = "Rayon" + ";" + "Libellé Rayon" + ";" + "Nb de Lignes"
    logMessage(header, rayonlines)
  }

  // Write total Lines file
  public void writeTotlinesFile() {
    logFileName = fileJobNumber + "-" + inOrderNumber + "-" + "totlines" + "-" + "rapport.txt"
    logger.debug("writeRapportFile ${logFileName}")
    header = "Sous total lignes par type d'erreur" + ";" + "Nb de Lignes"
    logMessage(header, totlines)
  }

  // Write sous total Lines file
  public void writeSsTotlinesFile() {
    logFileName = fileJobNumber + "-" + inOrderNumber + "-" + "ssTotlines" + "-" + "rapport.txt"
    logger.debug("writeRapportFile ${logFileName}")
    header = "Code Erreur" + ";" + "Libellé Erreur" + ";" + "Nb de Lignes"
    logMessage(header, ssTotlines)
  }

  // Write total integre file
  public void writeTotIntegreFile() {
    logFileName = fileJobNumber + "-" + inOrderNumber + "-" + "TotIntegre" + "-" + "rapport.txt"
    logger.debug("writeRapportFile ${logFileName}")
    header = "Total Integré" + ";" + "valeur"
    logMessage(header, totIntegres)
  }

  // Write total integre file
  public void writeTotRejeteFile() {
    logFileName = fileJobNumber + "-" + inOrderNumber + "-" + "TotRejete" + "-" + "rapport.txt"
    logger.debug("writeRapportFile ${logFileName}")
    header = "Total Rejeté" + ";" + "valeur"
    logMessage(header, totRejetes)
  }

  // Write header file
  public void writeRapportHeaderFile() {
    logFileName = fileJobNumber + "-" + inOrderNumber + "-" + "Header" + "-" + "rapport.txt"
    logger.debug("writeRapportFile ${logFileName}")
    header = "N° Magasin" + ";" + "Mode de transport" + ";" + "N° Demande" + ";" + "N° commande" + ";" + "Date Intégration" + ";" + "Code Fournisseur"+ ";" + "Date Pos" + ";" + "rotation" + ";" + "type d'approvisionnement" + ";" + "température" + ";" + "N° Transitaire" + ";" + "Incoterm"
    logMessage(header, "")
    countLines = 0
    lines = numMagasin + ";" + modeTransport + ";" + numDemande + ";" + numCommande + ";" + dateIntegration + ";" + codeFournisseur + ";" + datePosit + ";" + rotation + ";" + typeAppro + ";" + temperature + ";" + numTransitaire + ";" + incoterm
    logMessage("", lines)
  }

  // Write the file indicating the end of processing
  public void writeMailFile() {
    logFileName = fileJobNumber + "-" + inOrderNumber + "-" + "MailFile.txt"
    logger.debug("writeRapportFile ${logFileName}")
    header = "Adresses Mail"
    logMessage(header, MailLines)
  }

  // Write the file indicating the end of processing
  public void writeEndFile() {
    logFileName = fileJobNumber + "-" + inOrderNumber + "-" + "docNumber.xml"
    docnumber = fileJobNumber + "-" + inOrderNumber
    logger.debug("write-endfile ${logFileName}")
    header = "<?xml version=\"1.0\" encoding=\"UTF-8\"?><Document>  <DocumentType>RAPPORTINTEGRATION</DocumentType>  <DocumentNumber>${docnumber}</DocumentNumber>  <DocumentPath>F:\\RapportIntegration\\</DocumentPath></Document>"
    logMessage(header, "")
  }
  // Log message
  void logMessage(String header, String line) {

    if (logFileName.endsWith("docNumber.xml"))
      logger.debug("logMessage ${logFileName}")

    logger.debug("header = " + header)
    logger.debug("line = " + line)
    if (header.trim() != "") {
      log(header)
      //logger.debug("write header")
    }
    if (line.trim() != "") {
      log(line)
      //logger.debug("write line")
    }
  }

  // Log
  void log(String message) {
    IN60 = true
    //logger.debug(message)
    //message = LocalDateTime.now().toString() + ";" + message
    Closure<?> consumer = { PrintWriter printWriter ->
      logger.debug("message ${message}")
      printWriter.println(message)
    }
    if (logFileName.endsWith("docNumber.xml")) {
      logger.debug("logMessage ${logFileName} message:${message}")
      textFiles.write(logFileName, "UTF-8", false, consumer)
    } else {
      textFiles.write(logFileName, "UTF-8", true, consumer)
    }
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
}
