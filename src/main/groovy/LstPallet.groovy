/**
 * README
 * This extension is used by Mashup
 *
 * Name : EXT050MI.LstPallet
 * Description : List pallet
 * Date         Changed By   Description
 * 20230524     SEAR         LOG28 - Creation of files and containers
 * 20240319     MLECLERCQ    LOG28 - filters
 */

import java.math.RoundingMode
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

public class LstPallet extends ExtendM3Transaction {
  private final MIAPI mi
  private final LoggerAPI logger
  private final ProgramAPI program
  private final DatabaseAPI database
  private final SessionAPI session
  private final TransactionAPI transaction
  private final MICallerAPI miCaller
  private final UtilityAPI utility
  private boolean sameWarehouse
  private boolean sameIndex
  private boolean foundLineIndex
  private boolean sameCustomer
  private String whloInput
  private String uca4Input
  private String uca5Input
  private String uca6Input
  private String cunoInput
  private Long dlixInput
  private Integer currentCompany
  private String cunoOohead
  private double volume
  private double weight
  private double salesPrice
  private String baseUnit
  private String itemNumber
  private String description
  private String dossier
  private String semaine
  private String annee
  private double lnamOoline
  private double alqtOoline
  private int dmcsOoline
  private double cofsOoline
  private String spunOoline
  private String faciOoline
  private double allocatedQuantity
  private double allocatedQuantityUB
  private String commande
  private int lineNumber
  private int lineSuffix
  private String orstOoline
  private String custName
  private String custNumber
  private Long lineIndex
  private int connMhdish
  private String rscdMhdish
  private int sanitary
  private double free2
  private int dangerous
  private int sensitive

  private String ridnMitalo
  private int ridlMitalo
  private int ridxMitalo
  private String camuMitalo
  private Long ridiMitalo
  private double alqtMitalo
  private String whloMitalo
  private String csno
  private String orco
  private String popn
  private String suno

  private Map<String, Boolean> quafiltersInput
  private LinkedHashMap<String, Boolean> filterResults

  private String dgx4

  private int isRecordValid = 0
  private double nbOfCols

  private String jobNumber
  private Integer nbMaxRecord = 10000

  public LstPallet(LoggerAPI logger, MIAPI mi, DatabaseAPI database, ProgramAPI program, MICallerAPI miCaller, UtilityAPI utility) {
    this.logger = logger
    this.mi = mi
    this.database = database
    this.program = program
    this.miCaller = miCaller
    this.utility = utility
  }

  public void main() {

    LocalDateTime timeOfCreation = LocalDateTime.now()
    jobNumber = program.getJobNumber() + timeOfCreation.format(DateTimeFormatter.ofPattern("yyMMdd")) + timeOfCreation.format(DateTimeFormatter.ofPattern("HHmmss"))


    if (mi.in.get("CONO") == null) {
      currentCompany = (Integer)program.getLDAZD().CONO
    } else {
      currentCompany = mi.in.get("CONO")
    }

    quafiltersInput = new HashMap<String, Boolean>()
    filterResults = new LinkedHashMap<String, Boolean>()

    //Get mi inputs
    whloInput = (mi.in.get("WHLO") != null ? (String)mi.in.get("WHLO") : "")
    uca4Input = (mi.in.get("UCA4") != null ? (String)mi.in.get("UCA4") : "")
    uca5Input = (mi.in.get("UCA5") != null ? (String)mi.in.get("UCA5") : "")
    uca6Input = (mi.in.get("UCA6") != null ? (String)mi.in.get("UCA6") : "")
    cunoInput = (mi.in.get("CUNO") != null ? (String)mi.in.get("CUNO") : "")
    dlixInput = (Long)(mi.in.get("DLIX") != null ? mi.in.get("DLIX") : 0)

    quafiltersInput["PHYT"] = (mi.in.get("PHYT") != null ? (Boolean) mi.in.get("PHYT") : false)
    quafiltersInput["SANI"] = (mi.in.get("SANI") != null ? (Boolean) mi.in.get("SANI") : false)
    quafiltersInput["PETF"] = (mi.in.get("PETF") != null ? (Boolean) mi.in.get("PETF") : false)
    quafiltersInput["LAIT"] = (mi.in.get("LAIT") != null ? (Boolean) mi.in.get("LAIT") : false)
    quafiltersInput["ALCO"] = (mi.in.get("ALCO") != null ? (Boolean) mi.in.get("ALCO") : false)
    quafiltersInput["BIER"] = (mi.in.get("BIER") != null ? (Boolean) mi.in.get("BIER") : false)
    quafiltersInput["VIN1"] = (mi.in.get("VIN1") != null ? (Boolean) mi.in.get("VIN1") : false)
    quafiltersInput["CHAM"] = (mi.in.get("CHAM") != null ? (Boolean) mi.in.get("CHAM") : false)
    quafiltersInput["DGX1"] = (mi.in.get("DGX1") != null ? (Boolean) mi.in.get("DGX1") : false)
    quafiltersInput["DGX4"] = (mi.in.get("DGX4") != null ? (Boolean) mi.in.get("DGX4") : false)
    quafiltersInput["ISO1"] = (mi.in.get("ISO1") != null ? (Boolean) mi.in.get("ISO1") : false)
    quafiltersInput["DPH1"] = (mi.in.get("DPH1") != null ? (Boolean) mi.in.get("DPH1") : false)

    // check warehouse
    DBAction queryMitwhl = database.table("MITWHL").index("00").selection("MWWHLO").build()
    DBContainer MITWHL = queryMitwhl.getContainer()
    MITWHL.set("MWCONO", currentCompany)
    MITWHL.set("MWWHLO", whloInput)
    if(!queryMitwhl.read(MITWHL)){
      mi.error("Le dépôt " + whloInput + " n'existe pas")
      return
    }

    // check customer number
    if (cunoInput.length() > 0) {
      DBAction queryOcusma = database.table("OCUSMA").index("00").selection("OKCUNO").build()
      DBContainer OCUSMA = queryOcusma.getContainer()
      OCUSMA.set("OKCONO", currentCompany)
      OCUSMA.set("OKCUNO", cunoInput)
      if(!queryOcusma.read(OCUSMA)){
        mi.error("Le code client  " + cunoInput + " n'existe pas")
        return
      }
    }

    // check index number
    if ((mi.in.get("DLIX") != null)) {
      DBAction queryMhdish = database.table("MHDISH").index("00").selection("OQDLIX").build()
      DBContainer MHDISH = queryMhdish.getContainer()
      MHDISH.set("OQCONO", currentCompany)
      MHDISH.set("OQINOU", 1)
      MHDISH.set("OQDLIX", dlixInput)
      if(!queryMhdish.read(MHDISH)){
        mi.error("Index de livraison  " + dlixInput + " n'existe pas")
        return
      }
    }

    ExpressionFactory expressionOohead = database.getExpressionFactory("OOHEAD")
    if(uca4Input != ""){
      expressionOohead = expressionOohead.eq("OAUCA4", uca4Input)
    } else {
      expressionOohead = expressionOohead.ne("OAUCA4", "")
    }
    if(uca5Input != ""){
      expressionOohead = expressionOohead.and(expressionOohead.eq("OAUCA5", uca5Input))
    } else {
      expressionOohead = expressionOohead.and(expressionOohead.ne("OAUCA5", ""))
    }
    if(uca6Input != ""){
      expressionOohead = expressionOohead.and(expressionOohead.eq("OAUCA6", uca6Input))
    } else {
      expressionOohead = expressionOohead.and(expressionOohead.ne("OAUCA6", ""))
    }
    expressionOohead = expressionOohead.and(expressionOohead.le("OAORSL", '44'))

    DBAction queryOohead = database.table("OOHEAD").index("00").matching(expressionOohead).selection("OAORNO","OAUCA4","OAUCA5","OAUCA6","OACUNO").build()
    DBContainer containerOOHEAD = queryOohead.getContainer()
    containerOOHEAD.set("OACONO", currentCompany)

    if (!queryOohead.readAll(containerOOHEAD, 1, nbMaxRecord, OOHEADData)){
    }

    // list out data
    DBAction ListqueryEXT052 = database.table("EXT052")
      .index("10")
      .selection(
        "EXBJNO",
        "EXCONO",
        "EXUCA4",
        "EXUCA5",
        "EXUCA6",
        "EXORNO",
        "EXCAMU",
        "EXCUNO",
        "EXCUNM",
        "EXHAZI",
        "EXCFI2",
        "EXZSAN",
        "EXFILT",
        "EXSENS",
        "EXALQT",
        "EXGRWE",
        "EXVOL3",
        "EXZAAM",
        "EXDLIX",
        "EXCONN",
        "EXRSCD",
        "EXCOLS"
      )
      .build()

    DBContainer ListContainerEXT052 = ListqueryEXT052.getContainer()
    ListContainerEXT052.set("EXBJNO", jobNumber)

    //Record exists
    if (!ListqueryEXT052.readAll(ListContainerEXT052, 1, nbMaxRecord, outData)){
    }

    // delete workfile
    DBAction DelQuery = database.table("EXT052").index("00").build()
    DBContainer DelcontainerEXT052 = DelQuery.getContainer()
    DelcontainerEXT052.set("EXBJNO", jobNumber)
    if(!DelQuery.readAllLock(DelcontainerEXT052, 1, deleteCallBack)){
    }
  }

  // liste OOHEAD
  Closure<?> OOHEADData = { DBContainer containerOOHEAD ->

    int company = containerOOHEAD.get("OACONO")
    commande = containerOOHEAD.get("OAORNO")
    dossier = containerOOHEAD.get("OAUCA4")
    semaine = containerOOHEAD.get("OAUCA5")
    annee = containerOOHEAD.get("OAUCA6")
    cunoOohead = containerOOHEAD.get("OACUNO")
    logger.debug("found OOHEAD : " + commande)


    initFilters()

    // Get MITLO
    ExpressionFactory expressionMitalo = database.getExpressionFactory("MITALO")
    expressionMitalo = (expressionMitalo.eq("MQWHLO", whloInput))
    expressionMitalo = expressionMitalo.and(expressionMitalo.eq("MQPLSX", "0"))
    DBAction queryMitalo = database.table("MITALO").index("10").matching(expressionMitalo).selection("MQRIDN","MQRIDL","MQRIDX","MQRIDI","MQCAMU","MQALQT","MQITNO","MQWHLO").build()
    DBContainer MITALO = queryMitalo.getContainer()
    MITALO.set("MQCONO", company)
    MITALO.set("MQTTYP", 31)
    MITALO.set("MQRIDN", commande)
    if(queryMitalo.readAll(MITALO, 3, nbMaxRecord, MITALOData)){
    }
    logger.debug("commande : " +  commande)
    logger.debug("dossier : " +  dossier + "semaine : " +  semaine + "annee : " +  annee)
    logger.debug("sameWarehouse : " +  sameWarehouse)
  }

  // data MITALO
  Closure<?> MITALOData = { DBContainer ContainerMITALO ->
    ridnMitalo = ContainerMITALO.get("MQRIDN")
    ridlMitalo = ContainerMITALO.get("MQRIDL")
    ridxMitalo = ContainerMITALO.get("MQRIDX")
    camuMitalo = ContainerMITALO.get("MQCAMU")
    ridiMitalo = ContainerMITALO.get("MQRIDI")
    alqtMitalo = ContainerMITALO.get("MQALQT")
    whloMitalo = ContainerMITALO.get("MQWHLO")
    itemNumber = ContainerMITALO.get("MQITNO")

    logger.debug("found MITALO RIDN: " + ridnMitalo)
    logger.debug("found MITALO RIDL: " + ridlMitalo)
    logger.debug("found MITALO RIDI: " + ridiMitalo)

    if(ridiMitalo == 0) {
      DBAction queryMhdisl = database.table("MHDISL").index("10").selection("URDLIX").build()
      DBContainer MHDISL = queryMhdisl.getContainer()
      MHDISL.set("URCONO", currentCompany)
      MHDISL.set("URRORC", 3)
      MHDISL.set("URRIDN", ridnMitalo)
      MHDISL.set("URRIDL", ridlMitalo)
      MHDISL.set("URRIDX", ridxMitalo)
      if(queryMhdisl.readAll(MHDISL, 5, nbMaxRecord, outdataMhdisl)){
      }
    }

    sameWarehouse = false
    sameIndex = false
    sameCustomer = false
    foundLineIndex = false

    if (whloMitalo.equals(whloInput)) {
      sameWarehouse = true
    }

    if(cunoInput.length() > 0 ){
      if (cunoInput.trim().equals(cunoOohead.trim())) {
        sameCustomer = true
      }
    } else {
      sameCustomer = true
    }

    if ((mi.in.get("DLIX") == null)) {
      sameIndex = true
    } else {
      if (dlixInput == ridiMitalo) {
        sameIndex = true
      }
    }

    connMhdish = 0
    DBAction queryMhdish = database.table("MHDISH").index("00").selection("OQDLIX","OQCONN","OQRSCD").build()
    DBContainer MHDISH = queryMhdish.getContainer()
    MHDISH.set("OQCONO", currentCompany)
    MHDISH.set("OQINOU", 1)
    MHDISH.set("OQDLIX", ridiMitalo)
    if(queryMhdish.read(MHDISH)){
      connMhdish = MHDISH.get("OQCONN")
      rscdMhdish = MHDISH.get("OQRSCD")
      logger.debug("found MHDISH CONN: " + connMhdish)
    }

    logger.debug("sameIndex: " + sameIndex)
    logger.debug("sameWarehouse: " + sameWarehouse)
    logger.debug("sameCustomer: " + sameCustomer)

    if (sameIndex && sameWarehouse && sameCustomer) {
      logger.debug("valide line")

      spunOoline = ""
      // Get OOLINE
      DBAction queryOoline = database.table("OOLINE").index("00").selection("OBCUNO","OBORNO","OBPONR","OBPOSX","OBWHLO","OBSPUN","OBDMCS","OBCOFS","OBALQT","OBLNAM","OBFACI","OBITNO").build()
      DBContainer OOLINE = queryOoline.getContainer()
      OOLINE.set("OBCONO", currentCompany)
      OOLINE.set("OBORNO", ridnMitalo)
      OOLINE.set("OBPONR", ridlMitalo)
      OOLINE.set("OBPOSX", ridxMitalo)
      if(queryOoline.read(OOLINE)){
        spunOoline = OOLINE.get("OBSPUN")
        cofsOoline = OOLINE.get("OBCOFS")
        alqtOoline = OOLINE.get("OBALQT")
        lnamOoline = OOLINE.get("OBLNAM")
        dmcsOoline = OOLINE.get("OBDMCS")
        faciOoline = OOLINE.get("OBFACI")
        lineSuffix = OOLINE.get("OBPOSX")
        lineNumber = OOLINE.get("OBPONR")
      }

      // get OCUSMA
      DBAction queryOcusma = database.table("OCUSMA").index("00").selection("OKCUNO","OKCUNM").build()
      DBContainer OCUSMA = queryOcusma.getContainer()
      OCUSMA.set("OKCONO", currentCompany)
      OCUSMA.set("OKCUNO", cunoOohead)
      if(queryOcusma.read(OCUSMA)){
        custName = OCUSMA.get("OKCUNM")
        custNumber = OCUSMA.get("OKCUNO")
      }

      // get MITMAS
      baseUnit = ""
      suno = ""
      free2 = 0
      dangerous = 0
      DBAction queryMitmas = database.table("MITMAS").index("00").selection("MMITDS", "MMPUUN","MMUNMS", "MMGRWE", "MMVOL3","MMHAZI","MMCFI2","MMSUNO", "MMCFI4", "MMHAC1").build()
      DBContainer MITMAS = queryMitmas.getContainer()
      MITMAS.set("MMCONO", currentCompany)
      MITMAS.set("MMITNO", itemNumber)
      if(queryMitmas.read(MITMAS)){
        description = MITMAS.get("MMITDS")
        baseUnit = MITMAS.get("MMUNMS")
        volume = MITMAS.getDouble("MMVOL3")
        weight = MITMAS.getDouble("MMGRWE")
        dangerous = MITMAS.getInt("MMHAZI")
        dgx4 = MITMAS.get("MMHAC1")
        free2 = MITMAS.getDouble("MMCFI2")
        suno = MITMAS.get("MMSUNO")

        if(quafiltersInput.get("BIER") || quafiltersInput.get("VIN1")){
          if(!filterResults.get("BIER") || !filterResults.get("VIN1")){
            String cfi4 = MITMAS.get("MMCFI4")
            cfi4 = cfi4.trim()
            //Don't need to check if both are already true
            filterResults = (LinkedHashMap<String, Boolean>)filterBeerAndWine(cfi4, filterResults)
          }

          logger.debug("Bier or wine filters, their values are : " + filterResults.get("BIER") + ", " + filterResults.get("VIN1") + " for PAL: ${camuMitalo}")
        }
      }

      if(quafiltersInput.get("ISO1") || quafiltersInput.get("DPH1")){
        filterResults = (LinkedHashMap<String, Boolean>)filterOnCugex(itemNumber, filterResults)
        logger.debug("ISO or DPH filters, their values are : " + filterResults.get("ISO1") + ", " + filterResults.get("DPH1"))
      }

      logger.debug("suno = " + suno)
      logger.debug("itemNumber = " + itemNumber)
      popn = ""
      ExpressionFactory expressionMitpop = database.getExpressionFactory("MITPOP")
      expressionMitpop = expressionMitpop.eq("MPREMK", "SIGMA6")
      DBAction mitpopQuery = database.table("MITPOP").index("00").matching(expressionMitpop).selection("MPPOPN").build()
      DBContainer MITPOP = mitpopQuery.getContainer()
      MITPOP.set("MPCONO", currentCompany)
      MITPOP.set("MPALWT", 1)
      MITPOP.set("MPALWQ", "")
      MITPOP.set("MPITNO", itemNumber)
      if (!mitpopQuery.readAll(MITPOP, 4, nbMaxRecord, outdataMitpop)) {
      }
      logger.debug("popn = " + popn)

      logger.debug("faciOoline = " + faciOoline)
      csno =""
      orco = ""
      DBAction mitfacQuery = database.table("MITFAC").index("00").selection("M9CSNO","M9ORCO").build()
      DBContainer MITFAC = mitfacQuery.getContainer()
      MITFAC.set("M9CONO", currentCompany)
      MITFAC.set("M9FACI", faciOoline)
      MITFAC.set("M9ITNO", itemNumber)
      if(mitfacQuery.read(MITFAC)){
        csno = MITFAC.get("M9CSNO")
        orco = MITFAC.get("M9ORCO")
      }
      logger.debug("csno = ${csno} and orco = ${orco}")

      sanitary = 0
      sensitive = 0
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
        "F1CHB9"
      ).build()

      DBContainer containerCUGEX1 = queryCUGEX100.getContainer()
      containerCUGEX1.set("F1CONO", currentCompany)
      containerCUGEX1.set("F1FILE", "MITMAS")
      containerCUGEX1.set("F1PK01", itemNumber)

      if (queryCUGEX100.read(containerCUGEX1)) {
        sensitive = containerCUGEX1.get("F1CHB9")
      }

      DBAction ext032Query = database.table("EXT032").index("00").selection("EXZSAN","EXZALC").build()
      DBContainer EXT032 = ext032Query.getContainer()
      EXT032.set("EXCONO", currentCompany)
      EXT032.set("EXPOPN", popn)
      EXT032.set("EXSUNO", suno)
      EXT032.set("EXORCO", orco)
      if(ext032Query.read(EXT032)){
        sanitary = EXT032.get("EXZSAN")

        filterResults["SANI"] = sanitary == 1 ? true : false

        boolean alcool = (boolean)EXT032.get("EXZALC")

        logger.debug("Filtre alcool ? : " + quafiltersInput["ALCO"] + " , EXT032 ZALC = ${alcool}")
        if (quafiltersInput.get("ALCO") && !filterResults.get("ALCO")){
          //If alcool already true, don't need to check
          if(alcool){
            filterResults["ALCO"] = true
          }
        }

      }
      logger.debug("sanitary = " + sanitary)

      allocatedQuantityUB =  alqtMitalo

      double ALQT = new BigDecimal (allocatedQuantityUB).setScale(6, RoundingMode.HALF_EVEN).doubleValue()
      double GRWE = new BigDecimal (allocatedQuantityUB * weight).setScale(6, RoundingMode.HALF_EVEN).doubleValue()
      double VOL3 = new BigDecimal (allocatedQuantityUB * volume).setScale(6, RoundingMode.HALF_EVEN).doubleValue()
      double ZAAM = new BigDecimal (lnamOoline).setScale(6, RoundingMode.HALF_EVEN).doubleValue()

      DBAction queryMitaun = database.table("MITAUN").index("00").selection("MUALUN","MUCOFA").build()
      DBContainer MITAUN = queryMitaun.getContainer()
      MITAUN.set("MUCONO", currentCompany)
      MITAUN.set("MUITNO", itemNumber)
      MITAUN.set("MUAUTP", 1)
      MITAUN.set("MUALUN","COL")

      if(queryMitaun.read(MITAUN)){
        Double cofa = MITAUN.get("MUCOFA")
        logger.debug("MUCOFA = ${cofa}")
        nbOfCols = ALQT / cofa
        nbOfCols = new BigDecimal(nbOfCols).setScale(6, RoundingMode.HALF_EVEN).doubleValue()

        logger.debug("nbOfCols = ${nbOfCols}")
      }else{
        logger.debug("record not found in MITAUN")
      }


      logger.debug("line OK")

      if (quafiltersInput.containsValue(true)) {

        filterResults = (LinkedHashMap<String, Boolean>) filterOnEXT036(commande,lineNumber, lineSuffix,filterResults )

        for(resultKey in filterResults.keySet()){
          logger.debug("Result for key: ${resultKey} = " + filterResults.get(resultKey) + " for PAL = ${camuMitalo}")
        }

        boolean eq = areFiltersEquals()
        logger.debug("filters equals = ${eq}")
        isRecordValid = eq ? 1 : 0

      }else{
        isRecordValid = 0
      }

      logger.debug("Record is valid before EXT052: " + isRecordValid)

      //Check if record exists
      DBAction queryEXT052 = database.table("EXT052")
        .index("00")
        .selection(
          "EXBJNO",
          "EXCONO",
          "EXUCA4",
          "EXUCA5",
          "EXUCA6",
          "EXORNO",
          "EXCAMU",
          "EXCUNO",
          "EXCUNM",
          "EXHAZI",
          "EXCFI2",
          "EXZSAN",
          "EXSENS",
          "EXALQT",
          "EXGRWE",
          "EXVOL3",
          "EXZAAM",
          "EXDLIX",
          "EXCONN",
          "EXRSCD",
          "EXCOLS",
          "EXRGDT",
          "EXRGTM",
          "EXLMDT",
          "EXCHNO",
          "EXCHID"
        )
        .build()

      DBContainer containerEXT052 = queryEXT052.getContainer()
      containerEXT052.set("EXBJNO", jobNumber)
      containerEXT052.set("EXCONO", currentCompany)
      containerEXT052.set("EXUCA4", dossier)
      containerEXT052.set("EXUCA5", semaine)
      containerEXT052.set("EXUCA6", annee)
      containerEXT052.set("EXORNO", commande)
      containerEXT052.set("EXCAMU", camuMitalo)

      //Record exists
      if (queryEXT052.read(containerEXT052)) {
        Closure<?> updateEXT050 = { LockedResult lockedResultEXT052 ->
          Long lastDlix = lockedResultEXT052.getLong("EXDLIX")
          if (lastDlix > 0 && lastDlix != ridiMitalo) {
            lockedResultEXT052.set("EXDLIX", 0)
          } else {
            lockedResultEXT052.set("EXDLIX", ridiMitalo)
          }
          if(dangerous == 1)
            lockedResultEXT052.set("EXHAZI", dangerous)
          if((int) free2 > 0)
            lockedResultEXT052.set("EXCFI2", 1)
          if(sanitary == 1)
            lockedResultEXT052.set("EXZSAN", sanitary)
          if(sensitive == 1){
            lockedResultEXT052.set("EXSENS", sensitive)
          }
          if(isRecordValid == 1)
            lockedResultEXT052.set("EXFILT", isRecordValid)
          lockedResultEXT052.set("EXALQT", lockedResultEXT052.getDouble("EXALQT") + ALQT)
          lockedResultEXT052.set("EXGRWE", lockedResultEXT052.getDouble("EXGRWE") + GRWE)
          lockedResultEXT052.set("EXVOL3", lockedResultEXT052.getDouble("EXVOL3") + VOL3)
          lockedResultEXT052.set("EXZAAM", lockedResultEXT052.getDouble("EXZAAM") + ZAAM)
          lockedResultEXT052.set("EXCOLS", lockedResultEXT052.getDouble("EXCOLS") + nbOfCols)
          lockedResultEXT052.set("EXLMDT", utility.call("DateUtil", "currentDateY8AsInt"))
          lockedResultEXT052.setInt("EXCHNO", ((Integer)lockedResultEXT052.get("EXCHNO") + 1))
          lockedResultEXT052.set("EXCHID", program.getUser())
          lockedResultEXT052.update()

        }
        queryEXT052.readLock(containerEXT052, updateEXT050)
      } else {
        containerEXT052.set("EXBJNO", jobNumber)
        containerEXT052.set("EXCONO", currentCompany)
        containerEXT052.set("EXUCA4", dossier)
        containerEXT052.set("EXUCA5", semaine)
        containerEXT052.set("EXUCA6", annee)
        containerEXT052.set("EXORNO", commande)
        containerEXT052.set("EXCUNO", custNumber)
        containerEXT052.set("EXCUNM", custName)
        containerEXT052.set("EXHAZI", dangerous)
        containerEXT052.set("EXCFI2", free2)
        containerEXT052.set("EXZSAN", sanitary)
        containerEXT052.set("EXFILT", isRecordValid)
        containerEXT052.set("EXALQT", ALQT)
        containerEXT052.set("EXGRWE", GRWE)
        containerEXT052.set("EXVOL3", VOL3)
        containerEXT052.set("EXZAAM", ZAAM)
        containerEXT052.set("EXSENS", sensitive)
        containerEXT052.set("EXCOLS", nbOfCols)
        containerEXT052.set("EXDLIX", ridiMitalo)
        containerEXT052.set("EXCONN", connMhdish)
        containerEXT052.set("EXRSCD", rscdMhdish)
        containerEXT052.set("EXRGDT", utility.call("DateUtil", "currentDateY8AsInt"))
        containerEXT052.set("EXRGTM", utility.call("DateUtil", "currentTimeAsInt"))
        containerEXT052.set("EXLMDT", utility.call("DateUtil", "currentDateY8AsInt"))
        containerEXT052.set("EXCHNO", 1)
        containerEXT052.set("EXCHID", program.getUser())
        queryEXT052.insert(containerEXT052)
      }

      isRecordValid = 0

      initFilters()

    }
  }

  /**
   * Retrieve MHDISL data
   */
  Closure<?> DataMHDISL = { DBContainer containerMHDISL ->
    lineIndex = containerMHDISL.getLong("URDLIX")
    sameIndex = true
    foundLineIndex = true
  }

  /**
   * Retrieve EXT052 data
   */
  Closure<?> outData = { DBContainer containerEXT052 ->
    String dossierEXT052 = containerEXT052.get("EXUCA4")
    String semaineEXT052 = containerEXT052.get("EXUCA5")
    String anneeEXT052 = containerEXT052.get("EXUCA6")
    String camuEXT052 = containerEXT052.get("EXCAMU")
    String commandeEXT052 = containerEXT052.get("EXORNO")
    String customerEXT052 = containerEXT052.get("EXCUNO")
    String custNameEXT052 = containerEXT052.get("EXCUNM")
    String dangerousEXT052 = containerEXT052.get("EXHAZI")
    String free2EXT052 = containerEXT052.get("EXCFI2")
    String sanitaryEXT052 = containerEXT052.get("EXZSAN")
    String sensitiveEXT052 = containerEXT052.get("EXSENS")
    String allocatedEXT052 = containerEXT052.get("EXALQT")
    String weightEXT052 = containerEXT052.get("EXGRWE")
    String volumeEXT052 = containerEXT052.get("EXVOL3")
    String amountEXT052 = containerEXT052.get("EXZAAM")
    String colsEXT052 = containerEXT052.get("EXCOLS")
    String indexEXT052 = containerEXT052.get("EXDLIX")
    String shipmentEXT052 = containerEXT052.get("EXCONN")
    String reasonCodeEXT052 = containerEXT052.get("EXRSCD")
    String filterEXT052 = containerEXT052.get("EXFILT")
    logger.debug("EXT052 RSCD = " + containerEXT052.get("EXRSCD"))
    logger.debug("reasonCodeEXT052 = " + reasonCodeEXT052)


    mi.outData.put("UCA4", dossierEXT052)
    mi.outData.put("UCA5", semaineEXT052)
    mi.outData.put("UCA6", anneeEXT052)
    mi.outData.put("CAMU", camuEXT052)
    mi.outData.put("ORNO", commandeEXT052)
    mi.outData.put("CUNO", customerEXT052)
    mi.outData.put("CUNM", custNameEXT052)
    mi.outData.put("HAZI", dangerousEXT052)
    mi.outData.put("CFI2", free2EXT052)
    mi.outData.put("ZSAN", sanitaryEXT052)
    mi.outData.put("SENS", sensitiveEXT052)
    mi.outData.put("COLS", colsEXT052)
    mi.outData.put("ALQT", allocatedEXT052)
    mi.outData.put("GRWE", weightEXT052)
    mi.outData.put("VOL3", volumeEXT052)
    mi.outData.put("ZAAM", amountEXT052)
    mi.outData.put("DLIX", indexEXT052)
    mi.outData.put("CONN", shipmentEXT052)
    mi.outData.put("RSCD", reasonCodeEXT052)

    if(quafiltersInput.containsValue(true)){
      if(filterEXT052 == "1"){
        mi.write()
      }
    }else{
      mi.write()
    }
  }

  /**
   * Retrieve MITPOP data
   */
  Closure<?> outdataMitpop = { DBContainer MITPOP ->
    popn = MITPOP.get("MPPOPN")
  }
  /**
   * Delete
   */
  Closure<?> deleteCallBack = { LockedResult lockedResult ->
    lockedResult.delete()
  }
  /**
   * Retrieve MHDISL data
   */
  Closure<?> outdataMhdisl = { DBContainer MHDISL ->
    ridiMitalo = MHDISL.get("URDLIX")
    logger.debug("found MHDISL RIDI: " + ridiMitalo)
  }

  /**
   * Filter beer and wine
   */
  private filterBeerAndWine(cfi4,filterResults){
    logger.debug("MMCFI4 : ${cfi4}")

    //TODO : remove hard values
    if(cfi4 == "S" || cfi4 == "T"){
      filterResults["BIER"] = true
    }else if(cfi4 == "1" || cfi4 == "2" || cfi4 == "3" || cfi4 == "4" || cfi4 == "5" || cfi4 == "6" || cfi4 == "9"
      || cfi4 == "B" || cfi4 == "C" || cfi4 == "D" || cfi4 == "K" || cfi4 == "L" || cfi4 == "M" || cfi4 == "N"
      || cfi4 == "Q" || cfi4 == "U" || cfi4 == "X"
    ){
      filterResults["VIN1"] = true
    }

    return filterResults
  }

  /**
   * Filter on CUGEX1
   */
  private filterOnCugex(itno, filterResults){

    LinkedHashMap<String, Boolean> results = (LinkedHashMap<String, Boolean>)filterResults

    DBAction queryCugex1 = database.table("CUGEX1").index("00").selection("F1CHB9", "F1CHB6").build()
    DBContainer CUGEX1 = queryCugex1.getContainer()
    CUGEX1.set("F1CONO", currentCompany)
    CUGEX1.set("F1FILE", "MITMAS")
    CUGEX1.set("F1PK01", itno)

    logger.debug("In function filterOnCugex with ITNO: ${itno}")

    int chb6 = 0
    int chb9 = 0

    if(queryCugex1.readAll(CUGEX1,3,1, { DBContainer closureCugex1 ->
      chb6 = closureCugex1.get("F1CHB6")
      chb9 = closureCugex1.get("F1CHB9")
    })){
      logger.debug("Get filters in Cugex : CHB6 = " + CUGEX1.get("F1CHB6") + " and CHB9 = " + CUGEX1.get("F1CHB9") + "pour Article=${itno}")
      if(chb6 == 1){
        if(results.containsKey("DPH1")){
          results["DPH1"] = true
        }
      }

      if(chb9 == 1){
        if(results.containsKey("ISO1")){
          results["ISO1"] = true
        }
      }

      return results
    }
  }

  /**
   * Filter on EXT036
   */
  private filterOnEXT036(orno, ponr, posx, filterResults){

    LinkedHashMap<String, Boolean> results = (LinkedHashMap<String, Boolean>)filterResults
    posx = posx == null ? 0 : posx

    DBAction queryExt036 = database.table("EXT036").index("00").selection("EXORNO","EXPONR","EXITNO","EXZSTY", "EXZCTY").build()
    DBContainer containerEXT036 = queryExt036.getContainer()
    containerEXT036.set("EXCONO", currentCompany)
    containerEXT036.set("EXORNO", orno)
    containerEXT036.set("EXPONR", ponr as Integer)
    containerEXT036.set("EXPOSX", posx  as Integer)

    String zcty = ""
    String zsty = ""
    String itno = ""
    if(queryExt036.readAll(containerEXT036, 4,50, {DBContainer closureEXT036 ->
      zcty = closureEXT036.get("EXZCTY")
      zsty = closureEXT036.get("EXZSTY")
      itno = closureEXT036.get("EXITNO")
    })){
      logger.debug("In EXT036 ZCTY=${zcty} and ZSTY=${zsty} for ORNO:${orno} and PONR:${ponr}" )

      if(results.containsKey("PHYT") && zcty.trim() == "PHYTOSANITAIRE"){
        results["PHYT"] = true
      }

      if(results.containsKey("SANI") && zcty.trim() == "SANITAIRE"){
        logger.debug("In EXT036 , SANI = " + zcty.trim())
        results["SANI"] = true
      }

      if(results.containsKey("PETF") && zsty.trim() == "PET"){
        results["PETF"] = true
      }

      if(results.containsKey("LAIT") && zsty.trim() == "LAI"){
        results["LAIT"] = true
      }

      if(results.containsKey("CHAM") && zsty.trim() == "CHA"){
        results["CHAM"] = true
      }

      if(results.containsKey("DGX1") && zsty.trim() == "DGX"){
        results["DGX1"] = true
      }

      if(results.containsKey("DGX4") && zsty.trim() == "DGX"){
        DBAction queryMitmas = database.table("MITMAS").index("00").selection("MMHAC1").build()
        DBContainer containerMITMAS = queryMitmas.getContainer()
        containerMITMAS.set("MMCONO", currentCompany)
        containerMITMAS.set("MMITNO", itno)

        if(queryMitmas.read(containerMITMAS)){
          String haci = containerMITMAS.get("MMHAC1")
          if(haci == "4.1" || haci == "4.2" || haci == "4.3"){
            results["DGX4"] = true
          }
        }
      }

    }

    return results
  }

  /**
   * Initialize filters
   */
  private initFilters(){
    filterResults = new LinkedHashMap<>()

    for(key in quafiltersInput.keySet()){
      if(quafiltersInput.get(key) == true){
        filterResults.put(key,false)
      }
    }

  }

  /**
   * Check if filters are equals
   */
  private boolean areFiltersEquals(){
    Set<String> quaFiltersKeys = quafiltersInput.keySet()
    Collection<Boolean> quaFiltersValues = quafiltersInput.values()

    def filterResultsKeys = filterResults.keySet()
    def filterResultValues = filterResults.values()

    quaFiltersKeys.each {key ->
      logger.debug("quaFilters[${key}] = " + quafiltersInput[key])
    }

    filterResultsKeys.each { key ->
      logger.debug("filterResultsKeys[${key}] = " + filterResults[key])
    }

    return quafiltersInput.entrySet().containsAll(filterResults.entrySet())
  }
}
