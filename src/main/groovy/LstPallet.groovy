/****************************************************************************************
 Extension Name : EXT050MI.LstPallet
 Type : ExtendM3Transaction
 Author : SEAR
 Description
 This extension is used by Mashup
 List files and containers

 Description : List pallet
 Date         Changed By   Version      Description
 20230524     SEAR          1.0         LOG28 - Creation of files and containers
 20240319     MLECLERCQ     1.1         LOG28 - filters
 20250428     FLEBARS       1.2         Code review for infor validation
 20250627     FLEBARS       1.3         Quality filter modification for LAIT & PETFOOD
 ******************************************************************************************/

import java.math.RoundingMode
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

public class LstPallet extends ExtendM3Transaction {
  private final MIAPI mi
  private final LoggerAPI logger
  private final ProgramAPI program
  private final DatabaseAPI database

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
  private double allocatedQuantityUB
  private String commande
  private int lineNumber
  private int lineSuffix
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
  private Map<String, Boolean> filterResults
  private Map<String, String[]> ext034Map

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
      currentCompany = (Integer) program.getLDAZD().CONO
    } else {
      currentCompany = mi.in.get("CONO")
    }

    quafiltersInput = new HashMap<String, Boolean>()
    filterResults = new HashMap<String, Boolean>()

    //Get mi inputs
    whloInput = (mi.in.get("WHLO") != null ? (String) mi.in.get("WHLO") : "")
    uca4Input = (mi.in.get("UCA4") != null ? (String) mi.in.get("UCA4") : "")
    uca5Input = (mi.in.get("UCA5") != null ? (String) mi.in.get("UCA5") : "")
    uca6Input = (mi.in.get("UCA6") != null ? (String) mi.in.get("UCA6") : "")
    cunoInput = (mi.in.get("CUNO") != null ? (String) mi.in.get("CUNO") : "")
    dlixInput = (Long) (mi.in.get("DLIX") != null ? mi.in.get("DLIX") : 0)

    quafiltersInput["PHYT"] = (mi.in.get("PHYT") as Integer) == 1 ? true : false
    quafiltersInput["SANI"] = (mi.in.get("SANI") as Integer) == 1 ? true : false
    quafiltersInput["PETF"] = (mi.in.get("PETF") as Integer) == 1 ? true : false
    quafiltersInput["LAIT"] = (mi.in.get("LAIT") as Integer) == 1 ? true : false
    quafiltersInput["ALCO"] = (mi.in.get("ALCO") as Integer) == 1 ? true : false
    quafiltersInput["BIER"] = (mi.in.get("BIER") as Integer) == 1 ? true : false
    quafiltersInput["VIN1"] = (mi.in.get("VIN1") as Integer) == 1 ? true : false
    quafiltersInput["CHAM"] = (mi.in.get("CHAM") as Integer) == 1 ? true : false
    quafiltersInput["DGX1"] = (mi.in.get("DGX1") as Integer) == 1 ? true : false
    quafiltersInput["DGX4"] = (mi.in.get("DGX4") as Integer) == 1 ? true : false
    quafiltersInput["ISO1"] = (mi.in.get("ISO1") as Integer) == 1 ? true : false
    quafiltersInput["DPH1"] = (mi.in.get("DPH1") as Integer) == 1 ? true : false

    logger.debug("quafiltersInput ${quafiltersInput}")


    // check warehouse
    DBAction mitwhlQuery = database.table("MITWHL").index("00").selection("MWWHLO").build()
    DBContainer mitwhlRequest = mitwhlQuery.getContainer()
    mitwhlRequest.set("MWCONO", currentCompany)
    mitwhlRequest.set("MWWHLO", whloInput)
    if (!mitwhlQuery.read(mitwhlRequest)) {
      mi.error("Le dépôt " + whloInput + " n'existe pas")
      return
    }

    // check customer number
    if (cunoInput.length() > 0) {
      DBAction ocusmaQuery = database.table("OCUSMA").index("00").selection("OKCUNO").build()
      DBContainer ocusmaRequest = ocusmaQuery.getContainer()
      ocusmaRequest.set("OKCONO", currentCompany)
      ocusmaRequest.set("OKCUNO", cunoInput)
      if (!ocusmaQuery.read(ocusmaRequest)) {
        mi.error("Le code client  " + cunoInput + " n'existe pas")
        return
      }
    }

    // check index number
    if ((mi.in.get("DLIX") != null)) {
      DBAction mhdishQuery = database.table("MHDISH").index("00").selection("OQDLIX").build()
      DBContainer mhdishRequest = mhdishQuery.getContainer()
      mhdishRequest.set("OQCONO", currentCompany)
      mhdishRequest.set("OQINOU", 1)
      mhdishRequest.set("OQDLIX", dlixInput)
      if (!mhdishQuery.read(mhdishRequest)) {
        mi.error("Index de livraison  " + dlixInput + " n'existe pas")
        return
      }
    }

    ExpressionFactory ooheadExpression = database.getExpressionFactory("OOHEAD")
    if (uca4Input != "") {
      ooheadExpression = ooheadExpression.eq("OAUCA4", uca4Input)
    } else {
      ooheadExpression = ooheadExpression.ne("OAUCA4", "")
    }
    if (uca5Input != "") {
      ooheadExpression = ooheadExpression.and(ooheadExpression.eq("OAUCA5", uca5Input))
    } else {
      ooheadExpression = ooheadExpression.and(ooheadExpression.ne("OAUCA5", ""))
    }
    if (uca6Input != "") {
      ooheadExpression = ooheadExpression.and(ooheadExpression.eq("OAUCA6", uca6Input))
    } else {
      ooheadExpression = ooheadExpression.and(ooheadExpression.ne("OAUCA6", ""))
    }
    ooheadExpression = ooheadExpression.and(ooheadExpression.le("OAORSL", '44'))

    DBAction ooheadQuery = database.table("OOHEAD").index("00").matching(ooheadExpression).selection("OAORNO", "OAUCA4", "OAUCA5", "OAUCA6", "OACUNO").build()
    DBContainer ooheadRequest = ooheadQuery.getContainer()
    ooheadRequest.set("OACONO", currentCompany)

    if (!ooheadQuery.readAll(ooheadRequest, 1, nbMaxRecord, ooheadReader)) {
    }

    // list out data
    DBAction ext052Query = database.table("EXT052")
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

    DBContainer ext052Request = ext052Query.getContainer()
    ext052Request.set("EXBJNO", jobNumber)

    //Record exists
    if (!ext052Query.readAll(ext052Request, 1, nbMaxRecord, ext052Reader)) {
    }

    Closure<?> ext052DeleteReader = { DBContainer ext052Result ->
      Closure<?> ext052Deleter = { LockedResult ext052LockedResult ->
        ext052LockedResult.delete()
      }
      ext052Query.readLock(ext052Result, ext052Deleter)
    }

    //Record exists
    if (!ext052Query.readAll(ext052Request, 1, nbMaxRecord, ext052DeleteReader)) {
    }
  }

  // liste OOHEAD
  Closure<?> ooheadReader = { DBContainer ooheadResult ->
    int company = ooheadResult.get("OACONO")
    commande = ooheadResult.get("OAORNO")
    dossier = ooheadResult.get("OAUCA4")
    semaine = ooheadResult.get("OAUCA5")
    annee = ooheadResult.get("OAUCA6")
    cunoOohead = ooheadResult.get("OACUNO")


    filterResults = new HashMap<String, Boolean>()
    quafiltersInput.each { key, value ->
      if (value)
        filterResults.put(key, false)
    }
    // Get MITLO
    ExpressionFactory mitaloExpression = database.getExpressionFactory("MITALO")
    mitaloExpression = (mitaloExpression.eq("MQWHLO", whloInput))
    mitaloExpression = mitaloExpression.and(mitaloExpression.eq("MQPLSX", "0"))
    DBAction mitaloQuery = database.table("MITALO").index("10").matching(mitaloExpression).selection("MQRIDN", "MQRIDL", "MQRIDX", "MQRIDI", "MQCAMU", "MQALQT", "MQITNO", "MQWHLO").build()
    DBContainer mitaloRequest = mitaloQuery.getContainer()
    mitaloRequest.set("MQCONO", company)
    mitaloRequest.set("MQTTYP", 31)
    mitaloRequest.set("MQRIDN", commande)
    if (mitaloQuery.readAll(mitaloRequest, 3, nbMaxRecord, mitaloReader)) {
    }
    logger.debug("commande:${commande} dossier:${dossier} semaine:${semaine} annee:${annee}")
  }

  // data MITALO
  Closure<?> mitaloReader = { DBContainer mitaloResult ->
    ridnMitalo = mitaloResult.get("MQRIDN")
    ridlMitalo = mitaloResult.get("MQRIDL")
    ridxMitalo = mitaloResult.get("MQRIDX")
    camuMitalo = mitaloResult.get("MQCAMU")
    ridiMitalo = mitaloResult.get("MQRIDI")
    alqtMitalo = mitaloResult.get("MQALQT")
    whloMitalo = mitaloResult.get("MQWHLO")
    itemNumber = mitaloResult.get("MQITNO")

    if (ridiMitalo == 0) {
      DBAction mhdislQuery = database.table("MHDISL").index("10").selection("URDLIX").build()
      DBContainer mhdislRequest = mhdislQuery.getContainer()
      mhdislRequest.set("URCONO", currentCompany)
      mhdislRequest.set("URRORC", 3)
      mhdislRequest.set("URRIDN", ridnMitalo)
      mhdislRequest.set("URRIDL", ridlMitalo)
      mhdislRequest.set("URRIDX", ridxMitalo)
      if (mhdislQuery.readAll(mhdislRequest, 5, nbMaxRecord, mhdislReader)) {
      }
    }
    sameWarehouse = false
    sameIndex = false
    sameCustomer = false
    foundLineIndex = false

    if (whloMitalo.equals(whloInput)) {
      sameWarehouse = true
    }

    if (cunoInput.length() > 0) {
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
    DBAction mhdishQuery = database.table("MHDISH").index("00").selection("OQDLIX", "OQCONN", "OQRSCD").build()
    DBContainer mhdishRequest = mhdishQuery.getContainer()
    mhdishRequest.set("OQCONO", currentCompany)
    mhdishRequest.set("OQINOU", 1)
    mhdishRequest.set("OQDLIX", ridiMitalo)
    if (mhdishQuery.read(mhdishRequest)) {
      connMhdish = mhdishRequest.get("OQCONN")
      rscdMhdish = mhdishRequest.get("OQRSCD")
    }

    if (sameIndex && sameWarehouse && sameCustomer) {

      spunOoline = ""
      // Get OOLINE
      DBAction oolineQuery = database.table("OOLINE").index("00").selection("OBCUNO", "OBORNO", "OBPONR", "OBPOSX", "OBWHLO", "OBSPUN", "OBDMCS", "OBCOFS", "OBALQT", "OBLNAM", "OBFACI", "OBITNO").build()
      DBContainer oolineRequest = oolineQuery.getContainer()
      oolineRequest.set("OBCONO", currentCompany)
      oolineRequest.set("OBORNO", ridnMitalo)
      oolineRequest.set("OBPONR", ridlMitalo)
      oolineRequest.set("OBPOSX", ridxMitalo)
      if (oolineQuery.read(oolineRequest)) {
        spunOoline = oolineRequest.get("OBSPUN")
        cofsOoline = oolineRequest.get("OBCOFS")
        alqtOoline = oolineRequest.get("OBALQT")
        lnamOoline = oolineRequest.get("OBLNAM")
        dmcsOoline = oolineRequest.get("OBDMCS")
        faciOoline = oolineRequest.get("OBFACI")
        lineSuffix = oolineRequest.get("OBPOSX")
        lineNumber = oolineRequest.get("OBPONR")
      }

      // get OCUSMA
      DBAction ocusmaQuery = database.table("OCUSMA").index("00").selection("OKCUNO", "OKCUNM").build()
      DBContainer ocusmaRequest = ocusmaQuery.getContainer()
      ocusmaRequest.set("OKCONO", currentCompany)
      ocusmaRequest.set("OKCUNO", cunoOohead)
      if (ocusmaQuery.read(ocusmaRequest)) {
        custName = ocusmaRequest.get("OKCUNM")
        custNumber = ocusmaRequest.get("OKCUNO")
      }

      // get MITMAS
      baseUnit = ""
      suno = ""
      free2 = 0
      dangerous = 0
      DBAction mitmasQuery = database.table("MITMAS").index("00").selection("MMITDS", "MMPUUN", "MMUNMS", "MMGRWE", "MMVOL3", "MMHAZI", "MMCFI2", "MMSUNO", "MMCFI4", "MMHAC1").build()
      DBContainer mitmasRequest = mitmasQuery.getContainer()
      mitmasRequest.set("MMCONO", currentCompany)
      mitmasRequest.set("MMITNO", itemNumber)
      if (mitmasQuery.read(mitmasRequest)) {
        description = mitmasRequest.get("MMITDS")
        baseUnit = mitmasRequest.get("MMUNMS")
        volume = mitmasRequest.getDouble("MMVOL3")
        weight = mitmasRequest.getDouble("MMGRWE")
        dangerous = mitmasRequest.getInt("MMHAZI")
        dgx4 = mitmasRequest.get("MMHAC1")
        free2 = mitmasRequest.getDouble("MMCFI2")
        suno = mitmasRequest.get("MMSUNO")

        if (quafiltersInput.get("BIER") || quafiltersInput.get("VIN1")) {
          if (!filterResults.get("BIER") || !filterResults.get("VIN1")) {
            String cfi4 = mitmasRequest.get("MMCFI4")
            cfi4 = cfi4.trim()
            filterResults = (HashMap<String, Boolean>) filterBeerAndWine(cfi4, filterResults)
            logger.debug("filterResults after beer and wine: " + filterResults)
          }
        }
      }

      if (quafiltersInput.get("ISO1") || quafiltersInput.get("DPH1")) {
        filterResults = (HashMap<String, Boolean>) filterOnCugex(itemNumber, filterResults)
        logger.debug("filterResults after filterOnCugex: " + filterResults)
      }

      popn = ""
      ExpressionFactory mitpopExpression = database.getExpressionFactory("MITPOP")
      mitpopExpression = mitpopExpression.eq("MPREMK", "SIGMA6")
      DBAction mitpopQuery = database.table("MITPOP").index("00").matching(mitpopExpression).selection("MPPOPN").build()
      DBContainer mitpopRequest = mitpopQuery.getContainer()
      mitpopRequest.set("MPCONO", currentCompany)
      mitpopRequest.set("MPALWT", 1)
      mitpopRequest.set("MPALWQ", "")
      mitpopRequest.set("MPITNO", itemNumber)
      if (!mitpopQuery.readAll(mitpopRequest, 4, nbMaxRecord, mitpopReader)) {
      }

      csno = ""
      orco = ""
      DBAction mitfacQuery = database.table("MITFAC").index("00").selection("M9CSNO", "M9ORCO").build()
      DBContainer mitfacRequest = mitfacQuery.getContainer()
      mitfacRequest.set("M9CONO", currentCompany)
      mitfacRequest.set("M9FACI", faciOoline)
      mitfacRequest.set("M9ITNO", itemNumber)
      if (mitfacQuery.read(mitfacRequest)) {
        csno = mitfacRequest.get("M9CSNO")
        orco = mitfacRequest.get("M9ORCO")
      }

      sanitary = 0
      sensitive = 0
      DBAction cugex1Query = database.table("CUGEX1").index("00").selection("F1CONO",
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

      DBContainer cugex1Request = cugex1Query.getContainer()
      cugex1Request.set("F1CONO", currentCompany)
      cugex1Request.set("F1FILE", "MITMAS")
      cugex1Request.set("F1PK01", itemNumber)

      if (cugex1Query.read(cugex1Request)) {
        sensitive = cugex1Request.get("F1CHB9")
      }

      DBAction ext032Query = database.table("EXT032").index("00").selection("EXZSAN", "EXZALC").build()
      DBContainer ext032Request = ext032Query.getContainer()
      ext032Request.set("EXCONO", currentCompany)
      ext032Request.set("EXPOPN", popn)
      ext032Request.set("EXSUNO", suno)
      ext032Request.set("EXORCO", orco)
      if (ext032Query.read(ext032Request)) {
        sanitary = ext032Request.get("EXZSAN")

        filterResults["SANI"] = sanitary == 1 ? true : false

        boolean alcool = (boolean) ext032Request.get("EXZALC")

        if (quafiltersInput.get("ALCO") && !filterResults.get("ALCO")) {
          //If alcool already true, don't need to check
          if (alcool) {
            filterResults["ALCO"] = true
          }
        }

      }

      allocatedQuantityUB = alqtMitalo

      double dAlqt = new BigDecimal(allocatedQuantityUB).setScale(6, RoundingMode.HALF_EVEN).doubleValue()
      double dGrwe = new BigDecimal(allocatedQuantityUB * weight).setScale(6, RoundingMode.HALF_EVEN).doubleValue()
      double dVol3 = new BigDecimal(allocatedQuantityUB * volume).setScale(6, RoundingMode.HALF_EVEN).doubleValue()
      double dZaam = new BigDecimal(lnamOoline).setScale(6, RoundingMode.HALF_EVEN).doubleValue()

      DBAction mitaunQuery = database.table("MITAUN").index("00").selection("MUALUN", "MUCOFA").build()
      DBContainer mitaunRequest = mitaunQuery.getContainer()
      mitaunRequest.set("MUCONO", currentCompany)
      mitaunRequest.set("MUITNO", itemNumber)
      mitaunRequest.set("MUAUTP", 1)
      mitaunRequest.set("MUALUN", "COL")

      if (mitaunQuery.read(mitaunRequest)) {
        Double cofa = mitaunRequest.get("MUCOFA") as Double
        if (cofa == 0) {
          cofa = 1
        }
        nbOfCols = dAlqt / cofa
        nbOfCols = new BigDecimal(nbOfCols).setScale(6, RoundingMode.HALF_EVEN).doubleValue()
      }

      if (quafiltersInput.containsValue(true)) {

        filterResults = filterOnExt037(commande, lineNumber, lineSuffix, quafiltersInput)


        logger.debug("filterResults before writeline : " + filterResults + " " + (filterResults.containsValue(false)))
        boolean eq = !filterResults.containsValue(false)
        isRecordValid = eq ? 1 : 0

      } else {
        isRecordValid = 0
      }

      logger.debug("Record is valid before EXT052: " + isRecordValid)

      //Check if record exists
      DBAction ext052Query = database.table("EXT052")
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

      DBContainer ext052Request = ext052Query.getContainer()
      ext052Request.set("EXBJNO", jobNumber)
      ext052Request.set("EXCONO", currentCompany)
      ext052Request.set("EXUCA4", dossier)
      ext052Request.set("EXUCA5", semaine)
      ext052Request.set("EXUCA6", annee)
      ext052Request.set("EXORNO", commande)
      ext052Request.set("EXCAMU", camuMitalo)

      //Record exists
      if (ext052Query.read(ext052Request)) {
        Closure<?> updateExt052 = { LockedResult lockedResultEXT052 ->
          Long lastDlix = lockedResultEXT052.getLong("EXDLIX")
          if (lastDlix > 0 && lastDlix != ridiMitalo) {
            lockedResultEXT052.set("EXDLIX", 0)
          } else {
            lockedResultEXT052.set("EXDLIX", ridiMitalo)
          }
          if (dangerous == 1)
            lockedResultEXT052.set("EXHAZI", dangerous)
          if ((int) free2 > 0)
            lockedResultEXT052.set("EXCFI2", 1)
          if (sanitary == 1)
            lockedResultEXT052.set("EXZSAN", sanitary)
          if (sensitive == 1) {
            lockedResultEXT052.set("EXSENS", sensitive)
          }
          if (isRecordValid == 1)
            lockedResultEXT052.set("EXFILT", isRecordValid)
          lockedResultEXT052.set("EXALQT", lockedResultEXT052.getDouble("EXALQT") + dAlqt)
          lockedResultEXT052.set("EXGRWE", lockedResultEXT052.getDouble("EXGRWE") + dGrwe)
          lockedResultEXT052.set("EXVOL3", lockedResultEXT052.getDouble("EXVOL3") + dVol3)
          lockedResultEXT052.set("EXZAAM", lockedResultEXT052.getDouble("EXZAAM") + dZaam)
          lockedResultEXT052.set("EXCOLS", lockedResultEXT052.getDouble("EXCOLS") + nbOfCols)
          lockedResultEXT052.set("EXLMDT", utility.call("DateUtil", "currentDateY8AsInt"))
          lockedResultEXT052.setInt("EXCHNO", ((Integer) lockedResultEXT052.get("EXCHNO") + 1))
          lockedResultEXT052.set("EXCHID", program.getUser())
          lockedResultEXT052.update()

        }
        ext052Query.readLock(ext052Request, updateExt052)
      } else {
        ext052Request.set("EXBJNO", jobNumber)
        ext052Request.set("EXCONO", currentCompany)
        ext052Request.set("EXUCA4", dossier)
        ext052Request.set("EXUCA5", semaine)
        ext052Request.set("EXUCA6", annee)
        ext052Request.set("EXORNO", commande)
        ext052Request.set("EXCUNO", custNumber)
        ext052Request.set("EXCUNM", custName)
        ext052Request.set("EXHAZI", dangerous)
        ext052Request.set("EXCFI2", free2)
        ext052Request.set("EXZSAN", sanitary)
        ext052Request.set("EXFILT", isRecordValid)
        ext052Request.set("EXALQT", dAlqt)
        ext052Request.set("EXGRWE", dGrwe)
        ext052Request.set("EXVOL3", dVol3)
        ext052Request.set("EXZAAM", dZaam)
        ext052Request.set("EXSENS", sensitive)
        ext052Request.set("EXCOLS", nbOfCols)
        ext052Request.set("EXDLIX", ridiMitalo)
        ext052Request.set("EXCONN", connMhdish)
        ext052Request.set("EXRSCD", rscdMhdish)
        ext052Request.set("EXRGDT", utility.call("DateUtil", "currentDateY8AsInt"))
        ext052Request.set("EXRGTM", utility.call("DateUtil", "currentTimeAsInt"))
        ext052Request.set("EXLMDT", utility.call("DateUtil", "currentDateY8AsInt"))
        ext052Request.set("EXCHNO", 1)
        ext052Request.set("EXCHID", program.getUser())
        ext052Query.insert(ext052Request)
      }

      isRecordValid = 0
    }
  }

  /**
   * Retrieve EXT052 data
   */
  Closure<?> ext052Reader = { DBContainer ext052Result ->
    String dossierExt052 = ext052Result.get("EXUCA4")
    String semaineExt052 = ext052Result.get("EXUCA5")
    String anneeExt052 = ext052Result.get("EXUCA6")
    String camuExt052 = ext052Result.get("EXCAMU")
    String commandeExt052 = ext052Result.get("EXORNO")
    String customerExt052 = ext052Result.get("EXCUNO")
    String custNameExt052 = ext052Result.get("EXCUNM")
    String dangerousExt052 = ext052Result.get("EXHAZI")
    String free2Ext052 = ext052Result.get("EXCFI2")
    String sanitaryExt052 = ext052Result.get("EXZSAN")
    String sensitiveExt052 = ext052Result.get("EXSENS")
    String allocatedExt052 = ext052Result.get("EXALQT")
    String weightExt052 = ext052Result.get("EXGRWE")
    String volumeExt052 = ext052Result.get("EXVOL3")
    String amountExt052 = ext052Result.get("EXZAAM")
    String colsExt052 = ext052Result.get("EXCOLS")
    String indexExt052 = ext052Result.get("EXDLIX")
    String shipmentExt052 = ext052Result.get("EXCONN")
    String reasonCodeExt052 = ext052Result.get("EXRSCD")
    String filterExt052 = ext052Result.get("EXFILT")

    mi.outData.put("UCA4", dossierExt052)
    mi.outData.put("UCA5", semaineExt052)
    mi.outData.put("UCA6", anneeExt052)
    mi.outData.put("CAMU", camuExt052)
    mi.outData.put("ORNO", commandeExt052)
    mi.outData.put("CUNO", customerExt052)
    mi.outData.put("CUNM", custNameExt052)
    mi.outData.put("HAZI", dangerousExt052)
    mi.outData.put("CFI2", free2Ext052)
    mi.outData.put("ZSAN", sanitaryExt052)
    mi.outData.put("SENS", sensitiveExt052)
    mi.outData.put("COLS", colsExt052)
    mi.outData.put("ALQT", allocatedExt052)
    mi.outData.put("GRWE", weightExt052)
    mi.outData.put("VOL3", volumeExt052)
    mi.outData.put("ZAAM", amountExt052)
    mi.outData.put("DLIX", indexExt052)
    mi.outData.put("CONN", shipmentExt052)
    mi.outData.put("RSCD", reasonCodeExt052)

    if (quafiltersInput.containsValue(true)) {
      if (filterExt052 == "1") {
        mi.write()
      }
    } else {
      mi.write()
    }
  }

  /**
   * Retrieve MITPOP data
   */
  Closure<?> mitpopReader = { DBContainer mitpopResult ->
    popn = mitpopResult.get("MPPOPN")
  }

  /**
   * Retrieve MHDISL data
   */
  Closure<?> mhdislReader = { DBContainer mhdislResult ->
    ridiMitalo = mhdislResult.get("URDLIX")
  }

  /**
   * Filter beer and wine
   */
  private Map<String, Boolean> filterBeerAndWine(String cfi4, Map<String, Boolean> lFilterResults) {
    logger.debug("filterBeerAndWine itno:${itemNumber} cfi4:${cfi4}")
    if (cfi4 == "S" || cfi4 == "T") {
      lFilterResults["BIER"] = true
    } else if (cfi4 == "1" || cfi4 == "2" || cfi4 == "3" || cfi4 == "4" || cfi4 == "5" || cfi4 == "6" || cfi4 == "9"
      || cfi4 == "B" || cfi4 == "C" || cfi4 == "D" || cfi4 == "K" || cfi4 == "L" || cfi4 == "M" || cfi4 == "N"
      || cfi4 == "Q" || cfi4 == "U" || cfi4 == "X"
    ) {
      lFilterResults["VIN1"] = true
    }
    return lFilterResults
  }

  /**
   * Filter on CUGEX1
   */
  private filterOnCugex(String itno, Map<String, Boolean> filterResults) {
    logger.debug("filterOnCugex itno:${itno}")
    HashMap<String, Boolean> results = (HashMap<String, Boolean>) filterResults

    DBAction cugex1Query = database.table("CUGEX1").index("00").selection("F1CHB9", "F1CHB6").build()
    DBContainer cugex1Request = cugex1Query.getContainer()
    cugex1Request.set("F1CONO", currentCompany)
    cugex1Request.set("F1FILE", "MITMAS")
    cugex1Request.set("F1PK01", itno)

    int chb6 = 0
    int chb9 = 0

    if (cugex1Query.readAll(cugex1Request, 3, 1, { DBContainer cugex1Result ->
      chb6 = cugex1Result.get("F1CHB6")
      chb9 = cugex1Result.get("F1CHB9")
    })) {
      if (chb6 == 1) {
        if (results.containsKey("DPH1")) {
          results["DPH1"] = true
        }
      }

      if (chb9 == 1) {
        if (results.containsKey("ISO1")) {
          results["ISO1"] = true
        }
      }

      return results
    }
  }

  /**
   * Filter on EXT037
   */
  private HashMap<String, Boolean> filterOnExt037(String orno, int ponr, int posx, Map<String, Boolean> quafiltersInput) {

    logger.debug("filterOnExt037 orno:${orno} ponr:${ponr} posx:${posx}")

    HashMap<String, Boolean> results = (HashMap<String, Boolean>)filterResults
    posx = posx == null ? 0 : posx

    logger.debug("quafiltersInput " + quafiltersInput)

    DBAction ext037Query = database.table("EXT037").index("00").selection("EXORNO", "EXPONR", "EXITNO", "EXZCTY", "EXZCOD").build()
    DBContainer ext037Request = ext037Query.getContainer()
    ext037Request.set("EXCONO", currentCompany)
    ext037Request.set("EXORNO", orno)
    ext037Request.set("EXPONR", ponr as Integer)
    ext037Request.set("EXPOSX", posx as Integer)
    ext037Request.set("EXDLIX", 0)

    String zcty = ""
    String zcod = ""
    String itno = ""
    String zsty = ""
    if (ext037Query.readAll(ext037Request, 5, 50, { DBContainer ext037Reader ->
      zcty = ext037Reader.getString("EXZCTY").trim()
      zcod = ext037Reader.getString("EXZCOD").trim()
      itno = ext037Reader.getString("EXITNO").trim()
    })) {
      logger.debug("In EXT037 ZCTY=${zcty} and ZSTY=${zcod} for ORNO:${orno} and PONR:${ponr}")
      getExt034Data(zcod)
      zsty = ext034Map.get(zcod)[1]


      if (quafiltersInput.get("PHYT") && zcty.trim() == "PHYTOSANITAIRE") {
        results["PHYT"] = true
      }

      if (quafiltersInput.get("SANI") && zcty.trim() == "SANITAIRE") {
        logger.debug("In EXT037 , SANI = " + zcty.trim())
        results["SANI"] = true
      }

      if (quafiltersInput.get("PETF") && zsty.trim() == "PETFOOD") {
        logger.debug("In EXT037 , PETF = " + zcty.trim())
        results["PETF"] = true
      }

      if (quafiltersInput.get("LAIT") && zsty.trim() == "LAIT") {
        results["LAIT"] = true
      }

      if (quafiltersInput.get("CHAM") && zcod.trim() == "CHA") {
        results["CHAM"] = true
      }

      if (quafiltersInput.get("DGX1") && zcod.trim() == "DGX") {
        results["DGX1"] = true
      }

      if (quafiltersInput.get("DGX4") && zcod.trim() == "DGX") {
        DBAction mitmasQuery = database.table("MITMAS").index("00").selection("MMHAC1").build()
        DBContainer mitmasRequest = mitmasQuery.getContainer()
        mitmasRequest.set("MMCONO", currentCompany)
        mitmasRequest.set("MMITNO", itno)

        if (mitmasQuery.read(mitmasRequest)) {
          String haci = mitmasRequest.getString("MMHAC1").trim()
          if (haci == "4.1" || haci == "4.2" || haci == "4.3") {
            results["DGX4"] = true
          }
        }
      }

    }
    return results
  }

  /**
   * Retrieve EXT034 data
   * @param zcod
   */
  public void getExt034Data(String zcod){
    if (ext034Map == null) {
      ext034Map = new HashMap<String, String[]>()
    }
    if (ext034Map.containsKey(zcod)) {
      return
    }
    DBAction ext034Query = database.table("EXT034").index("00").selection("EXZDES", "EXZCTY", "EXZSTY").build()
    DBContainer ext034Request = ext034Query.getContainer()
    ext034Request.set("EXCONO", currentCompany)
    ext034Request.set("EXZCOD", zcod)
    if (ext034Query.read(ext034Request)){
      String zcty = ext034Request.getString("EXZCTY").trim()
      String zsty = ext034Request.getString("EXZSTY").trim()
      ext034Map.put(zcod, [zcty, zsty].toArray(new String[0]))
    }
  }

}
