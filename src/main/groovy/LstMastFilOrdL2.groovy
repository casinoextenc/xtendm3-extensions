/****************************************************************************************
 Extension Name: EXT050MI.LstMastFilOrdL2
 Type: ExtendM3Transaction
 Script Author: SEAR
 Date: 2023-05-16
 Description:
 * List master file order line

 Revision History:
 Name                    Date             Version          Description of Changes
 SEAR                    2023-05-16       1.0              LOG28 - Creation of files and containers
 MLECLERCQ               2023-08-18       1.1              LOG28 - EXT050 ZAAM = OOLINE LNAM
 MLECLERCQ               2024-03-24       1.2              LOG28 - Filtres Qualité
 MLECLERCQ               2024-10-11       1.3              LOG28 - added nb of cols
 ARENARD                 2025-04-28       1.4              Extension has been fixed
 FLEBARS                 2025-05-05       1.5              Apply xtendm3 team remarks
 FLEBARS                 2025-06-27       1.6              Quality filter modification for LAIT & PETFOOD
 ******************************************************************************************/

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.math.RoundingMode

public class LstMastFilOrdL2 extends ExtendM3Transaction {
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
  private String whloOoline
  private String whloInput
  private String uca4Input
  private String uca5Input
  private String uca6Input
  private String cunoInput
  private Long dlixInput
  private Integer currentCompany
  private String cunoOoline
  private String ornoOoline
  private int ponrOoline
  private int posxOoline
  private double volume
  private double weight

  private String baseUnit
  private String itemNumber
  private String description
  private String dossier
  private String semaine
  private String annee
  private double saprOoline
  private double alqtOoline
  private double orqtOoline
  private int dmcsOoline
  private double cofsOoline
  private String spunOoline
  private String ltypOoline
  private double lnamOoline

  private String commande
  private String orstOoline
  private String custName
  private String custNumber
  private boolean allocMethod
  private Long lineIndex
  private int connMhdish
  private String rscdMhdish
  private double nbOfCols
  private double nbOfPals

  private Map<String, Boolean> quaFiltersInput
  private Map<String, String[]> ext034Map

  private Integer nbMaxRecord = 10000

  private String jobNumber

  public LstMastFilOrdL2(LoggerAPI logger, MIAPI mi, DatabaseAPI database, ProgramAPI program, MICallerAPI miCaller, UtilityAPI utility) {
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

    quaFiltersInput = new HashMap<String, Boolean>()

    //Get mi inputs
    whloInput = (mi.in.get("WHLO") != null ? (String) mi.in.get("WHLO") : "")
    uca4Input = (mi.in.get("UCA4") != null ? (String) mi.in.get("UCA4") : "")
    uca5Input = (mi.in.get("UCA5") != null ? (String) mi.in.get("UCA5") : "")
    uca6Input = (mi.in.get("UCA6") != null ? (String) mi.in.get("UCA6") : "")
    cunoInput = (mi.in.get("CUNO") != null ? (String) mi.in.get("CUNO") : "")
    dlixInput = (Long) (mi.in.get("DLIX") != null ? mi.in.get("DLIX") : 0)

    quaFiltersInput["PHYT"] = (mi.in.get("PHYT") != null ? (Boolean) mi.in.get("PHYT") : false)
    quaFiltersInput["SANI"] = (mi.in.get("SANI") != null ? (Boolean) mi.in.get("SANI") : false)
    quaFiltersInput["PETF"] = (mi.in.get("PETF") != null ? (Boolean) mi.in.get("PETF") : false)
    quaFiltersInput["LAIT"] = (mi.in.get("LAIT") != null ? (Boolean) mi.in.get("LAIT") : false)
    quaFiltersInput["ALCO"] = (mi.in.get("ALCO") != null ? (Boolean) mi.in.get("ALCO") : false)
    quaFiltersInput["BIER"] = (mi.in.get("BIER") != null ? (Boolean) mi.in.get("BIER") : false)
    quaFiltersInput["VIN1"] = (mi.in.get("VIN1") != null ? (Boolean) mi.in.get("VIN1") : false)
    quaFiltersInput["CHAM"] = (mi.in.get("CHAM") != null ? (Boolean) mi.in.get("CHAM") : false)
    quaFiltersInput["DGX1"] = (mi.in.get("DGX1") != null ? (Boolean) mi.in.get("DGX1") : false)
    quaFiltersInput["DGX4"] = (mi.in.get("DGX4") != null ? (Boolean) mi.in.get("DGX4") : false)
    quaFiltersInput["ISO1"] = (mi.in.get("ISO1") != null ? (Boolean) mi.in.get("ISO1") : false)
    quaFiltersInput["DPH1"] = (mi.in.get("DPH1") != null ? (Boolean) mi.in.get("DPH1") : false)


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
      DBAction mhdishQuery = database.table("MHDISH").index("00").selection("OQDLIX", "OQRSCD").build()
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

    DBAction ooheadQuery = database.table("OOHEAD").index("00").matching(ooheadExpression).selection("OAORNO", "OAUCA4", "OAUCA5", "OAUCA6").build()
    DBContainer ooheadRequest = ooheadQuery.getContainer()
    ooheadRequest.set("OACONO", currentCompany)

    if (!ooheadQuery.readAll(ooheadRequest, 1, 100, ooheadReader)) {
    }

    // list out data
    DBAction listQueryEXT051 = database.table("EXT051")
      .index("10")
      .selection(
        "EXBJNO",
        "EXCONO",
        "EXUCA4",
        "EXUCA5",
        "EXUCA6",
        "EXORNO",
        "EXPONR",
        "EXPOSX",
        "EXLTYP",
        "EXORST",
        "EXCUNO",
        "EXCUNM",
        "EXITNO",
        "EXITDS",
        "EXALQT",
        "EXGRWE",
        "EXVOL3",
        "EXZAAM",
        "EXDLIX",
        "EXCONN",
        "EXRSCD",
        "EXCOLS",
        "EXPALS"
      )
      .build()

    DBContainer listContainerEXT051 = listQueryEXT051.getContainer()
    listContainerEXT051.set("EXBJNO", jobNumber)

    //Record exists
    if (!listQueryEXT051.readAll(listContainerEXT051, 1, nbMaxRecord, ext051Reader)) {
    }

    // delete workfile
    DBAction delQuery = database.table("EXT051").index("00").build()
    DBContainer delContainerEXT051 = delQuery.getContainer()
    delContainerEXT051.set("EXBJNO", jobNumber)
    if (!delQuery.readAllLock(delContainerEXT051, 1, deleteCallBack)) {
      mi.error("L'enregistrement n'existe pas en EXT051")
      return
    }
  }

  /**
   * Retrieve OOHEAD data
   * @param itno
   * @param filterResults
   * @return
   */
  Closure<?> ooheadReader = { DBContainer ooheadResult ->
    commande = ooheadResult.get("OAORNO")
    dossier = ooheadResult.get("OAUCA4")
    semaine = ooheadResult.get("OAUCA5")
    annee = ooheadResult.get("OAUCA6")

    // Get OOLINE
    ExpressionFactory oolineExpression = database.getExpressionFactory("OOLINE")
    oolineExpression = (oolineExpression.lt("OBORST", "44"))
    oolineExpression = oolineExpression.and(oolineExpression.gt("OBORST", "22"))

    DBAction oolineQuery = database.table("OOLINE").index("00").matching(oolineExpression).selection("OBCUNO", "OBORNO", "OBPONR", "OBPOSX", "OBWHLO", "OBSPUN", "OBDMCS", "OBCOFS", "OBORQT", "OBALQT", "OBSAPR", "OBLTYP", "OBORST", "OBITNO", "OBLNAM").build()
    DBContainer oolineRequest = oolineQuery.getContainer()
    oolineRequest.set("OBCONO", currentCompany)
    oolineRequest.set("OBORNO", commande)
    if (oolineQuery.readAll(oolineRequest, 2, nbMaxRecord, oolineReader)) {
    }
    logger.debug("commande : " + commande)
    logger.debug("dossier : " + dossier + "semaine : " + semaine + "annee : " + annee)
    logger.debug("sameWarehouse : " + sameWarehouse)
  }

  /**
   * Retrieve OOLINE data
   * @param containerOoline
   */
  Closure<?> oolineReader = { DBContainer oolineResult ->
    whloOoline = oolineResult.get("OBWHLO")
    cunoOoline = oolineResult.get("OBCUNO")
    ornoOoline = oolineResult.get("OBORNO")
    ponrOoline = oolineResult.get("OBPONR")
    posxOoline = oolineResult.get("OBPOSX")
    spunOoline = oolineResult.get("OBSPUN")
    cofsOoline = oolineResult.get("OBCOFS")
    orqtOoline = oolineResult.get("OBORQT")
    alqtOoline = oolineResult.get("OBALQT")
    saprOoline = oolineResult.get("OBSAPR")
    dmcsOoline = oolineResult.get("OBDMCS")
    ltypOoline = oolineResult.get("OBLTYP")
    orstOoline = oolineResult.get("OBORST")
    lnamOoline = oolineResult.get("OBLNAM")
    itemNumber = oolineResult.get("OBITNO")

    sameWarehouse = false
    sameIndex = false
    sameCustomer = false
    foundLineIndex = false

    if (whloOoline.equals(whloInput)) {
      sameWarehouse = true
    }
    logger.debug("OOLINE sameWarehouse = " + sameWarehouse)

    if (cunoInput.length() > 0) {
      if (cunoInput.trim().equals(cunoOoline.trim())) {
        sameCustomer = true
      }
    } else {
      sameCustomer = true
    }

    logger.debug("OOLINE sameCustomer = " + sameCustomer)

    // check index number
    lineIndex = 0
    if ((mi.in.get("DLIX") != null)) {
      ExpressionFactory mhdislQuery = database.getExpressionFactory("MHDISL")
      mhdislQuery = mhdislQuery.eq("URDLIX", dlixInput.toString())
      DBAction queryMhdisl = database.table("MHDISL").index("10").matching(mhdislQuery).selection("URDLIX").build()
      DBContainer mhdislRequest = queryMhdisl.getContainer()
      mhdislRequest.set("URCONO", currentCompany)
      mhdislRequest.set("URRORC", 3)
      mhdislRequest.set("URRIDN", ornoOoline)
      mhdislRequest.set("URRIDL", ponrOoline)
      if (queryMhdisl.readAll(mhdislRequest, 4, nbMaxRecord, mhdislReader)) {
      }
    } else {
      DBAction mhdislQuery = database.table("MHDISL").index("10").selection("URDLIX").build()
      DBContainer mhdislRequest = mhdislQuery.getContainer()
      mhdislRequest.set("URCONO", currentCompany)
      mhdislRequest.set("URRORC", 3)
      mhdislRequest.set("URRIDN", ornoOoline)
      mhdislRequest.set("URRIDL", ponrOoline)
      if (mhdislQuery.readAll(mhdislRequest, 4, nbMaxRecord, mhdislReader)) {
      }
    }


    if ((mi.in.get("DLIX") == null)) {
      sameIndex = true
    }
    logger.debug("ornoOoline + PONR = " + ornoOoline + ";" + ponrOoline)
    logger.debug("itemNumber = " + itemNumber)
    logger.debug("whloOoline = " + whloOoline)
    logger.debug("foundLineIndex = " + foundLineIndex)
    connMhdish = 0
    if (foundLineIndex) {

      DBAction queryMhdish = database.table("MHDISH").index("00").selection("OQDLIX", "OQCONN", "OQRSCD").build()
      DBContainer MHDISH = queryMhdish.getContainer()
      MHDISH.set("OQCONO", currentCompany)
      MHDISH.set("OQINOU", 1)
      MHDISH.set("OQDLIX", lineIndex)
      if (queryMhdish.read(MHDISH)) {
        connMhdish = MHDISH.get("OQCONN")
        rscdMhdish = MHDISH.get("OQRSCD")
      }
    }

    // get allocation method in MITBAL
    allocMethod = false
    DBAction mitbalQuery = database.table("MITBAL").index("00").selection("MBALMT").build()
    DBContainer mitbalRequest = mitbalQuery.getContainer()
    mitbalRequest.set("MBCONO", currentCompany)
    mitbalRequest.set("MBITNO", itemNumber)
    mitbalRequest.set("MBWHLO", whloOoline)
    if (mitbalQuery.read(mitbalRequest)) {
      if (6 == mitbalRequest.getInt("MBALMT") || 7 == mitbalRequest.getInt("MBALMT")) {
        allocMethod = true
      }
      logger.debug("found MITBAL with ALMT = " + mitbalRequest.getInt("MBALMT"))
    }

    if (sameIndex && sameWarehouse && sameCustomer && allocMethod) {
      logger.debug("samedata")

      if ((mi.in.get("DLIX") != null)) {
        DBAction mhdishQuery = database.table("MHDISH").index("00").selection("OQDLIX").build()
        DBContainer mhdishRequest = mhdishQuery.getContainer()
        mhdishRequest.set("OQCONO", currentCompany)
        mhdishRequest.set("OQINOU", 1)
        mhdishRequest.set("OQDLIX", dlixInput)
        if (!mhdishQuery.read(mhdishRequest)) {
        }
      }
      // get OCUSMA
      DBAction ocusmaQuery = database.table("OCUSMA").index("00").selection("OKCUNO", "OKCUNM").build()
      DBContainer ocusmaRequest = ocusmaQuery.getContainer()
      ocusmaRequest.set("OKCONO", currentCompany)
      ocusmaRequest.set("OKCUNO", cunoOoline)
      if (ocusmaQuery.read(ocusmaRequest)) {
        custName = ocusmaRequest.get("OKCUNM")
        custNumber = ocusmaRequest.get("OKCUNO")
      }

      // get MITMAS
      baseUnit = ""
      DBAction mitmasQuery = database.table("MITMAS").index("00").selection("MMITDS", "MMPUUN", "MMUNMS", "MMGRWE", "MMVOL3", "MMSPUN").build()
      DBContainer mitmasRequest = mitmasQuery.getContainer()
      mitmasRequest.set("MMCONO", currentCompany)
      mitmasRequest.set("MMITNO", itemNumber)
      if (mitmasQuery.read(mitmasRequest)) {
        description = mitmasRequest.get("MMITDS")
        baseUnit = mitmasRequest.get("MMUNMS")
        volume = mitmasRequest.getDouble("MMVOL3")
        weight = mitmasRequest.getDouble("MMGRWE")

      }

      double ALQT = new BigDecimal(alqtOoline).setScale(6, RoundingMode.HALF_EVEN).doubleValue()
      double GRWE = new BigDecimal(alqtOoline * weight).setScale(6, RoundingMode.HALF_EVEN).doubleValue()
      double VOL3 = new BigDecimal(alqtOoline * volume).setScale(6, RoundingMode.HALF_EVEN).doubleValue()

      ExpressionFactory mitaunExpression = database.getExpressionFactory("MITAUN")
      mitaunExpression = mitaunExpression.eq("MUALUN","COL")
      mitaunExpression = mitaunExpression.or(mitaunExpression.eq("MUALUN","UPA"))
      DBAction mituanQuery = database.table("MITAUN").index("00").matching(mitaunExpression).selection("MUALUN","MUCOFA").build()
      DBContainer mitaunRequest = mituanQuery.getContainer()
      mitaunRequest.set("MUCONO", currentCompany)
      mitaunRequest.set("MUITNO", itemNumber)
      mitaunRequest.set("MUAUTP", 1)

      if(mituanQuery.readAll(mitaunRequest,3, nbMaxRecord, dataMITAUN)){
      }

      //Check if record exists
      DBAction ext051Query = database.table("EXT051")
        .index("00")
        .selection(
          "EXBJNO",
          "EXCONO",
          "EXUCA4",
          "EXUCA5",
          "EXUCA6",
          "EXORNO",
          "EXPONR",
          "EXPOSX",
          "EXLTYP",
          "EXORST",
          "EXCUNO",
          "EXCUNM",
          "EXITNO",
          "EXITDS",
          "EXALQT",
          "EXGRWE",
          "EXVOL3",
          "EXZAAM",
          "EXDLIX",
          "EXCONN",
          "EXRSCD",
          "EXCOLS",
          "EXPALS",
          "EXRGDT",
          "EXRGTM",
          "EXLMDT",
          "EXCHNO",
          "EXCHID"
        )
        .build()

      DBContainer ext051Request = ext051Query.getContainer()
      ext051Request.set("EXBJNO", jobNumber)
      ext051Request.set("EXCONO", currentCompany)
      ext051Request.set("EXUCA4", dossier)
      ext051Request.set("EXUCA5", semaine)
      ext051Request.set("EXUCA6", annee)
      ext051Request.set("EXORNO", commande)
      ext051Request.set("EXPONR", ponrOoline)
      ext051Request.set("EXPOSX", posxOoline)

      //Record exists
      if (!ext051Query.read(ext051Request)) {
        ext051Request.set("EXBJNO", jobNumber)
        ext051Request.set("EXCONO", currentCompany)
        ext051Request.set("EXUCA4", dossier)
        ext051Request.set("EXUCA5", semaine)
        ext051Request.set("EXUCA6", annee)
        ext051Request.set("EXORNO", commande)
        ext051Request.set("EXPONR", ponrOoline)
        ext051Request.set("EXPOSX", posxOoline)
        ext051Request.set("EXLTYP", ltypOoline)
        ext051Request.set("EXORST", orstOoline)
        ext051Request.set("EXCUNO", custNumber)
        ext051Request.set("EXCUNM", custName)
        ext051Request.set("EXITNO", itemNumber)
        ext051Request.set("EXITDS", description)
        ext051Request.set("EXALQT", ALQT)
        ext051Request.set("EXGRWE", GRWE)
        ext051Request.set("EXVOL3", VOL3)
        ext051Request.set("EXCOLS", nbOfCols)
        ext051Request.set("EXPALS", nbOfPals)
        ext051Request.set("EXZAAM", lnamOoline)
        ext051Request.set("EXDLIX", lineIndex)
        ext051Request.set("EXCONN", connMhdish)
        ext051Request.set("EXRSCD", rscdMhdish)
        ext051Request.set("EXRGDT", utility.call("DateUtil", "currentDateY8AsInt"))
        ext051Request.set("EXRGTM", utility.call("DateUtil", "currentTimeAsInt"))
        ext051Request.set("EXLMDT", utility.call("DateUtil", "currentDateY8AsInt"))
        ext051Request.set("EXCHNO", 1)
        ext051Request.set("EXCHID", program.getUser())
        ext051Query.insert(ext051Request)
      }
    }
  }

  /**
   * Retrieve MHDISL data
   * @param containerMHDISL
   */
  Closure<?> mhdislReader = { DBContainer containerMHDISL ->
    lineIndex = containerMHDISL.getLong("URDLIX")
    sameIndex = true
    foundLineIndex = true
  }

  /**
   * Retrieve MITAUN data
   * @param containerMITAUN
   */
  Closure<?> dataMITAUN = { DBContainer containerMITAUN ->
    double ALQT = new BigDecimal(alqtOoline).setScale(6, RoundingMode.HALF_EVEN).doubleValue()
    Double cofa = containerMITAUN.get("MUCOFA")
    String alun = containerMITAUN.get("MUALUN")
    logger.debug("MUALUNE = ${alun} ,MUCOFA = ${cofa}")

    if(alun.equals("COL")){
      nbOfCols = ALQT / (cofa != 0 ? cofa : 1)
      nbOfCols = new BigDecimal(nbOfCols).setScale(6, RoundingMode.HALF_EVEN).doubleValue()
      logger.debug("nbOfCols = ${nbOfCols} and ALQT = ${ALQT} with COFA = ${cofa}"  )
    }else if(alun.equals("UPA")){
      nbOfPals = ALQT / (cofa != 0 ? cofa : 1)
      nbOfPals = new BigDecimal(nbOfPals).setScale(6, RoundingMode.HALF_EVEN).doubleValue()
      logger.debug("nbOfPals = ${nbOfPals} and ALQT = ${ALQT} with COFA = ${cofa}")
    }


  }

  /**
   * Retrieve EXT051 data
   * @param containerEXT051
   */
  Closure<?> ext051Reader = { DBContainer ext051Result ->
    String dossierEXT051 = ext051Result.get("EXUCA4")
    String semaineEXT051 = ext051Result.get("EXUCA5")
    String anneeEXT051 = ext051Result.get("EXUCA6")
    String commandeEXT051 = ext051Result.get("EXORNO")
    String lineEXT051 = ext051Result.get("EXPONR")
    String suffixeEXT051 = ext051Result.get("EXPOSX")
    String lineTypeEXT051 = ext051Result.get("EXLTYP")
    String statusEXT051 = ext051Result.get("EXORST")
    String customerEXT051 = ext051Result.get("EXCUNO")
    String custNameEXT051 = ext051Result.get("EXCUNM")
    String itemEXT051 = ext051Result.get("EXITNO")
    String descriptionEXT051 = ext051Result.get("EXITDS")
    String allocatedEXT051 = ext051Result.get("EXALQT")
    String weightEXT051 = ext051Result.get("EXGRWE")
    String volumeEXT051 = ext051Result.get("EXVOL3")
    String amountEXT051 = ext051Result.get("EXZAAM")
    String indexEXT051 = ext051Result.get("EXDLIX")
    String shipmentEXT051 = ext051Result.get("EXCONN")
    String priorityEXT051 = ext051Result.get("EXRSCD")
    String colsEXT051 = ext051Result.get("EXCOLS")
    String palsEXT051 = ext051Result.get("EXPALS")
    String posxEXT051 = ext051Result.get("EXPOSX") as Integer

    boolean isRecordValid = false


    if (quaFiltersInput.containsValue(true)) {
      LinkedHashMap<String, Boolean> filterResults = new LinkedHashMap<>()
      for(key in quaFiltersInput.keySet()){
        if(quaFiltersInput.get(key) == true){
          filterResults.put(key,false)
        }
      }

      logger.debug("Before check alcool")

      if(quaFiltersInput.get("ALCO") || quaFiltersInput.get("BIER") || quaFiltersInput.get("VIN1")){
        filterResults = (LinkedHashMap<String, Boolean>)filterAlcool(itemEXT051, filterResults)
      }

      logger.debug("Before check cugex")
      if(quaFiltersInput.get("ISO1") || quaFiltersInput.get("DPH1")){

        filterResults = (LinkedHashMap<String, Boolean>)filterOnCugex(itemEXT051, filterResults)
      }

      for(resultKey in filterResults.keySet()){
        logger.debug("Result for key: ${resultKey} = " + filterResults.get(resultKey))
      }

      filterResults = (LinkedHashMap<String, Boolean>) filterOnExt037(commandeEXT051,lineEXT051, suffixeEXT051,filterResults )

      isRecordValid = filterResults.containsValue(false) ? false : true

    }else{
      isRecordValid = true
    }

    if(isRecordValid){
      mi.outData.put("UCA4", dossierEXT051)
      mi.outData.put("UCA5", semaineEXT051)
      mi.outData.put("UCA6", anneeEXT051)
      mi.outData.put("ORNO", commandeEXT051)
      mi.outData.put("PONR", lineEXT051)
      mi.outData.put("LTYP", lineTypeEXT051)
      mi.outData.put("ORST", statusEXT051)
      mi.outData.put("CUNO", customerEXT051)
      mi.outData.put("CUNM", custNameEXT051)
      mi.outData.put("ITNO", itemEXT051)
      mi.outData.put("ITDS", descriptionEXT051)
      mi.outData.put("ALQT", allocatedEXT051)
      mi.outData.put("GRWE", weightEXT051)
      mi.outData.put("VOL3", volumeEXT051)
      mi.outData.put("ZAAM", amountEXT051)
      mi.outData.put("DLIX", indexEXT051)
      mi.outData.put("CONN", shipmentEXT051)
      mi.outData.put("RSCD", priorityEXT051)
      mi.outData.put("COLS", colsEXT051)
      mi.outData.put("PALS", palsEXT051)
      mi.outData.put("POSX", posxEXT051)
      mi.write()
    }

  }

  /**
   * Delete callback
   * @param orno
   * @param ponr
   * @param posx
   * @param filterResults
   * @return
   */
  Closure<?> deleteCallBack = { LockedResult lockedResult ->
    lockedResult.delete()
  }

  /**
   * Filter alcool
   * @param orno
   * @param ponr
   * @param posx
   * @param filterResults
   * @return
   */
  private filterAlcool(String itno,Map<String, Boolean>filterResults){

    //Get suno from MITMAS
    DBAction queryMitmas = database.table("MITMAS").index("00").selection("MMITNO","MMSUNO", "MMCFI4").build()

    DBContainer containerMITMAS = queryMitmas.getContainer()
    containerMITMAS.set("MMCONO", currentCompany)
    containerMITMAS.set("MMITNO", itno)

    String suno = ""
    String popn = ""
    String orco = ""
    String cfi4 = ""

    if(queryMitmas.read(containerMITMAS)){
      suno = containerMITMAS.get("MMSUNO")
      cfi4 = containerMITMAS.get("MMCFI4")

      cfi4 = cfi4.trim()

      logger.debug("MITMAS CFI4 = ${cfi4}")

      //TODO : remove hard values
      if(cfi4 == "S" || cfi4 == "T"){

        filterResults["BIER"] = true
      }else if(cfi4 == "1" || cfi4 == "2" || cfi4 == "3" || cfi4 == "4" || cfi4 == "5" || cfi4 == "6" || cfi4 == "9"
        || cfi4 == "B" || cfi4 == "C" || cfi4 == "D" || cfi4 == "K" || cfi4 == "L" || cfi4 == "M" || cfi4 == "N"
        || cfi4 == "Q" || cfi4 == "U" || cfi4 == "X"
      ){
        filterResults["VIN1"] = true
      }

    }

    if(!quaFiltersInput.get("ALCO")){
      //Don't need to check more if not Alcool filter
      return filterResults
    }

    //Get item POPN
    ExpressionFactory expressionMITPOP = database.getExpressionFactory("MITPOP")
    expressionMITPOP = expressionMITPOP.eq("MPREMK", "SIGMA6")

    DBAction queryMitpop = database.table("MITPOP").index("00").matching(expressionMITPOP).selection("MPPOPN","MPITNO","MPREMK").build()
    DBContainer containerMITPOP = queryMitpop.getContainer()
    containerMITPOP.set("MPCONO", currentCompany)
    containerMITPOP.set("MPALWT", 1)
    containerMITPOP.set("MPALWQ", "")
    containerMITPOP.set("MPITNO", itno)

    if(queryMitpop.readAll(containerMITPOP,4,1, {DBContainer closureMITPOP ->
      popn = closureMITPOP.get("MPPOPN")
      logger.debug("In closureMITPOP, popn is : " + popn)
    } )){}

    DBAction queryMitfac = database.table("MITFAC").index("00").selection("M9FACI", "M9ITNO", "M9ORCO").build()
    DBContainer containerMITFAC = queryMitfac.getContainer()
    containerMITFAC.set("M9CONO", currentCompany)
    containerMITFAC.set("M9FACI", "E10")
    containerMITFAC.set("M9ITNO", itno)

    if(queryMitfac.read(containerMITFAC)){
      orco = containerMITFAC.get("M9ORCO")
      logger.debug("In closureMITFAC, orco is : " + orco)
    }

    DBAction queryExt032 = database.table("EXT032").index("00").selection("EXZALC", "EXPOPN","EXSUNO","EXORCO").build()
    DBContainer containerEXT032 = queryExt032.getContainer()
    containerEXT032.set("EXCONO", currentCompany)
    containerEXT032.set("EXPOPN", popn)
    containerEXT032.set("EXSUNO", suno)
    containerEXT032.set("EXORCO", orco)

    if(queryExt032.read(containerEXT032)){
      if(containerEXT032.get("EXZALC")){

        filterResults["ALCO"] = true
        logger.debug("Alcool is present for itno : " + itno)
      }
    }

    return filterResults
  }

  /**
   * Filter on CUGEX1
   * @param itno
   * @param filterResults
   * @return
   */
  private filterOnCugex(String itno, Map<String, Boolean> filterResults){

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
   * Filter on EXT037
   * @param orno
   * @param ponr
   * @param posx
   * @param filterResults
   * @return
   */
  private Map<String, Boolean> filterOnExt037(String orno, String ponr, String posx, Map<String, Boolean> filterResults){

    LinkedHashMap<String, Boolean> results = (LinkedHashMap<String, Boolean>)filterResults
    posx = posx == null ? 0 : posx

    DBAction ext037Query = database.table("EXT037").index("00").selection("EXORNO","EXPONR","EXITNO","EXZCOD", "EXZCTY").build()
    DBContainer ext037Request = ext037Query.getContainer()
    ext037Request.set("EXCONO", currentCompany)
    ext037Request.set("EXORNO", orno)
    ext037Request.set("EXPONR", ponr as Integer)
    ext037Request.set("EXPOSX", posx  as Integer)
    ext037Request.set("EXDLIX", 0)

    String zcty = ""
    String zcod = ""
    String itno = ""
    String zsty = ""

    logger.debug("Read EXT037 for ORNO:${orno} and PONR:${ponr}" )


    if(ext037Query.readAll(ext037Request, 5,100, {DBContainer ext037Result ->
      zcty = ext037Result.getString("EXZCTY").trim()
      zcod = ext037Result.getString("EXZCOD").trim()
      itno = ext037Result.getString("EXITNO").trim()
    })){
      logger.debug("In EXT037 ZCTY=${zcty} and ZCOD=${zcod} for ORNO:${orno} and PONR:${ponr}" )
      getExt034Data(zcod)
      zsty = ext034Map.get(zcod)[1]

      if(results.containsKey("PHYT") && zcty.trim() == "PHYTOSANITAIRE"){
        results["PHYT"] = true
      }

      if(results.containsKey("SANI") && zcty.trim() == "SANITAIRE"){
        results["SANI"] = true
      }

      if(results.containsKey("PETF") && zsty.trim() == "PETFOOD"){
        results["PETF"] = true
      }

      if(results.containsKey("LAIT") && zsty.trim() == "LAIT"){
        results["LAIT"] = true
      }

      if(results.containsKey("CHAM") && zcod.trim() == "CHA"){
        results["CHAM"] = true
      }

      if(results.containsKey("DGX1") && zcod.trim() == "DGX"){
        results["DGX1"] = true
      }

      if(results.containsKey("DGX4") && zcod.trim() == "DGX"){
        logger.debug("Check DGX4 for item ${itno} in MITMAS")
        DBAction mitmasQuery = database.table("MITMAS").index("00").selection("MMHAC1").build()
        DBContainer mitmasRequest = mitmasQuery.getContainer()
        mitmasRequest.set("MMCONO", currentCompany)
        mitmasRequest.set("MMITNO", itno)

        if(mitmasQuery.read(mitmasRequest)){
          String haci = mitmasRequest.getString("MMHAC1").trim()
          if(haci == "4.1" || haci == "4.2" || haci == "4.3"){
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
