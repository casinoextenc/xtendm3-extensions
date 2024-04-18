/**
 * README
 * This extension is used by Mashup
 *
 * Name : EXT050MI.LstMastFilOrdL2
 * Description : List master file order line
 * Date         Changed By   Description
 * 20230516     SEAR         LOG28 - Creation of files and containers
 * 20230818     MLECLERCQ    LOG28 - EXT050 ZAAM = OOLINE LNAM
 * 20240324     MLECLERCQ    LOG28 - Filtres Qualité
 */
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.math.RoundingMode

public class LstMastFilOrdL2 extends ExtendM3Transaction {
  private final MIAPI mi
  private final LoggerAPI logger
  private final ProgramAPI program
  private final DatabaseAPI database
  private final SessionAPI session
  private final TransactionAPI transaction
  private final MICallerAPI miCaller
  private final UtilityAPI utility
  private String parm
  private boolean validOrder
  private boolean sameWarehouse
  private boolean sameIndex
  private boolean foundLineIndex
  private boolean sameCustomer
  private String whlo_OOLINE
  private String whlo_input
  private String uca4_input
  private String uca5_input
  private String uca6_input
  private String cuno_input
  private Long dlix_input
  private Integer currentCompany
  private String cuno_OOLINE
  private String orno_OOLINE
  private int ponr_OOLINE
  private int posx_OOLINE
  private double volume
  private double weight
  private double salesPrice
  private String baseUnit
  private String ItemNumber
  private String description
  private String dossier
  private String semaine
  private String annee
  private double sapr_OOLINE
  private double alqt_OOLINE
  private double orqt_OOLINE
  private int dmcs_OOLINE
  private double cofs_OOLINE
  private String spun_OOLINE
  private String ltyp_OOLINE
  private double lnam_OOLINE
  private double allocatedQuantity
  private double allocatedQuantityUB
  private String commande
  private String orst_OOLINE
  private String cust_name
  private String cust_number
  private boolean allocMethod
  private Long lineIndex
  private int conn_MHDISH
  private String rscd_MHDISH //20230821 MLQ

  private Map<String, Boolean> quaFilters_input

  private boolean hasBeer = false
  private boolean hasWine = false
  private boolean isDPH = false
  private boolean isISO = false
  private boolean isPhyto = false
  private boolean isSanit = false
  private boolean isPetFood = false
  private boolean isMilk = false
  private boolean isChamp = false
  private boolean isDgx = false
  private boolean isDgx4 = false



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

    quaFilters_input = new HashMap<String, Boolean>()

    //Get mi inputs
    whlo_input = (mi.in.get("WHLO") != null ? (String) mi.in.get("WHLO") : "")
    uca4_input = (mi.in.get("UCA4") != null ? (String) mi.in.get("UCA4") : "")
    uca5_input = (mi.in.get("UCA5") != null ? (String) mi.in.get("UCA5") : "")
    uca6_input = (mi.in.get("UCA6") != null ? (String) mi.in.get("UCA6") : "")
    cuno_input = (mi.in.get("CUNO") != null ? (String) mi.in.get("CUNO") : "")
    dlix_input = (Long) (mi.in.get("DLIX") != null ? mi.in.get("DLIX") : 0)

    quaFilters_input["PHYT"] = (mi.in.get("PHYT") != null ? (Boolean) mi.in.get("PHYT") : false)
    quaFilters_input["SANI"] = (mi.in.get("SANI") != null ? (Boolean) mi.in.get("SANI") : false)
    quaFilters_input["PETF"] = (mi.in.get("PETF") != null ? (Boolean) mi.in.get("PETF") : false)
    quaFilters_input["LAIT"] = (mi.in.get("LAIT") != null ? (Boolean) mi.in.get("LAIT") : false)
    quaFilters_input["ALCO"] = (mi.in.get("ALCO") != null ? (Boolean) mi.in.get("ALCO") : false)
    quaFilters_input["BIER"] = (mi.in.get("BIER") != null ? (Boolean) mi.in.get("BIER") : false)
    quaFilters_input["VIN1"] = (mi.in.get("VIN1") != null ? (Boolean) mi.in.get("VIN1") : false)
    quaFilters_input["CHAM"] = (mi.in.get("CHAM") != null ? (Boolean) mi.in.get("CHAM") : false)
    quaFilters_input["DGX1"] = (mi.in.get("DGX1") != null ? (Boolean) mi.in.get("DGX1") : false)
    quaFilters_input["DGX4"] = (mi.in.get("DGX4") != null ? (Boolean) mi.in.get("DGX4") : false)
    quaFilters_input["ISO1"] = (mi.in.get("ISO1") != null ? (Boolean) mi.in.get("ISO1") : false)
    quaFilters_input["DPH1"] = (mi.in.get("DPH1") != null ? (Boolean) mi.in.get("DPH1") : false)


    // check warehouse
    DBAction query_MITWHL = database.table("MITWHL").index("00").selection("MWWHLO").build()
    DBContainer MITWHL = query_MITWHL.getContainer()
    MITWHL.set("MWCONO", currentCompany)
    MITWHL.set("MWWHLO", whlo_input)
    if (!query_MITWHL.read(MITWHL)) {
      mi.error("Le dépôt " + whlo_input + " n'existe pas")
    }

    // check customer number
    if (cuno_input.length() > 0) {
      DBAction query_OCUSMA = database.table("OCUSMA").index("00").selection("OKCUNO").build()
      DBContainer OCUSMA = query_OCUSMA.getContainer()
      OCUSMA.set("OKCONO", currentCompany)
      OCUSMA.set("OKCUNO", cuno_input)
      if (!query_OCUSMA.read(OCUSMA)) {
        mi.error("Le code client  " + cuno_input + " n'existe pas")
        return
      }
    }

    // check index number
    if ((mi.in.get("DLIX") != null)) {
      DBAction query_MHDISH = database.table("MHDISH").index("00").selection("OQDLIX", "OQRSCD").build()
      DBContainer MHDISH = query_MHDISH.getContainer()
      MHDISH.set("OQCONO", currentCompany)
      MHDISH.set("OQINOU", 1)
      MHDISH.set("OQDLIX", dlix_input)
      if (!query_MHDISH.read(MHDISH)) {
        mi.error("Index de livraison  " + dlix_input + " n'existe pas")
        return
      }
    }

    ExpressionFactory expression_OOHEAD = database.getExpressionFactory("OOHEAD")
    if (uca4_input != "") {
      expression_OOHEAD = expression_OOHEAD.eq("OAUCA4", uca4_input)
    } else {
      expression_OOHEAD = expression_OOHEAD.ne("OAUCA4", "")
    }
    if (uca5_input != "") {
      expression_OOHEAD = expression_OOHEAD.and(expression_OOHEAD.eq("OAUCA5", uca5_input))
    } else {
      expression_OOHEAD = expression_OOHEAD.and(expression_OOHEAD.ne("OAUCA5", ""))
    }
    if (uca6_input != "") {
      expression_OOHEAD = expression_OOHEAD.and(expression_OOHEAD.eq("OAUCA6", uca6_input))
    } else {
      expression_OOHEAD = expression_OOHEAD.and(expression_OOHEAD.ne("OAUCA6", ""))
    }

    DBAction query_OOHEAD = database.table("OOHEAD").index("01").matching(expression_OOHEAD).selection("OAORNO", "OAUCA4", "OAUCA5", "OAUCA6").build()
    DBContainer containerOOHEAD = query_OOHEAD.getContainer()
    containerOOHEAD.set("OACONO", currentCompany)

    if (!query_OOHEAD.readAll(containerOOHEAD, 1, OOHEADData)) {
    }

    // list out data
    DBAction ListqueryEXT051 = database.table("EXT051")
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
        "EXRSCD" //20230821 MLQ
      )
      .build()

    DBContainer ListContainerEXT051 = ListqueryEXT051.getContainer()
    ListContainerEXT051.set("EXBJNO", jobNumber)

    //Record exists
    if (!ListqueryEXT051.readAll(ListContainerEXT051, 1, outData)) {
    }

    // delete workfile
    DBAction DelQuery = database.table("EXT051").index("00").build()
    DBContainer DelcontainerEXT051 = DelQuery.getContainer()
    DelcontainerEXT051.set("EXBJNO", jobNumber)
    if (!DelQuery.readAllLock(DelcontainerEXT051, 1, deleteCallBack)) {
      mi.error("L'enregistrement n'existe pas")
      return
    }
  }

  // liste OOHEAD
  Closure<?> OOHEADData = { DBContainer containerOOHEAD ->

    int company = containerOOHEAD.get("OACONO")
    commande = containerOOHEAD.get("OAORNO")
    dossier = containerOOHEAD.get("OAUCA4")
    semaine = containerOOHEAD.get("OAUCA5")
    annee = containerOOHEAD.get("OAUCA6")

    // Get OOLINE
    ExpressionFactory expression_OOLINE = database.getExpressionFactory("OOLINE")
    expression_OOLINE = (expression_OOLINE.lt("OBORST", "44"))
    expression_OOLINE = expression_OOLINE.and(expression_OOLINE.gt("OBORST", "22"))

    DBAction query_OOLINE = database.table("OOLINE").index("00").matching(expression_OOLINE).selection("OBCUNO", "OBORNO", "OBPONR", "OBPOSX", "OBWHLO", "OBSPUN", "OBDMCS", "OBCOFS", "OBORQT", "OBALQT", "OBSAPR", "OBLTYP", "OBORST", "OBITNO", "OBLNAM").build()
    DBContainer OOLINE = query_OOLINE.getContainer()
    OOLINE.set("OBCONO", company)
    OOLINE.set("OBORNO", commande)
    if (query_OOLINE.readAll(OOLINE, 2, OOLINEData)) {
    }


    logger.debug("commande : " + commande)
    logger.debug("dossier : " + dossier + "semaine : " + semaine + "annee : " + annee)
    logger.debug("sameWarehouse : " + sameWarehouse)
  }

  // data OOLINE
  Closure<?> OOLINEData = { DBContainer ContainerOOLINE ->
    whlo_OOLINE = ContainerOOLINE.get("OBWHLO")
    cuno_OOLINE = ContainerOOLINE.get("OBCUNO")
    orno_OOLINE = ContainerOOLINE.get("OBORNO")
    ponr_OOLINE = ContainerOOLINE.get("OBPONR")
    posx_OOLINE = ContainerOOLINE.get("OBPOSX")
    spun_OOLINE = ContainerOOLINE.get("OBSPUN")
    cofs_OOLINE = ContainerOOLINE.get("OBCOFS")
    orqt_OOLINE = ContainerOOLINE.get("OBORQT")
    alqt_OOLINE = ContainerOOLINE.get("OBALQT")
    sapr_OOLINE = ContainerOOLINE.get("OBSAPR")
    dmcs_OOLINE = ContainerOOLINE.get("OBDMCS")
    ltyp_OOLINE = ContainerOOLINE.get("OBLTYP")
    orst_OOLINE = ContainerOOLINE.get("OBORST")
    lnam_OOLINE = ContainerOOLINE.get("OBLNAM")
    ItemNumber = ContainerOOLINE.get("OBITNO")

    sameWarehouse = false
    sameIndex = false
    sameCustomer = false
    foundLineIndex = false

    if (whlo_OOLINE.equals(whlo_input)) {
      sameWarehouse = true
    }
    logger.debug("sameWarehouse = " + sameWarehouse)

    if (cuno_input.length() > 0) {
      if (cuno_input.trim().equals(cuno_OOLINE.trim())) {
        sameCustomer = true
      }
    } else {
      sameCustomer = true
    }

    logger.debug("sameCustomer = " + sameCustomer)

    // check index number
    lineIndex = 0
    if ((mi.in.get("DLIX") != null)) {
      ExpressionFactory expression_MHDISL = database.getExpressionFactory("MHDISL")
      expression_MHDISL = expression_MHDISL.eq("URDLIX", dlix_input.toString())
      DBAction query_MHDISL = database.table("MHDISL").index("10").matching(expression_MHDISL).selection("URDLIX").build()
      DBContainer MHDISL = query_MHDISL.getContainer()
      MHDISL.set("URCONO", currentCompany)
      MHDISL.set("URRORC", 3)
      MHDISL.set("URRIDN", orno_OOLINE)
      MHDISL.set("URRIDL", ponr_OOLINE)
      if (query_MHDISL.readAll(MHDISL, 4, DataMHDISL)) {
      }
    } else {
      DBAction query_MHDISL = database.table("MHDISL").index("10").selection("URDLIX").build()
      DBContainer MHDISL = query_MHDISL.getContainer()
      MHDISL.set("URCONO", currentCompany)
      MHDISL.set("URRORC", 3)
      MHDISL.set("URRIDN", orno_OOLINE)
      MHDISL.set("URRIDL", ponr_OOLINE)
      if (query_MHDISL.readAll(MHDISL, 4, DataMHDISL)) {
      }
    }


    if ((mi.in.get("DLIX") == null)) {
      sameIndex = true
    }
    logger.debug("orno_OOLINE + PONR = " + orno_OOLINE + ";" + ponr_OOLINE)
    logger.debug("ItemNumber = " + ItemNumber)
    logger.debug("whlo_OOLINE = " + whlo_OOLINE)
    logger.debug("foundLineIndex = " + foundLineIndex)
    conn_MHDISH = 0
    if (foundLineIndex) {

      DBAction query_MHDISH = database.table("MHDISH").index("00").selection("OQDLIX", "OQCONN", "OQRSCD").build()
      DBContainer MHDISH = query_MHDISH.getContainer()
      MHDISH.set("OQCONO", currentCompany)
      MHDISH.set("OQINOU", 1)
      MHDISH.set("OQDLIX", lineIndex)
      if (query_MHDISH.read(MHDISH)) {
        conn_MHDISH = MHDISH.get("OQCONN")
        rscd_MHDISH = MHDISH.get("OQRSCD") //20230821 MLQ
      }
    }

    // get allocation method in MITBAL
    allocMethod = false
    DBAction query_MITBAL = database.table("MITBAL").index("00").selection("MBALMT").build()
    DBContainer MITBAL = query_MITBAL.getContainer()
    MITBAL.set("MBCONO", currentCompany)
    MITBAL.set("MBITNO", ItemNumber)
    MITBAL.set("MBWHLO", whlo_OOLINE)
    if (query_MITBAL.read(MITBAL)) {
      if (6 == MITBAL.getInt("MBALMT") || 7 == MITBAL.getInt("MBALMT")) {
        allocMethod = true
      }
      logger.debug("found MITBAL with ALMT = " + MITBAL.getInt("MBALMT"))
    }

    if (sameIndex && sameWarehouse && sameCustomer && allocMethod) {
      logger.debug("samedata")

      if ((mi.in.get("DLIX") != null)) {
        DBAction query_MHDISH = database.table("MHDISH").index("00").selection("OQDLIX").build()
        DBContainer MHDISH = query_MHDISH.getContainer()
        MHDISH.set("OQCONO", currentCompany)
        MHDISH.set("OQINOU", 1)
        MHDISH.set("OQDLIX", dlix_input)
        if (!query_MHDISH.read(MHDISH)) {
          // mi.error("Index de livraison  " + dlix_input + " n'existe pas")
        }
      }
      // get OCUSMA
      DBAction query_OCUSMA = database.table("OCUSMA").index("00").selection("OKCUNO", "OKCUNM").build()
      DBContainer OCUSMA = query_OCUSMA.getContainer()
      OCUSMA.set("OKCONO", currentCompany)
      OCUSMA.set("OKCUNO", cuno_OOLINE)
      if (query_OCUSMA.read(OCUSMA)) {
        cust_name = OCUSMA.get("OKCUNM")
        cust_number = OCUSMA.get("OKCUNO")
      }

      // get MITMAS
      baseUnit = ""
      DBAction query_MITMAS = database.table("MITMAS").index("00").selection("MMITDS", "MMPUUN", "MMUNMS", "MMGRWE", "MMVOL3", "MMSPUN").build()
      DBContainer MITMAS = query_MITMAS.getContainer()
      MITMAS.set("MMCONO", currentCompany)
      MITMAS.set("MMITNO", ItemNumber)
      if (query_MITMAS.read(MITMAS)) {
        description = MITMAS.get("MMITDS")
        baseUnit = MITMAS.get("MMUNMS")
        volume = MITMAS.getDouble("MMVOL3")
        weight = MITMAS.getDouble("MMGRWE")

      }

      double ALQT = new BigDecimal(alqt_OOLINE).setScale(6, RoundingMode.HALF_EVEN).doubleValue()
      double GRWE = new BigDecimal(alqt_OOLINE * weight).setScale(6, RoundingMode.HALF_EVEN).doubleValue()
      double VOL3 = new BigDecimal(alqt_OOLINE * volume).setScale(6, RoundingMode.HALF_EVEN).doubleValue()
      //double ZAAM = new BigDecimal (alqt_OOLINE * sapr_OOLINE).setScale(6, RoundingMode.HALF_EVEN).doubleValue()

      //Check if record exists
      DBAction queryEXT051 = database.table("EXT051")
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
          "EXRSCD", //20230821 MLQ
          "EXRGDT",
          "EXRGTM",
          "EXLMDT",
          "EXCHNO",
          "EXCHID"
        )
        .build()

      DBContainer containerEXT051 = queryEXT051.getContainer()
      containerEXT051.set("EXBJNO", jobNumber)
      containerEXT051.set("EXCONO", currentCompany)
      containerEXT051.set("EXUCA4", dossier)
      containerEXT051.set("EXUCA5", semaine)
      containerEXT051.set("EXUCA6", annee)
      containerEXT051.set("EXORNO", commande)
      containerEXT051.set("EXPONR", ponr_OOLINE)
      containerEXT051.set("EXPOSX", posx_OOLINE)

      //Record exists
      if (!queryEXT051.read(containerEXT051)) {
        containerEXT051.set("EXBJNO", jobNumber)
        containerEXT051.set("EXCONO", currentCompany)
        containerEXT051.set("EXUCA4", dossier)
        containerEXT051.set("EXUCA5", semaine)
        containerEXT051.set("EXUCA6", annee)
        containerEXT051.set("EXORNO", commande)
        containerEXT051.set("EXPONR", ponr_OOLINE)
        containerEXT051.set("EXPOSX", posx_OOLINE)
        containerEXT051.set("EXLTYP", ltyp_OOLINE)
        containerEXT051.set("EXORST", orst_OOLINE)
        containerEXT051.set("EXCUNO", cust_number)
        containerEXT051.set("EXCUNM", cust_name)
        containerEXT051.set("EXITNO", ItemNumber)
        containerEXT051.set("EXITDS", description)
        containerEXT051.set("EXALQT", ALQT)
        containerEXT051.set("EXGRWE", GRWE)
        containerEXT051.set("EXVOL3", VOL3)
        containerEXT051.set("EXZAAM", lnam_OOLINE)
        containerEXT051.set("EXDLIX", lineIndex)
        containerEXT051.set("EXCONN", conn_MHDISH)
        containerEXT051.set("EXRSCD", rscd_MHDISH) //20230821 MLQ
        containerEXT051.set("EXRGDT", utility.call("DateUtil", "currentDateY8AsInt"))
        containerEXT051.set("EXRGTM", utility.call("DateUtil", "currentTimeAsInt"))
        containerEXT051.set("EXLMDT", utility.call("DateUtil", "currentDateY8AsInt"))
        containerEXT051.set("EXCHNO", 1)
        containerEXT051.set("EXCHID", program.getUser())
        queryEXT051.insert(containerEXT051)
      }
    }

  }

  Closure<?> DataMHDISL = { DBContainer containerMHDISL ->
    lineIndex = containerMHDISL.getLong("URDLIX")
    sameIndex = true
    foundLineIndex = true
  }

  Closure<?> outData = { DBContainer containerEXT051 ->
    String dossierEXT051 = containerEXT051.get("EXUCA4")
    String semaineEXT051 = containerEXT051.get("EXUCA5")
    String anneeEXT051 = containerEXT051.get("EXUCA6")
    String commandeEXT051 = containerEXT051.get("EXORNO")
    String lineEXT051 = containerEXT051.get("EXPONR")
    String suffixeEXT051 = containerEXT051.get("EXPOSX")
    String lineTypeEXT051 = containerEXT051.get("EXLTYP")
    String statusEXT051 = containerEXT051.get("EXORST")
    String customerEXT051 = containerEXT051.get("EXCUNO")
    String custNameEXT051 = containerEXT051.get("EXCUNM")
    String itemEXT051 = containerEXT051.get("EXITNO")
    String descriptionEXT051 = containerEXT051.get("EXITDS")
    String allocatedEXT051 = containerEXT051.get("EXALQT")
    String weightEXT051 = containerEXT051.get("EXGRWE")
    String volumeEXT051 = containerEXT051.get("EXVOL3")
    String amountEXT051 = containerEXT051.get("EXZAAM")
    String indexEXT051 = containerEXT051.get("EXDLIX")
    String shipmentEXT051 = containerEXT051.get("EXCONN")
    String priorityEXT051 = containerEXT051.get("EXRSCD") //20230821 MLQ
    String posxEXT051 = containerEXT051.get("EXPOSX") as Integer //20230821 MLQ

    boolean isRecordValid = false


    if (quaFilters_input.containsValue(true)) {

      LinkedHashMap<String, Boolean> filterResults = new LinkedHashMap<>()
      for(key in quaFilters_input.keySet()){
        if(quaFilters_input.get(key) == true){
          filterResults.put(key,false)
        }
      }

      logger.debug("Before check alcool")

      if(quaFilters_input.get("ALCO") || quaFilters_input.get("BIER") || quaFilters_input.get("VIN1")){
        filterResults = (LinkedHashMap<String, Boolean>)filterAlcool(itemEXT051, filterResults)
      }

      logger.debug("Before check cugex")
      if(quaFilters_input.get("ISO1") || quaFilters_input.get("DPH1")){

        filterResults = (LinkedHashMap<String, Boolean>)filterOnCugex(itemEXT051, filterResults)
      }

      for(resultKey in filterResults.keySet()){
        logger.debug("Result for key: ${resultKey} = " + filterResults.get(resultKey))
      }

      filterResults = (LinkedHashMap<String, Boolean>) filterOnEXT036(commandeEXT051,lineEXT051, suffixeEXT051,filterResults )

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
      mi.outData.put("RSCD", priorityEXT051) //20230821 MLQ
      mi.outData.put("POSX", posxEXT051)
      mi.write()
    }

  }

  Closure<?> deleteCallBack = { LockedResult lockedResult ->
    lockedResult.delete()
  }

  private filterAlcool(itno,filterResults){

    //Get suno from MITMAS
    DBAction query_MITMAS = database.table("MITMAS").index("00").selection("MMITNO","MMSUNO", "MMCFI4").build()

    DBContainer containerMITMAS = query_MITMAS.getContainer()
    containerMITMAS.set("MMCONO", currentCompany)
    containerMITMAS.set("MMITNO", itno)

    String suno = ""
    String popn = ""
    String orco = ""
    String cfi4 = ""

    if(query_MITMAS.read(containerMITMAS)){
      suno = containerMITMAS.get("MMSUNO")
      cfi4 = containerMITMAS.get("MMCFI4")

      cfi4 = cfi4.trim()

      logger.debug("MITMAS CFI4 = ${cfi4}")

      //TODO : remove hard values
      if(cfi4 == "S" || cfi4 == "T"){
        /*hasBeer = true
        hasWine = false*/

        filterResults["BIER"] = true
      }else if(cfi4 == "1" || cfi4 == "2" || cfi4 == "3" || cfi4 == "4" || cfi4 == "5" || cfi4 == "6" || cfi4 == "9"
        || cfi4 == "B" || cfi4 == "C" || cfi4 == "D" || cfi4 == "K" || cfi4 == "L" || cfi4 == "M" || cfi4 == "N"
        || cfi4 == "Q" || cfi4 == "U" || cfi4 == "X"
      ){
        filterResults["VIN1"] = true
      }

    }

    if(!quaFilters_input.get("ALCO")){
      //Don't need to check more if not Alcool filter
      return filterResults
    }

    //Get item POPN
    ExpressionFactory expressionMITPOP = database.getExpressionFactory("MITPOP")
    expressionMITPOP = expressionMITPOP.eq("MPREMK", "SIGMA6")

    DBAction query_MITPOP = database.table("MITPOP").index("00").matching(expressionMITPOP).selection("MPPOPN","MPITNO","MPREMK").build()
    DBContainer containerMITPOP = query_MITPOP.getContainer()
    containerMITPOP.set("MPCONO", currentCompany)
    containerMITPOP.set("MPALWT", 1)
    containerMITPOP.set("MPALWQ", "")
    containerMITPOP.set("MPITNO", itno)

    if(query_MITPOP.readAll(containerMITPOP,4,1, {DBContainer closureMITPOP ->
      popn = closureMITPOP.get("MPPOPN")
      logger.debug("In closureMITPOP, popn is : " + popn)
    } )){}

    DBAction query_MITFAC = database.table("MITFAC").index("00").selection("M9FACI", "M9ITNO", "M9ORCO").build()
    DBContainer containerMITFAC = query_MITFAC.getContainer()
    containerMITFAC.set("M9CONO", currentCompany)
    containerMITFAC.set("M9FACI", "E10")
    containerMITFAC.set("M9ITNO", itno)

    if(query_MITFAC.read(containerMITFAC)){
      orco = containerMITFAC.get("M9ORCO")
      logger.debug("In closureMITFAC, orco is : " + orco)
    }

    DBAction query_EXT032 = database.table("EXT032").index("00").selection("EXZALC", "EXPOPN","EXSUNO","EXORCO").build()
    DBContainer containerEXT032 = query_EXT032.getContainer()
    containerEXT032.set("EXCONO", currentCompany)
    containerEXT032.set("EXPOPN", popn)
    containerEXT032.set("EXSUNO", suno)
    containerEXT032.set("EXORCO", orco)

    if(query_EXT032.read(containerEXT032)){
      if(containerEXT032.get("EXZALC")){

        filterResults["ALCO"] = true
        logger.debug("Alcool is present for itno : " + itno)
      }
    }

    return filterResults
  }

  private filterOnCugex(itno, filterResults){

    LinkedHashMap<String, Boolean> results = (LinkedHashMap<String, Boolean>)filterResults

    DBAction query_CUGEX1 = database.table("CUGEX1").index("00").selection("F1CHB9", "F1CHB6").build()
    DBContainer CUGEX1 = query_CUGEX1.getContainer()
    CUGEX1.set("F1CONO", currentCompany)
    CUGEX1.set("F1FILE", "MITMAS")
    CUGEX1.set("F1PK01", itno)

    logger.debug("In function filterOnCugex with ITNO: ${itno}")

    int chb6 = 0
    int chb9 = 0

    if(query_CUGEX1.readAll(CUGEX1,3,1, { DBContainer closureCugex1 ->
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


  private filterOnEXT036(orno, ponr, posx, filterResults){

    LinkedHashMap<String, Boolean> results = (LinkedHashMap<String, Boolean>)filterResults
    posx = posx == null ? 0 : posx

    DBAction query_EXT036 = database.table("EXT036").index("00").selection("EXORNO","EXPONR","EXITNO","EXZSTY", "EXZCTY").build()
    DBContainer containerEXT036 = query_EXT036.getContainer()
    containerEXT036.set("EXCONO", currentCompany)
    containerEXT036.set("EXORNO", orno)
    containerEXT036.set("EXPONR", ponr as Integer)
    containerEXT036.set("EXPOSX", posx  as Integer)

    String zcty = ""
    String zsty = ""
    String itno = ""
    if(query_EXT036.readAll(containerEXT036, 4,50, {DBContainer closureEXT036 ->
      zcty = closureEXT036.get("EXZCTY")
      zsty = closureEXT036.get("EXZSTY")
      itno = closureEXT036.get("EXITNO")
    })){
      logger.debug("In EXT036 ZCTY=${zcty} and ZSTY=${zsty} for ORNO:${orno} and PONR:${ponr}" )

      if(results.containsKey("PHYT") && zcty.trim() == "PHYTOSANITAIRE"){
        results["PHYT"] = true
      }

      if(results.containsKey("SANI") && zcty.trim() == "SANITAIRE"){
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
        DBAction query_MITMAS = database.table("MITMAS").index("00").selection("MMHAC1").build()
        DBContainer containerMITMAS = query_MITMAS.getContainer()
        containerMITMAS.set("MMCONO", currentCompany)
        containerMITMAS.set("MMITNO", itno)

        if(query_MITMAS.read(containerMITMAS)){
          String haci = containerMITMAS.get("MMHAC1")
          if(haci == "4.1" || haci == "4.2" || haci == "4.3"){
            results["DGX4"] = true
          }
        }
      }

    }

    return results
  }





}
