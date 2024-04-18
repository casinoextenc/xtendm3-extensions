/**
 * README
 * This extension is used by Mashup
 *
 * Name : EXT050MI.LstPallet
 * Description : List pallet
 * Date         Changed By   Description
 * 20230524     SEAR         LOG28 - Creation of files and containers
 * 20240319     MLECLERCQ    LOG28 - Alcool filter
 */
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.math.RoundingMode

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
  private String whlo_input
  private String uca4_input
  private String uca5_input
  private String uca6_input
  private String cuno_input
  private Long dlix_input
  private Integer currentCompany
  private String cuno_OOHEAD
  private double volume
  private double weight
  private double salesPrice
  private String baseUnit
  private String ItemNumber
  private String description
  private String dossier
  private String semaine
  private String annee
  private double lnam_OOLINE
  private double alqt_OOLINE
  private int dmcs_OOLINE
  private double cofs_OOLINE
  private String spun_OOLINE
  private String faci_OOLINE
  private double allocatedQuantity
  private double allocatedQuantityUB
  private String commande
  private int lineNumber
  private int lineSuffix
  private String orst_OOLINE
  private String cust_name
  private String cust_number
  private Long lineIndex
  private int conn_MHDISH
  private String rscd_MHDISH
  private int sanitary
  private double free2
  private int dangerous

  private String ridn_MITALO
  private int ridl_MITALO
  private int ridx_MITALO
  private String camu_MITALO
  private Long ridi_MITALO
  private double alqt_MITALO
  private String whlo_MITALO
  private String csno
  private String orco
  private String popn
  private String suno

  private Map<String, Boolean> quaFilters_input
  private LinkedHashMap<String, Boolean> filterResults

  private String dgx4

  private boolean hasAlcool = false
  private boolean hasBeer = false
  private boolean hasWine = false
  private boolean isISO = false
  private boolean isDPH = false
  private boolean isPhyto = false
  private boolean isSanit = false
  private boolean isPetFood = false
  private boolean isMilk = false
  private boolean isChamp = false
  private boolean isDgx = false
  private boolean isDgx4 = false

  private int isRecordValid = 0

  private String jobNumber

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

    quaFilters_input = new HashMap<String, Boolean>()

    //Get mi inputs
    whlo_input = (mi.in.get("WHLO") != null ? (String)mi.in.get("WHLO") : "")
    uca4_input = (mi.in.get("UCA4") != null ? (String)mi.in.get("UCA4") : "")
    uca5_input = (mi.in.get("UCA5") != null ? (String)mi.in.get("UCA5") : "")
    uca6_input = (mi.in.get("UCA6") != null ? (String)mi.in.get("UCA6") : "")
    cuno_input = (mi.in.get("CUNO") != null ? (String)mi.in.get("CUNO") : "")
    dlix_input = (Long)(mi.in.get("DLIX") != null ? mi.in.get("DLIX") : 0)

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
    if(!query_MITWHL.read(MITWHL)){
      mi.error("Le dépôt " + whlo_input + " n'existe pas")
      return
    }

    // check customer number
    if (cuno_input.length() > 0) {
      DBAction query_OCUSMA = database.table("OCUSMA").index("00").selection("OKCUNO").build()
      DBContainer OCUSMA = query_OCUSMA.getContainer()
      OCUSMA.set("OKCONO", currentCompany)
      OCUSMA.set("OKCUNO", cuno_input)
      if(!query_OCUSMA.read(OCUSMA)){
        mi.error("Le code client  " + cuno_input + " n'existe pas")
        return
      }
    }

    // check index number
    if ((mi.in.get("DLIX") != null)) {
      DBAction query_MHDISH = database.table("MHDISH").index("00").selection("OQDLIX").build()
      DBContainer MHDISH = query_MHDISH.getContainer()
      MHDISH.set("OQCONO", currentCompany)
      MHDISH.set("OQINOU", 1)
      MHDISH.set("OQDLIX", dlix_input)
      if(!query_MHDISH.read(MHDISH)){
        mi.error("Index de livraison  " + dlix_input + " n'existe pas")
        return
      }
    }

    ExpressionFactory expression_OOHEAD = database.getExpressionFactory("OOHEAD")
    if(uca4_input != ""){
      expression_OOHEAD = expression_OOHEAD.eq("OAUCA4", uca4_input)
    } else {
      expression_OOHEAD = expression_OOHEAD.ne("OAUCA4", "")
    }
    if(uca5_input != ""){
      expression_OOHEAD = expression_OOHEAD.and(expression_OOHEAD.eq("OAUCA5", uca5_input))
    } else {
      expression_OOHEAD = expression_OOHEAD.and(expression_OOHEAD.ne("OAUCA5", ""))
    }
    if(uca6_input != ""){
      expression_OOHEAD = expression_OOHEAD.and(expression_OOHEAD.eq("OAUCA6", uca6_input))
    } else {
      expression_OOHEAD = expression_OOHEAD.and(expression_OOHEAD.ne("OAUCA6", ""))
    }
    expression_OOHEAD = expression_OOHEAD.and(expression_OOHEAD.le("OAORSL", '44'))

    DBAction query_OOHEAD = database.table("OOHEAD").index("00").matching(expression_OOHEAD).selection("OAORNO","OAUCA4","OAUCA5","OAUCA6","OACUNO").build()
    DBContainer containerOOHEAD = query_OOHEAD.getContainer()
    containerOOHEAD.set("OACONO", currentCompany)

    if (!query_OOHEAD.readAll(containerOOHEAD, 1, OOHEADData)){
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
        "EXALQT",
        "EXGRWE",
        "EXVOL3",
        "EXZAAM",
        "EXDLIX",
        "EXCONN",
        "EXRSCD"
      )
      .build()

    DBContainer ListContainerEXT052 = ListqueryEXT052.getContainer()
    ListContainerEXT052.set("EXBJNO", jobNumber)

    //Record exists
    if (!ListqueryEXT052.readAll(ListContainerEXT052, 1, outData)){
    }

    // delete workfile
    DBAction DelQuery = database.table("EXT052").index("00").build()
    DBContainer DelcontainerEXT052 = DelQuery.getContainer()
    DelcontainerEXT052.set("EXBJNO", jobNumber)
    if(!DelQuery.readAllLock(DelcontainerEXT052, 1, deleteCallBack)){
      /*      mi.error("L'enregistrement n'existe pas")
       return*/
    }
  }

  // liste OOHEAD
  Closure<?> OOHEADData = { DBContainer containerOOHEAD ->

    int company = containerOOHEAD.get("OACONO")
    commande = containerOOHEAD.get("OAORNO")
    dossier = containerOOHEAD.get("OAUCA4")
    semaine = containerOOHEAD.get("OAUCA5")
    annee = containerOOHEAD.get("OAUCA6")
    cuno_OOHEAD = containerOOHEAD.get("OACUNO")
    // logger.debug("found OOHEAD : " + commande)

    filterResults = new LinkedHashMap<>()

    for(key in quaFilters_input.keySet()){
      if(quaFilters_input.get(key) == true){
        filterResults.put(key,false)
      }
    }

    // Get MITLO
    ExpressionFactory expression_MITALO = database.getExpressionFactory("MITALO")
    expression_MITALO = (expression_MITALO.eq("MQWHLO", whlo_input))
    expression_MITALO = expression_MITALO.and(expression_MITALO.eq("MQPLSX", "0"))
    DBAction query_MITALO = database.table("MITALO").index("10").matching(expression_MITALO).selection("MQRIDN","MQRIDL","MQRIDX","MQRIDI","MQCAMU","MQALQT","MQITNO","MQWHLO").build()
    DBContainer MITALO = query_MITALO.getContainer()
    MITALO.set("MQCONO", company)
    MITALO.set("MQTTYP", 31)
    MITALO.set("MQRIDN", commande)
    if(query_MITALO.readAll(MITALO, 3, MITALOData)){
    }


    //logger.debug("commande : " +  commande)
    //logger.debug("dossier : " +  dossier + "semaine : " +  semaine + "annee : " +  annee)
    //logger.debug("sameWarehouse : " +  sameWarehouse)
  }

  // data MITALO
  Closure<?> MITALOData = { DBContainer ContainerMITALO ->
    ridn_MITALO = ContainerMITALO.get("MQRIDN")
    ridl_MITALO = ContainerMITALO.get("MQRIDL")
    ridx_MITALO = ContainerMITALO.get("MQRIDX")
    camu_MITALO = ContainerMITALO.get("MQCAMU")
    ridi_MITALO = ContainerMITALO.get("MQRIDI")
    alqt_MITALO = ContainerMITALO.get("MQALQT")
    whlo_MITALO = ContainerMITALO.get("MQWHLO")
    ItemNumber = ContainerMITALO.get("MQITNO")

    logger.debug("found MITALO RIDN: " + ridn_MITALO)
    logger.debug("found MITALO RIDL: " + ridl_MITALO)
    logger.debug("found MITALO RIDI: " + ridi_MITALO)

    if(ridi_MITALO == 0) {
      DBAction query_MHDISL = database.table("MHDISL").index("10").selection("URDLIX").build()
      DBContainer MHDISL = query_MHDISL.getContainer()
      MHDISL.set("URCONO", currentCompany)
      MHDISL.set("URRORC", 3)
      MHDISL.set("URRIDN", ridn_MITALO)
      MHDISL.set("URRIDL", ridl_MITALO)
      MHDISL.set("URRIDX", ridx_MITALO)
      if(query_MHDISL.readAll(MHDISL, 5, outData_MHDISL)){
      }
    }

    sameWarehouse = false
    sameIndex = false
    sameCustomer = false
    foundLineIndex = false

    if (whlo_MITALO.equals(whlo_input)) {
      sameWarehouse = true
    }

    if(cuno_input.length() > 0 ){
      if (cuno_input.trim().equals(cuno_OOHEAD.trim())) {
        sameCustomer = true
      }
    } else {
      sameCustomer = true
    }

    if ((mi.in.get("DLIX") == null)) {
      sameIndex = true
    } else {
      if (dlix_input == ridi_MITALO) {
        sameIndex = true
      }
    }

    conn_MHDISH = 0
    DBAction query_MHDISH = database.table("MHDISH").index("00").selection("OQDLIX","OQCONN","OQRSCD").build()
    DBContainer MHDISH = query_MHDISH.getContainer()
    MHDISH.set("OQCONO", currentCompany)
    MHDISH.set("OQINOU", 1)
    MHDISH.set("OQDLIX", ridi_MITALO)
    if(query_MHDISH.read(MHDISH)){
      conn_MHDISH = MHDISH.get("OQCONN")
      rscd_MHDISH = MHDISH.get("OQRSCD")
      logger.debug("found MHDISH CONN: " + conn_MHDISH)
    }

    //logger.debug("sameIndex: " + sameIndex)
    //logger.debug("sameWarehouse: " + sameWarehouse)
    //logger.debug("sameCustomer: " + sameCustomer)

    if (sameIndex && sameWarehouse && sameCustomer) {
      //logger.debug("valide line")

      spun_OOLINE = ""
      // Get OOLINE
      DBAction query_OOLINE = database.table("OOLINE").index("00").selection("OBCUNO","OBORNO","OBPONR","OBPOSX","OBWHLO","OBSPUN","OBDMCS","OBCOFS","OBALQT","OBLNAM","OBFACI","OBITNO").build()
      DBContainer OOLINE = query_OOLINE.getContainer()
      OOLINE.set("OBCONO", currentCompany)
      OOLINE.set("OBORNO", ridn_MITALO)
      OOLINE.set("OBPONR", ridl_MITALO)
      OOLINE.set("OBPOSX", ridx_MITALO)
      if(query_OOLINE.read(OOLINE)){
        spun_OOLINE = OOLINE.get("OBSPUN")
        cofs_OOLINE = OOLINE.get("OBCOFS")
        alqt_OOLINE = OOLINE.get("OBALQT")
        lnam_OOLINE = OOLINE.get("OBLNAM")
        dmcs_OOLINE = OOLINE.get("OBDMCS")
        faci_OOLINE = OOLINE.get("OBFACI")
        lineSuffix = OOLINE.get("OBPOSX")
        lineNumber = OOLINE.get("OBPONR")
      }

      // get OCUSMA
      DBAction query_OCUSMA = database.table("OCUSMA").index("00").selection("OKCUNO","OKCUNM").build()
      DBContainer OCUSMA = query_OCUSMA.getContainer()
      OCUSMA.set("OKCONO", currentCompany)
      OCUSMA.set("OKCUNO", cuno_OOHEAD)
      if(query_OCUSMA.read(OCUSMA)){
        cust_name = OCUSMA.get("OKCUNM")
        cust_number = OCUSMA.get("OKCUNO")
      }

      // get MITMAS
      baseUnit = ""
      suno = ""
      free2 = 0
      dangerous = 0
      DBAction query_MITMAS = database.table("MITMAS").index("00").selection("MMITDS", "MMPUUN","MMUNMS", "MMGRWE", "MMVOL3","MMHAZI","MMCFI2","MMSUNO", "MMCFI4", "MMHAC1").build()
      DBContainer MITMAS = query_MITMAS.getContainer()
      MITMAS.set("MMCONO", currentCompany)
      MITMAS.set("MMITNO", ItemNumber)
      if(query_MITMAS.read(MITMAS)){
        description = MITMAS.get("MMITDS")
        baseUnit = MITMAS.get("MMUNMS")
        volume = MITMAS.getDouble("MMVOL3")
        weight = MITMAS.getDouble("MMGRWE")
        dangerous = MITMAS.getInt("MMHAZI")
        dgx4 = MITMAS.get("MMHAC1")
        free2 = MITMAS.getDouble("MMCFI2")
        suno = MITMAS.get("MMSUNO")



        if(quaFilters_input.get("BIER") || quaFilters_input.get("VIN1")){
          if(!filterResults.get("BIER") || !filterResults.get("VIN1")){
            String cfi4 = MITMAS.get("MMCFI4")
            cfi4 = cfi4.trim()
            //Don't need to check if both are already true
            filterResults = (LinkedHashMap<String, Boolean>)filterBeerAndWine(cfi4, filterResults)
          }

          logger.debug("Bier or wine filters, their values are : " + filterResults.get("BIER") + ", " + filterResults.get("VIN1"))
        }
      }

      if(quaFilters_input.get("ISO1") || quaFilters_input.get("DPH1")){
        filterResults = (LinkedHashMap<String, Boolean>)filterOnCugex(ItemNumber, filterResults)
        logger.debug("ISO or DPH filters, their values are : " + filterResults.get("ISO1") + ", " + filterResults.get("DPH1"))
      }

      logger.debug("suno = " + suno)
      logger.debug("ItemNumber = " + ItemNumber)
      popn = ""
      ExpressionFactory expression_MITPOP = database.getExpressionFactory("MITPOP")
      expression_MITPOP = expression_MITPOP.eq("MPREMK", "SIGMA6")
      DBAction MITPOP_query = database.table("MITPOP").index("00").matching(expression_MITPOP).selection("MPPOPN").build()
      DBContainer MITPOP = MITPOP_query.getContainer()
      MITPOP.set("MPCONO", currentCompany)
      MITPOP.set("MPALWT", 1)
      MITPOP.set("MPALWQ", "")
      MITPOP.set("MPITNO", ItemNumber)
      if (!MITPOP_query.readAll(MITPOP, 4, outData_MITPOP)) {
      }
      logger.debug("popn = " + popn)

      logger.debug("faci_OOLINE = " + faci_OOLINE)
      csno =""
      orco = ""
      DBAction MITFAC_query = database.table("MITFAC").index("00").selection("M9CSNO","M9ORCO").build()
      DBContainer MITFAC = MITFAC_query.getContainer()
      MITFAC.set("M9CONO", currentCompany)
      MITFAC.set("M9FACI", faci_OOLINE)
      MITFAC.set("M9ITNO", ItemNumber)
      if(MITFAC_query.read(MITFAC)){
        csno = MITFAC.get("M9CSNO")
        orco = MITFAC.get("M9ORCO")
      }
      logger.debug("csno = ${csno} and orco = ${orco}")

      sanitary = 0



      DBAction EXT032_query = database.table("EXT032").index("00").selection("EXZSAN","EXZALC").build()
      DBContainer EXT032 = EXT032_query.getContainer()
      EXT032.set("EXCONO", currentCompany)
      EXT032.set("EXPOPN", popn)
      EXT032.set("EXSUNO", suno)
      EXT032.set("EXORCO", orco)
      if(EXT032_query.read(EXT032)){
        sanitary = EXT032.get("EXZSAN")

        boolean alcool = (boolean)EXT032.get("EXZALC")

        logger.debug("Filtre alcool ? : " + quaFilters_input["ALCO"] + " , EXT032 ZALC = ${alcool}")
        if (quaFilters_input.get("ALCO") && !filterResults.get("ALCO")){
          //If alcool already true, don't need to check
            if(alcool){
              /*hasAlcool = true
              isRecordValid = true*/
              filterResults["ALCO"] = true
            }
        }

      }
      logger.debug("sanitary = " + sanitary)

      // convert to Basic unit
      /*
       if (!baseUnit.equals(spun_OOLINE)) {
       if (dmcs_OOLINE.equals("1")) {
       allocatedQuantityUB = alqt_MITALO * dmcs_OOLINE
       } else {
       allocatedQuantityUB = alqt_MITALO / dmcs_OOLINE
       }
       } else {
       allocatedQuantityUB =  alqt_MITALO
       }
       */
      allocatedQuantityUB =  alqt_MITALO

      double ALQT = new BigDecimal (allocatedQuantityUB).setScale(6, RoundingMode.HALF_EVEN).doubleValue()
      double GRWE = new BigDecimal (allocatedQuantityUB * weight).setScale(6, RoundingMode.HALF_EVEN).doubleValue()
      double VOL3 = new BigDecimal (allocatedQuantityUB * volume).setScale(6, RoundingMode.HALF_EVEN).doubleValue()
      double ZAAM = new BigDecimal (lnam_OOLINE).setScale(6, RoundingMode.HALF_EVEN).doubleValue()


      logger.debug("line OK")

      if (quaFilters_input.containsValue(true)) {
        logger.debug("Des filtres sont présents")

        for(resultKey in filterResults.keySet()){
          logger.debug("Result for key: ${resultKey} = " + filterResults.get(resultKey))
        }

        filterResults = (LinkedHashMap<String, Boolean>) filterOnEXT036(commande,lineNumber, lineSuffix,filterResults )

        isRecordValid = filterResults.containsValue(false) ? 0 : 1
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
            "EXALQT",
            "EXGRWE",
            "EXVOL3",
            "EXZAAM",
            "EXDLIX",
            "EXCONN",
            "EXRSCD",
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
        containerEXT052.set("EXCAMU", camu_MITALO)

      //Record exists
      if (queryEXT052.read(containerEXT052)) {
        Closure<?> updateEXT050 = { LockedResult lockedResultEXT052 ->
          Long last_dlix = lockedResultEXT052.getLong("EXDLIX")
          if (last_dlix > 0 && last_dlix != ridi_MITALO) {
            lockedResultEXT052.set("EXDLIX", 0)
          } else {
            lockedResultEXT052.set("EXDLIX", ridi_MITALO)
          }
          if(dangerous == 1)
            lockedResultEXT052.set("EXHAZI", dangerous)
          if((int) free2 > 0)
            lockedResultEXT052.set("EXCFI2", 1)
          if(sanitary == 1)
            lockedResultEXT052.set("EXZSAN", sanitary)
          if(isRecordValid == 1)
            lockedResultEXT052.set("EXFILT", isRecordValid)
          lockedResultEXT052.set("EXALQT", lockedResultEXT052.getDouble("EXALQT") + ALQT)
          lockedResultEXT052.set("EXGRWE", lockedResultEXT052.getDouble("EXGRWE") + GRWE)
          lockedResultEXT052.set("EXVOL3", lockedResultEXT052.getDouble("EXVOL3") + VOL3)
          lockedResultEXT052.set("EXZAAM", lockedResultEXT052.getDouble("EXZAAM") + ZAAM)
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
        containerEXT052.set("EXCUNO", cust_number)
        containerEXT052.set("EXCUNM", cust_name)
        containerEXT052.set("EXHAZI", dangerous)
        containerEXT052.set("EXCFI2", free2)
        containerEXT052.set("EXZSAN", sanitary)
        containerEXT052.set("EXFILT", isRecordValid)
        containerEXT052.set("EXALQT", ALQT)
        containerEXT052.set("EXGRWE", GRWE)
        containerEXT052.set("EXVOL3", VOL3)
        containerEXT052.set("EXZAAM", ZAAM)
        containerEXT052.set("EXDLIX", ridi_MITALO)
        containerEXT052.set("EXCONN", conn_MHDISH)
        containerEXT052.set("EXRSCD", rscd_MHDISH)
        containerEXT052.set("EXRGDT", utility.call("DateUtil", "currentDateY8AsInt"))
        containerEXT052.set("EXRGTM", utility.call("DateUtil", "currentTimeAsInt"))
        containerEXT052.set("EXLMDT", utility.call("DateUtil", "currentDateY8AsInt"))
        containerEXT052.set("EXCHNO", 1)
        containerEXT052.set("EXCHID", program.getUser())
        queryEXT052.insert(containerEXT052)
      }


    }
  }

  Closure<?> DataMHDISL = { DBContainer containerMHDISL ->
    lineIndex = containerMHDISL.getLong("URDLIX")
    sameIndex = true
    foundLineIndex = true
  }

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
    String allocatedEXT052 = containerEXT052.get("EXALQT")
    String weightEXT052 = containerEXT052.get("EXGRWE")
    String volumeEXT052 = containerEXT052.get("EXVOL3")
    String amountEXT052 = containerEXT052.get("EXZAAM")
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
      mi.outData.put("ALQT", allocatedEXT052)
      mi.outData.put("GRWE", weightEXT052)
      mi.outData.put("VOL3", volumeEXT052)
      mi.outData.put("ZAAM", amountEXT052)
      mi.outData.put("DLIX", indexEXT052)
      mi.outData.put("CONN", shipmentEXT052)
      mi.outData.put("RSCD", reasonCodeEXT052)

    if(quaFilters_input.containsValue(true)){
      if(filterEXT052 == "1"){
        mi.write()
      }  
    }else{
      mi.write()
    }
    
      

    




  }

  Closure<?> outData_MITPOP = { DBContainer MITPOP ->
    popn = MITPOP.get("MPPOPN")
  }
  Closure<?> deleteCallBack = { LockedResult lockedResult ->
    lockedResult.delete()
  }
  Closure<?> outData_MHDISL = { DBContainer MHDISL ->
    ridi_MITALO = MHDISL.get("URDLIX")
    logger.debug("found MHDISL RIDI: " + ridi_MITALO)
  }

  private filterBeerAndWine(cfi4,filterResults){
    logger.debug("MMCFI4 : ${cfi4}")

    //TODO : remove hard values
    if(cfi4 == "S" || cfi4 == "T"){
      /*hasBeer = true
      logger.debug("record cfi4 = ${cfi4}, hasBeer : ${hasBeer}")*/
      filterResults["BIER"] = true
      filterResults["ALCO"] = true
    }else if(cfi4 == "1" || cfi4 == "2" || cfi4 == "3" || cfi4 == "4" || cfi4 == "5" || cfi4 == "6" || cfi4 == "9"
      || cfi4 == "B" || cfi4 == "C" || cfi4 == "D" || cfi4 == "K" || cfi4 == "L" || cfi4 == "M" || cfi4 == "N"
      || cfi4 == "Q" || cfi4 == "U" || cfi4 == "X"
    ){
      //hasWine = true
      filterResults["VIN1"] = true
      filterResults["ALCO"] = true
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
