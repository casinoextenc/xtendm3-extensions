/**
 * README
 * This extension is used by Mashup
 *
 * Name : EXT050MI.LstShipment2
 * Description : List shipment
 * Date         Changed By   Description
 * 20230601     SEAR         LOG28 - Creation of files and containers
 */

import java.math.RoundingMode
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

public class LstShipment2 extends ExtendM3Transaction {
  private final MIAPI mi
  private final LoggerAPI logger
  private final ProgramAPI program
  private final DatabaseAPI database
  private final SessionAPI session
  private final TransactionAPI transaction
  private final MICallerAPI miCaller
  private final UtilityAPI utility
  private String parm
  private String whloOoline
  private String whloInput
  private long dlixInput
  private long dlixMhdish
  private long previousDlixMhdish
  private int connMhdish
  private int connInput
  private String ude1Input
  private int ccudInput
  private int currentCompany
  private boolean sameUDE1
  private boolean sameCCUD
  private Boolean foundDradtr
  private long index = 0
  private int shipment = 0
  private String dossier = ""
  private String semaine = ""
  private String annee = ""
  private int arrivalDate = 0
  private int departureDate = 0
  private int packagindDate = 0

  private long ship = 0

  private String orno
  private int line
  private String itno
  private double qty = 0
  private double grwe = 0
  private double vol3 = 0
  private double lnam = 0
  private double nbOfCols = 0
  private Integer nbMaxRecord = 10000

  private String jobNumber

  public LstShipment2(LoggerAPI logger, MIAPI mi, DatabaseAPI database, ProgramAPI program, MICallerAPI miCaller, UtilityAPI utility) {
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

    //Get mi inputs
    whloInput = (mi.in.get("WHLO") != null ? (String)mi.in.get("WHLO") : "")
    ude1Input = (mi.in.get("UDE1") != null ? (String)mi.in.get("UDE1") : "")
    dlixInput = (Long)(mi.in.get("DLIX") != null ? mi.in.get("DLIX") : 0)
    ccudInput = (int)(mi.in.get("CCUD") != null ? mi.in.get("CCUD") : 0)
    connInput = (int)(mi.in.get("CONN") != null ? mi.in.get("CONN") : 0)

    // check warehouse
    DBAction queryMitwhl = database.table("MITWHL").index("00").selection("MWWHLO").build()
    DBContainer MITWHL = queryMitwhl.getContainer()
    MITWHL.set("MWCONO", currentCompany)
    MITWHL.set("MWWHLO", whloInput)
    if(!queryMitwhl.read(MITWHL)){
      mi.error("Le dépôt " + whloInput + " n'existe pas")
      return
    }

    ExpressionFactory expressionMhdish = database.getExpressionFactory("MHDISH")
    if(mi.in.get("CONN") != null){
      expressionMhdish = expressionMhdish.eq("OQCONN", connInput.toString().trim())
    }else{
      expressionMhdish = (expressionMhdish.ne("OQCONN", "0"))
    }

    if(mi.in.get("DLIX") != null){
      expressionMhdish = expressionMhdish.and(expressionMhdish.eq("OQDLIX", dlixInput.toString().trim()))
    }
    expressionMhdish = expressionMhdish.and(expressionMhdish.eq("OQRLDT", "0"))
    expressionMhdish = expressionMhdish.and(expressionMhdish.lt("OQPGRS", "50"))

    DBAction queryMhdish = database.table("MHDISH").index("94").matching(expressionMhdish).selection("OQDLIX","OQCONN").build()
    DBContainer containerMHDISH = queryMhdish.getContainer()
    containerMHDISH.set("OQCONO", currentCompany)
    containerMHDISH.set("OQINOU", 1)
    containerMHDISH.set("OQWHLO", whloInput)


    if (queryMhdish.readAll(containerMHDISH, 3, nbMaxRecord, MHDISHData)){
    }

    // list out data
    DBAction ListqueryEXT056 = database.table("EXT056")
      .index("00")
      .selection(
        "EXBJNO",
        "EXCONO",
        "EXUCA4",
        "EXUCA5",
        "EXUCA6",
        "EXCOLS",
        "EXGRWE",
        "EXVOL3",
        "EXLNAM",
        "EXETAD",
        "EXETDD",
        "EXCCUD",
        "EXCONN",
        "EXDLIX",
        "EXRGDT",
        "EXRGTM",
        "EXLMDT",
        "EXCHNO",
        "EXCHID"
      )
      .build()

    DBContainer ListContainerEXT056 = ListqueryEXT056.getContainer()
    ListContainerEXT056.set("EXBJNO", jobNumber)

    //Record exists
    if (!ListqueryEXT056.readAll(ListContainerEXT056, 1, nbMaxRecord, outData)){

    }

    // delete workfile
    DBAction DelQuery = database.table("EXT056").index("00").build()
    DBContainer DelcontainerEXT056 = DelQuery.getContainer()
    DelcontainerEXT056.set("EXBJNO", jobNumber)
    if(!DelQuery.readAllLock(DelcontainerEXT056, 1, deleteCallBack)){
    }
  }

  // liste MHDISH
  Closure<?> MHDISHData = { DBContainer containerMHDISH ->

    sameUDE1 = false
    sameCCUD = false
    dlixMhdish = containerMHDISH.get("OQDLIX")
    if(!previousDlixMhdish || !previousDlixMhdish.equals(dlixMhdish)){
      previousDlixMhdish = dlixMhdish
    }
    connMhdish = containerMHDISH.get("OQCONN")
    logger.debug("DLIX : " + dlixMhdish)


    foundDradtr = true
    index = 0
    shipment = 0
    dossier = ""
    semaine = ""
    annee = ""
    arrivalDate = 0
    departureDate = 0
    packagindDate = 0

    DBAction queryDradtr = database.table("DRADTR").index("00").selection("DRDLIX","DRCONN","DRETAD","DRETDD","DRCCUD","DRUDE1","DRUDE2","DRUDE3").build()
    DBContainer DRADTR = queryDradtr.getContainer()
    DRADTR.set("DRCONO", currentCompany)
    DRADTR.set("DRTLVL", 1)
    DRADTR.set("DRCONN", connMhdish)
    //DRADTR.set("DRDLIX", dlixMhdish)
    if(!queryDradtr.readAll(DRADTR, 3, nbMaxRecord, DRADTRData)){
      foundDradtr = false
    }

    if (mi.in.get("UDE1") != null) {
      if (ude1Input.trim() == dossier.trim()) {
        sameUDE1 = true
      }
    } else {
      sameUDE1 = true
    }

    if (mi.in.get("CCUD") != null) {
      if (ccudInput == packagindDate) {
        sameCCUD = true
      }
    } else {
      sameCCUD = true
    }

    if (foundDradtr && sameUDE1 && sameCCUD) {
      logger.debug("has DLIX : " + dlixMhdish)

      qty = 0
      grwe = 0
      vol3 = 0
      nbOfCols = 0
      lnam = 0

      if(dlixMhdish != 0){
        DBAction queryMhdisl = database.table("MHDISL").index("00").selection("URRIDN","URRIDL","URITNO","URTRQT").build()
        DBContainer MHDISL = queryMhdisl.getContainer()
        MHDISL.set("URCONO", currentCompany)
        MHDISL.set("URDLIX", dlixMhdish)
        MHDISL.set("URRORC", 3)

        if(queryMhdisl.readAll(MHDISL,3, nbMaxRecord, MHDISLData)){}
      }

      logger.debug("after MHDISLDATA, nbOfCols = ${nbOfCols}")

      //Check if record exists
      DBAction queryEXT056 = database.table("EXT056")
        .index("00")
        .selection(
          "EXBJNO",
          "EXCONO",
          "EXUCA4",
          "EXUCA5",
          "EXUCA6",
          "EXCOLS",
          "EXVOL3",
          "EXGRWE",
          "EXLNAM",
          "EXETAD",
          "EXETDD",
          "EXCCUD",
          "EXCONN",
          "EXDLIX",
          "EXRGDT",
          "EXRGTM",
          "EXLMDT",
          "EXCHNO",
          "EXCHID"
        )
        .build()

      DBContainer containerEXT056 = queryEXT056.getContainer()
      containerEXT056.set("EXBJNO", jobNumber)
      containerEXT056.set("EXCONO", currentCompany)
      containerEXT056.set("EXUCA4", dossier)
      containerEXT056.set("EXUCA5", semaine)
      containerEXT056.set("EXUCA6", annee)
      containerEXT056.set("EXDLIX", dlixMhdish)
      containerEXT056.set("EXCONN", connMhdish)

      //Record exists
      if (queryEXT056.read(containerEXT056)) {
        Closure<?> updateEXT056 = { LockedResult lockedResultEXT056 ->
          lockedResultEXT056.set("EXETAD", arrivalDate)
          lockedResultEXT056.set("EXETDD", departureDate)
          lockedResultEXT056.set("EXCCUD", packagindDate)
          lockedResultEXT056.set("EXLMDT", utility.call("DateUtil", "currentDateY8AsInt"))
          lockedResultEXT056.setInt("EXCHNO", ((Integer)lockedResultEXT056.get("EXCHNO") + 1))
          lockedResultEXT056.set("EXCHID", program.getUser())
          lockedResultEXT056.update()
        }
        queryEXT056.readLock(containerEXT056, updateEXT056)
      } else {
        containerEXT056.set("EXBJNO", jobNumber)
        containerEXT056.set("EXCONO", currentCompany)
        containerEXT056.set("EXUCA4", dossier)
        containerEXT056.set("EXUCA5", semaine)
        containerEXT056.set("EXUCA6", annee)
        containerEXT056.set("EXDLIX", dlixMhdish)
        containerEXT056.set("EXCONN", connMhdish)
        containerEXT056.set("EXCOLS", nbOfCols)
        containerEXT056.set("EXVOL3", vol3)
        containerEXT056.set("EXGRWE", grwe)
        containerEXT056.set("EXLNAM", lnam)
        containerEXT056.set("EXETAD", arrivalDate)
        containerEXT056.set("EXETDD", departureDate)
        containerEXT056.set("EXCCUD", packagindDate)
        containerEXT056.set("EXRGDT", utility.call("DateUtil", "currentDateY8AsInt"))
        containerEXT056.set("EXRGTM", utility.call("DateUtil", "currentTimeAsInt"))
        containerEXT056.set("EXLMDT", utility.call("DateUtil", "currentDateY8AsInt"))
        containerEXT056.set("EXCHNO", 1)
        containerEXT056.set("EXCHID", program.getUser())
        queryEXT056.insert(containerEXT056)
      }
    }
  }

  // data OOLINE
  Closure<?> OOLINEData = { DBContainer ContainerOOLINE ->
    whloOoline = ContainerOOLINE.get("OBWHLO")
    return
  }

  // data DRADTR
  Closure<?> DRADTRData = { DBContainer ContaineDRADTR ->
    arrivalDate = ContaineDRADTR.getInt("DRETAD")
    departureDate = ContaineDRADTR.getInt("DRETDD")
    //packagindDate = ContaineDRADTR.getInt("DRCCUD")
    dossier = ContaineDRADTR.get("DRUDE1")
    semaine = ContaineDRADTR.get("DRUDE2")
    annee = ContaineDRADTR.get("DRUDE3")
    logger.debug("dossier : " + dossier)
    logger.debug("semaine : " + semaine)
    logger.debug("annee : " + annee)

    DBAction queryDradtrLiv = database.table("DRADTR").index("00").selection("DRDLIX","DRCCUD").build()
    DBContainer dradtrLiv = queryDradtrLiv.getContainer()
    dradtrLiv.set("DRCONO", currentCompany)
    dradtrLiv.set("DRTLVL", 2)
    dradtrLiv.set("DRCONN", 0)
    logger.debug("dradtrLiv dlixMhdish : " + dlixMhdish)
    dradtrLiv.set("DRDLIX", dlixMhdish)

    if(queryDradtrLiv.read(dradtrLiv)){
      packagindDate = dradtrLiv.getInt("DRCCUD")

      ship = dradtrLiv.get("DRDLIX")
      logger.debug("Empotage : " + packagindDate + " pour DLIX : " + ship)
    }
    return
  }

  /**
   * Get MHDISL data
   */
  Closure<?> MHDISLData = { DBContainer ContainerMHDISL ->

    double lineQty = ContainerMHDISL.get("URTRQT")

    qty += lineQty
    logger.debug("In MHDISLData, URTRQT = ${lineQty} and qty = ${qty}" )

    itno = ContainerMHDISL.get("URITNO")
    orno = ContainerMHDISL.get("URRIDN")
    line = ContainerMHDISL.get("URRIDL")
    //qty = ContainerMHDISL.get("URTRQT")
    qty = new BigDecimal(qty).setScale(6, RoundingMode.HALF_EVEN).doubleValue()

    DBAction queryMitmas = database.table("MITMAS").index("00").selection("MMGRWE","MMVOL3").build()
    DBContainer MITMAS = queryMitmas.getContainer()
    MITMAS.set("MMCONO", currentCompany)
    MITMAS.set("MMITNO", itno)

    if(queryMitmas.read(MITMAS)){

      double grweMitmas = MITMAS.get("MMGRWE")
      double vol3Mitmas = MITMAS.get("MMVOL3")

      grweMitmas = new BigDecimal(grweMitmas).setScale(6, RoundingMode.HALF_EVEN).doubleValue()
      vol3Mitmas = new BigDecimal(vol3Mitmas).setScale(6, RoundingMode.HALF_EVEN).doubleValue()
      logger.debug("previousDlixMhdish = ${previousDlixMhdish}, and dlixMhdish = ${dlixMhdish}")
      if(previousDlixMhdish.equals(dlixMhdish)){

        grwe += grweMitmas * lineQty
        vol3 += vol3Mitmas * lineQty
      }else{
        logger.debug("DLIX is not the same : ")
        grwe = grweMitmas * lineQty
        vol3 = vol3Mitmas * lineQty
      }

      grwe = new BigDecimal(grwe).setScale(6, RoundingMode.HALF_EVEN).doubleValue()
      vol3 = new BigDecimal(vol3).setScale(6, RoundingMode.HALF_EVEN).doubleValue()
    }

    DBAction queryOoline = database.table("OOLINE").index("00").selection("OBLNAM").build()
    DBContainer OOLINE = queryOoline.getContainer()
    OOLINE.set("OBCONO", currentCompany)
    OOLINE.set("OBORNO", orno)
    OOLINE.set("OBPONR", line)
    OOLINE.set("OBPOSX", 0)

    if(queryOoline.read(OOLINE)){
      double lnamOoline = OOLINE.get("OBLNAM")
      lnamOoline = new BigDecimal(lnamOoline).setScale(6, RoundingMode.HALF_EVEN).doubleValue()
      if(previousDlixMhdish.equals(dlixMhdish)){
        lnam += lnamOoline
      }else{
        lnam = lnamOoline
      }

      lnam = new BigDecimal(lnam).setScale(6, RoundingMode.HALF_EVEN).doubleValue()
    }

    DBAction queryMitaun = database.table("MITAUN").index("00").selection("MUCOFA").build()
    DBContainer MITAUN = queryMitaun.getContainer()
    MITAUN.set("MUCONO", currentCompany)
    MITAUN.set("MUITNO", itno)
    MITAUN.set("MUAUTP", 1)
    MITAUN.set("MUALUN", "COL")

    if(queryMitaun.read(MITAUN)){
      logger.debug("In MITMAS, lineQty is ${lineQty}")
      double cofa = MITAUN.get("MUCOFA")
      logger.debug("in MITAUN before NbOfCols, qty = ${qty} and cofa = ${cofa}, so qty / cofa = " + qty / cofa + " for dlix = ${dlixMhdish} , ORNO: ${orno}, LINE: ${line}, itno: ${itno}")
      if(previousDlixMhdish.equals(dlixMhdish)){
        nbOfCols += lineQty / cofa
      }else{
        nbOfCols = lineQty / cofa
      }
      logger.debug("in MITAUN after NbOfCols, qty = ${qty} and cofa = ${cofa}, so qty / cofa = " + qty / cofa + " for dlix = ${dlixMhdish} , ORNO: ${orno}, LINE: ${line}, itno: ${itno}")

      nbOfCols = new BigDecimal(nbOfCols).setScale(6, RoundingMode.HALF_EVEN).doubleValue()
    }
  }

  /**
   * Get EXT056 data
   */
  Closure<?> outData = { DBContainer containerEXT056 ->
    String dossierEXT056 = containerEXT056.get("EXUCA4")
    String semaineEXT056 = containerEXT056.get("EXUCA5")
    String anneeEXT056 = containerEXT056.get("EXUCA6")
    String indexEXT056 = containerEXT056.getLong("EXDLIX")
    String shipmentEXT056 = containerEXT056.get("EXCONN")
    String arrivalDateEXT056 = containerEXT056.get("EXETAD")
    String departureDateEXT056 = containerEXT056.get("EXETDD")
    String packagindDateEXT056 = containerEXT056.get("EXCCUD")
    String colsEXT056 = containerEXT056.get("EXCOLS")
    String grweEXT056 = containerEXT056.get("EXGRWE")
    String vol3EXT056 = containerEXT056.get("EXVOL3")
    String lnamEXT056 = containerEXT056.get("EXLNAM")

    logger.debug("in OutDatas : Cols = ${colsEXT056}" )

    mi.outData.put("UDE1", dossierEXT056)
    mi.outData.put("UDE2", semaineEXT056)
    mi.outData.put("UDE3", anneeEXT056)
    mi.outData.put("ETDD", departureDateEXT056)
    mi.outData.put("ETAD", arrivalDateEXT056)
    mi.outData.put("DLIX", indexEXT056)
    mi.outData.put("CONN", shipmentEXT056)
    mi.outData.put("CCUD", packagindDateEXT056)
    mi.outData.put("COLS", colsEXT056)
    mi.outData.put("GRWE", grweEXT056)
    mi.outData.put("VOL3", vol3EXT056)
    mi.outData.put("LNAM", lnamEXT056)
    mi.write()
  }

  /**
   * Delete callback
   */
  Closure<?> deleteCallBack = { LockedResult lockedResult ->
    lockedResult.delete()
  }
}
