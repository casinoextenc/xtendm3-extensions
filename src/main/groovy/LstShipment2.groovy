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
  private boolean sameUde1
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
    DBAction mitwhlQuery = database.table("MITWHL").index("00").selection("MWWHLO").build()
    DBContainer mitwhlRequest = mitwhlQuery.getContainer()
    mitwhlRequest.set("MWCONO", currentCompany)
    mitwhlRequest.set("MWWHLO", whloInput)
    if(!mitwhlQuery.read(mitwhlRequest)){
      mi.error("Le dépôt " + whloInput + " n'existe pas")
      return
    }

    ExpressionFactory mhdishExpression = database.getExpressionFactory("MHDISH")
    if(mi.in.get("CONN") != null){
      mhdishExpression = mhdishExpression.eq("OQCONN", connInput.toString().trim())
    }else{
      mhdishExpression = (mhdishExpression.ne("OQCONN", "0"))
    }

    if(mi.in.get("DLIX") != null){
      mhdishExpression = mhdishExpression.and(mhdishExpression.eq("OQDLIX", dlixInput.toString().trim()))
    }
    mhdishExpression = mhdishExpression.and(mhdishExpression.eq("OQRLDT", "0"))
    mhdishExpression = mhdishExpression.and(mhdishExpression.lt("OQPGRS", "50"))

    DBAction mhdishQuery = database.table("MHDISH").index("94").matching(mhdishExpression).selection("OQDLIX","OQCONN").build()
    DBContainer mhdishRequest = mhdishQuery.getContainer()
    mhdishRequest.set("OQCONO", currentCompany)
    mhdishRequest.set("OQINOU", 1)
    mhdishRequest.set("OQWHLO", whloInput)


    if (mhdishQuery.readAll(mhdishRequest, 3, nbMaxRecord, mhdishReader)){
    }

    // list out data
    DBAction ext056Query = database.table("EXT056")
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

    DBContainer ext056Request = ext056Query.getContainer()
    ext056Request.set("EXBJNO", jobNumber)

    //Record exists
    if (!ext056Query.readAll(ext056Request, 1, nbMaxRecord, ext056Reader)){

    }

    Closure<?> ext056LReader = {DBContainer ext056Result ->
      Closure<?> ext056Deleter = { LockedResult lockedResultEXT056 ->
        lockedResultEXT056.delete()
      }
      ext056Query.readLock(ext056Result, ext056Deleter)
    }
    // delete workfile
    DBContainer ext056DelRequest = ext056Query.getContainer()
    ext056DelRequest.set("EXBJNO", jobNumber)
    if(!ext056Query.readAll(ext056DelRequest, 1, 10000, ext056LReader)){
    }
  }

  // liste MHDISH
  Closure<?> mhdishReader = { DBContainer mhdishResult ->

    sameUde1 = false
    sameCCUD = false
    dlixMhdish = mhdishResult.get("OQDLIX")
    if(!previousDlixMhdish || !previousDlixMhdish.equals(dlixMhdish)){
      previousDlixMhdish = dlixMhdish
    }
    connMhdish = mhdishResult.get("OQCONN")
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

    DBAction dradtrQuery = database.table("DRADTR").index("00").selection("DRDLIX","DRCONN","DRETAD","DRETDD","DRCCUD","DRUDE1","DRUDE2","DRUDE3").build()
    DBContainer dradtrRequest = dradtrQuery.getContainer()
    dradtrRequest.set("DRCONO", currentCompany)
    dradtrRequest.set("DRTLVL", 1)
    dradtrRequest.set("DRCONN", connMhdish)
    //DRADTR.set("DRDLIX", dlixMhdish)
    if(!dradtrQuery.readAll(dradtrRequest, 3, nbMaxRecord, dradtrReader)){
      foundDradtr = false
    }

    if (mi.in.get("UDE1") != null) {
      if (ude1Input.trim() == dossier.trim()) {
        sameUde1 = true
      }
    } else {
      sameUde1 = true
    }

    if (mi.in.get("CCUD") != null) {
      if (ccudInput == packagindDate) {
        sameCCUD = true
      }
    } else {
      sameCCUD = true
    }

    if (foundDradtr && sameUde1 && sameCCUD) {
      logger.debug("has DLIX : " + dlixMhdish)

      qty = 0
      grwe = 0
      vol3 = 0
      nbOfCols = 0
      lnam = 0

      if(dlixMhdish != 0){
        DBAction mhdislQuery = database.table("MHDISL").index("00").selection("URRIDN","URRIDL","URITNO","URTRQT").build()
        DBContainer mhdsilRequest = mhdislQuery.getContainer()
        mhdsilRequest.set("URCONO", currentCompany)
        mhdsilRequest.set("URDLIX", dlixMhdish)
        mhdsilRequest.set("URRORC", 3)

        if(mhdislQuery.readAll(mhdsilRequest,3, nbMaxRecord, mhdislReader)){}
      }

      logger.debug("after MHDISLDATA, nbOfCols = ${nbOfCols}")

      //Check if record exists
      DBAction ext056Query = database.table("EXT056")
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

      DBContainer ext056Request = ext056Query.getContainer()
      ext056Request.set("EXBJNO", jobNumber)
      ext056Request.set("EXCONO", currentCompany)
      ext056Request.set("EXUCA4", dossier)
      ext056Request.set("EXUCA5", semaine)
      ext056Request.set("EXUCA6", annee)
      ext056Request.set("EXDLIX", dlixMhdish)
      ext056Request.set("EXCONN", connMhdish)

      //Record exists
      if (ext056Query.read(ext056Request)) {
        Closure<?> updateEXT056 = { LockedResult lockedResultEXT056 ->
          lockedResultEXT056.set("EXETAD", arrivalDate)
          lockedResultEXT056.set("EXETDD", departureDate)
          lockedResultEXT056.set("EXCCUD", packagindDate)
          lockedResultEXT056.set("EXLMDT", utility.call("DateUtil", "currentDateY8AsInt"))
          lockedResultEXT056.setInt("EXCHNO", ((Integer)lockedResultEXT056.get("EXCHNO") + 1))
          lockedResultEXT056.set("EXCHID", program.getUser())
          lockedResultEXT056.update()
        }
        ext056Query.readLock(ext056Request, updateEXT056)
      } else {
        ext056Request.set("EXBJNO", jobNumber)
        ext056Request.set("EXCONO", currentCompany)
        ext056Request.set("EXUCA4", dossier)
        ext056Request.set("EXUCA5", semaine)
        ext056Request.set("EXUCA6", annee)
        ext056Request.set("EXDLIX", dlixMhdish)
        ext056Request.set("EXCONN", connMhdish)
        ext056Request.set("EXCOLS", nbOfCols)
        ext056Request.set("EXVOL3", vol3)
        ext056Request.set("EXGRWE", grwe)
        ext056Request.set("EXLNAM", lnam)
        ext056Request.set("EXETAD", arrivalDate)
        ext056Request.set("EXETDD", departureDate)
        ext056Request.set("EXCCUD", packagindDate)
        ext056Request.set("EXRGDT", utility.call("DateUtil", "currentDateY8AsInt"))
        ext056Request.set("EXRGTM", utility.call("DateUtil", "currentTimeAsInt"))
        ext056Request.set("EXLMDT", utility.call("DateUtil", "currentDateY8AsInt"))
        ext056Request.set("EXCHNO", 1)
        ext056Request.set("EXCHID", program.getUser())
        ext056Query.insert(ext056Request)
      }
    }
  }

  // data DRADTR
  Closure<?> dradtrReader = { DBContainer dradtrResult ->
    arrivalDate = dradtrResult.getInt("DRETAD")
    departureDate = dradtrResult.getInt("DRETDD")
    //packagindDate = ContaineDRADTR.getInt("DRCCUD")
    dossier = dradtrResult.get("DRUDE1")
    semaine = dradtrResult.get("DRUDE2")
    annee = dradtrResult.get("DRUDE3")
    logger.debug("dossier : " + dossier)
    logger.debug("semaine : " + semaine)
    logger.debug("annee : " + annee)

    DBAction dradtrLivQuery = database.table("DRADTR").index("00").selection("DRDLIX","DRCCUD").build()
    DBContainer dradtrLivRequest = dradtrLivQuery.getContainer()
    dradtrLivRequest.set("DRCONO", currentCompany)
    dradtrLivRequest.set("DRTLVL", 2)
    dradtrLivRequest.set("DRCONN", 0)
    logger.debug("dradtrLiv dlixMhdish : " + dlixMhdish)
    dradtrLivRequest.set("DRDLIX", dlixMhdish)

    if(dradtrLivQuery.read(dradtrLivRequest)){
      packagindDate = dradtrLivRequest.getInt("DRCCUD")

      ship = dradtrLivRequest.get("DRDLIX")
      logger.debug("Empotage : " + packagindDate + " pour DLIX : " + ship)
    }
    return
  }

  /**
   * Get MHDISL data
   */
  Closure<?> mhdislReader = { DBContainer mhdislResult ->

    double lineQty = mhdislResult.get("URTRQT")

    qty += lineQty
    logger.debug("In MHDISLData, URTRQT = ${lineQty} and qty = ${qty}" )

    itno = mhdislResult.get("URITNO")
    orno = mhdislResult.get("URRIDN")
    line = mhdislResult.get("URRIDL")
    //qty = ContainerMHDISL.get("URTRQT")
    qty = new BigDecimal(qty).setScale(6, RoundingMode.HALF_EVEN).doubleValue()

    DBAction mitmasQuery = database.table("MITMAS").index("00").selection("MMGRWE","MMVOL3").build()
    DBContainer mitmasRequest = mitmasQuery.getContainer()
    mitmasRequest.set("MMCONO", currentCompany)
    mitmasRequest.set("MMITNO", itno)

    if(mitmasQuery.read(mitmasRequest)){

      double grweMitmas = mitmasRequest.get("MMGRWE")
      double vol3Mitmas = mitmasRequest.get("MMVOL3")

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

    DBAction oolineQuery = database.table("OOLINE").index("00").selection("OBLNAM").build()
    DBContainer oolineRequest = oolineQuery.getContainer()
    oolineRequest.set("OBCONO", currentCompany)
    oolineRequest.set("OBORNO", orno)
    oolineRequest.set("OBPONR", line)
    oolineRequest.set("OBPOSX", 0)

    if(oolineQuery.read(oolineRequest)){
      double lnamOoline = oolineRequest.get("OBLNAM")
      lnamOoline = new BigDecimal(lnamOoline).setScale(6, RoundingMode.HALF_EVEN).doubleValue()
      if(previousDlixMhdish.equals(dlixMhdish)){
        lnam += lnamOoline
      }else{
        lnam = lnamOoline
      }

      lnam = new BigDecimal(lnam).setScale(6, RoundingMode.HALF_EVEN).doubleValue()
    }

    DBAction mitaunQuery = database.table("MITAUN").index("00").selection("MUCOFA").build()
    DBContainer mitaunRequest = mitaunQuery.getContainer()
    mitaunRequest.set("MUCONO", currentCompany)
    mitaunRequest.set("MUITNO", itno)
    mitaunRequest.set("MUAUTP", 1)
    mitaunRequest.set("MUALUN", "COL")

    if(mitaunQuery.read(mitaunRequest)){
      logger.debug("In MITMAS, lineQty is ${lineQty}")
      double cofa = mitaunRequest.get("MUCOFA")
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
  Closure<?> ext056Reader = { DBContainer ext056Result ->
    String dossierEXT056 = ext056Result.get("EXUCA4")
    String semaineEXT056 = ext056Result.get("EXUCA5")
    String anneeEXT056 = ext056Result.get("EXUCA6")
    String indexEXT056 = ext056Result.getLong("EXDLIX")
    String shipmentEXT056 = ext056Result.get("EXCONN")
    String arrivalDateEXT056 = ext056Result.get("EXETAD")
    String departureDateEXT056 = ext056Result.get("EXETDD")
    String packagindDateEXT056 = ext056Result.get("EXCCUD")
    String colsEXT056 = ext056Result.get("EXCOLS")
    String grweEXT056 = ext056Result.get("EXGRWE")
    String vol3EXT056 = ext056Result.get("EXVOL3")
    String lnamEXT056 = ext056Result.get("EXLNAM")

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
}
