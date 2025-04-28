/**
 * README
 * This extension is used by Mashup
 *
 * Name : EXT050MI.LstShipment1
 * Description : batch template
 * Date         Changed By   Description
 * 20230511     SEAR         LOG28 - Creation of files and containers
 * 20241010     MLECLERCQ    LOG28 - Added palets count per container
 */

import java.math.RoundingMode
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

public class LstShipment1 extends ExtendM3Transaction {
  private final MIAPI mi
  private final LoggerAPI logger
  private final ProgramAPI program
  private final DatabaseAPI database

  private final MICallerAPI miCaller
  private final UtilityAPI utility


  private String uca4Input
  private String uca5Input
  private String uca6Input
  private String trca
  private String tx15
  private double frcp
  private int currentCompany
  private double vol3
  private String commande
  private int ligneCommande
  private int suffixeCommande
  private double volume
  private double weight

  private String baseUnit
  private String ItemNumber
  private double lnamOoline
  private double alqtOoline
  private double orqtOoline
  private int dmcsOoline
  private double cofsOoline
  private String spunOoline

  private double allocatedQuantityUB
  private double totAlqt
  private double totGrwe
  private double totVol3
  private double totZaam
  private String whlo
  private ArrayList<String> actualPals
  private Integer nbMaxRecord = 10000

  private String jobNumber

  public LstShipment1(LoggerAPI logger, MIAPI mi, DatabaseAPI database, ProgramAPI program, MICallerAPI miCaller, UtilityAPI utility) {
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

    currentCompany = (Integer)program.getLDAZD().CONO

    //Get mi inputs
    uca4Input = (mi.in.get("UCA4") != null ? (String)mi.in.get("UCA4") : "")
    uca5Input = (mi.in.get("UCA5") != null ? (String)mi.in.get("UCA5") : "")
    uca6Input = (mi.in.get("UCA6") != null ? (String)mi.in.get("UCA6") : "")

    // Get DRADTR
    ExpressionFactory dradtrExpression = database.getExpressionFactory("DRADTR")
    dradtrExpression = dradtrExpression.eq("DRUDE1", uca4Input)
    dradtrExpression = dradtrExpression.and(dradtrExpression.eq("DRUDE2", uca5Input))
    dradtrExpression = dradtrExpression.and(dradtrExpression.eq("DRUDE3", uca6Input))

    DBAction dradtrQuery = database.table("DRADTR").index("00").matching(dradtrExpression).selection("DRCONN").build()
    DBContainer dradtrRequest = dradtrQuery.getContainer()
    dradtrRequest.set("DRCONO", currentCompany)
    dradtrRequest.set("DRTLVL", 1)
    if(dradtrQuery.readAll(dradtrRequest, 2, nbMaxRecord, dradtrReader)){
    }
  }

  // data DRADTR
  Closure<?> dradtrReader = { DBContainer dradtrResult ->
    int conn = dradtrResult.get("DRCONN")

    // get Shipment
    trca = ""
    ExpressionFactory dconsiExpression = database.getExpressionFactory("DCONSI")
    dconsiExpression = dconsiExpression.lt("DACSTL","60")

    DBAction dconsiQuery = database.table("DCONSI").index("00").matching(dconsiExpression).selection("DACONN", "DATRCA").build()
    DBContainer dconsiRequest = dconsiQuery.getContainer()
    dconsiRequest.set("DACONO", currentCompany)
    dconsiRequest.set("DACONN", conn)
    if(dconsiQuery.read(dconsiRequest)){
      trca = dconsiRequest.get("DATRCA")

      // get Transportation equipments
      vol3 = 0
      tx15 = ""
      DBAction dcarriQuery = database.table("DCARRI").index("00").selection("DCTRCA","DCTX15","DCVOL3","DCFRCP","DCGRWE").build()
      DBContainer dcarriRequest = dcarriQuery.getContainer()
      dcarriRequest.set("DCCONO", currentCompany)
      dcarriRequest.set("DCTRCA", trca.trim())
      if(dcarriQuery.read(dcarriRequest)){
        vol3 = dcarriRequest.get("DCVOL3")
        tx15 = dcarriRequest.get("DCTX15")
        frcp = dcarriRequest.get("DCGRWE")
      }
      logger.debug("CONN:${conn}")
      totAlqt = 0
      totGrwe = 0
      totVol3 = 0
      totZaam = 0
      actualPals = new ArrayList<>()
      DBAction mhdishQuery = database.table("MHDISH").index("20").selection("OQDLIX").build()
      DBContainer mhdishRequest = mhdishQuery.getContainer()
      mhdishRequest.set("OQCONO", currentCompany)
      mhdishRequest.set("OQINOU", 1)
      mhdishRequest.set("OQCONN", conn)
      mhdishQuery.readAll(mhdishRequest,3 , nbMaxRecord, mhdishReader)

      double lalqt = new BigDecimal (totAlqt).setScale(6, RoundingMode.HALF_EVEN).doubleValue()
      double lvol3 = new BigDecimal (totVol3).setScale(6, RoundingMode.HALF_EVEN).doubleValue()
      double lgrwe = new BigDecimal (totGrwe).setScale(6, RoundingMode.HALF_EVEN).doubleValue()
      double lzaam = new BigDecimal (totZaam).setScale(6, RoundingMode.HALF_EVEN).doubleValue()

      mi.outData.put("CONN", conn.toString())
      mi.outData.put("TRCA", trca + "-" + tx15)
      mi.outData.put("ZVOL", vol3.toString())
      mi.outData.put("FRCP", frcp.toString())
      mi.outData.put("ALQT", lalqt.toString())
      mi.outData.put("VOL3", lvol3.toString())
      mi.outData.put("GRWE", lgrwe.toString())
      mi.outData.put("ZAAM", lzaam.toString())
      mi.outData.put("PALS", actualPals.size().toString())
      mi.write()
    }
  }

  // data MHSIDH
  Closure<?> mhdishReader = { DBContainer mhdishResult ->

    Long dlix = mhdishResult.get("OQDLIX")
    whlo = mhdishResult.get("OQWHLO")

    DBAction mhdislQuery = database.table("MHDISL").index("00").selection("URDLIX","URRIDN","URRIDL","URRIDX").build()
    DBContainer mhdislRequest = mhdislQuery.getContainer()
    mhdislRequest.set("URCONO", currentCompany)
    mhdislRequest.set("URDLIX", dlix)
    mhdislRequest.set("URRORC", 3)
    mhdislQuery.readAll(mhdislRequest,3 , nbMaxRecord, mhdislReader)
  }

  // data MHSIDH
  Closure<?> mhdislReader = { DBContainer mhdislResult ->
    commande = mhdislResult.get("URRIDN")
    ligneCommande = mhdislResult.get("URRIDL")
    suffixeCommande = mhdislResult.get("URRIDX")

    DBAction mitaloQuery = database.table("MITALO").index("20").selection("MQCAMU").build()
    DBContainer mitaloRequest = mitaloQuery.getContainer()
    mitaloRequest.set("MQCONO",currentCompany)
    mitaloRequest.set("MQTTYP",31)
    mitaloRequest.set("MQRIDN",commande)

    logger.debug("prepare read MITALO")

    if(!mitaloQuery.readAll(mitaloRequest,3, nbMaxRecord, mitaloReader)){
    }

    // get OOLINE
    DBAction oolineQuery = database.table("OOLINE").index("00").selection("OBCUNO","OBORNO","OBPONR","OBPOSX","OBSPUN","OBDMCS","OBCOFS","OBORQT","OBALQT","OBLNAM","OBITNO").build()
    DBContainer oolineRequest = oolineQuery.getContainer()
    oolineRequest.set("OBCONO", currentCompany)
    oolineRequest.set("OBORNO", commande)
    oolineRequest.set("OBPONR",ligneCommande)
    oolineRequest.set("OBPOSX", suffixeCommande)
    if(oolineQuery.read(oolineRequest)) {
      spunOoline = oolineRequest.get("OBSPUN")
      cofsOoline = oolineRequest.get("OBCOFS")
      orqtOoline = oolineRequest.get("OBORQT")
      alqtOoline = oolineRequest.get("OBALQT")
      lnamOoline = oolineRequest.get("OBLNAM")
      dmcsOoline = oolineRequest.get("OBDMCS")
      ItemNumber = oolineRequest.get("OBITNO")
      logger.debug("OOLINE DATA ORNO:${commande} PONR:${ligneCommande} ITNO:${ItemNumber} DMCS:${dmcsOoline} COFS:${cofsOoline} ORQT:${orqtOoline} ALQT:${alqtOoline}")

      // get MITMAS
      baseUnit = ""
      volume = 0
      weight = 0
      DBAction mitmasQuery = database.table("MITMAS").index("00").selection("MMPUUN","MMUNMS", "MMGRWE", "MMVOL3").build()
      DBContainer mitmasRequest = mitmasQuery.getContainer()
      mitmasRequest.set("MMCONO", currentCompany)
      mitmasRequest.set("MMITNO", ItemNumber)
      if(mitmasQuery.read(mitmasRequest)){
        baseUnit = mitmasRequest.get("MMUNMS")
        volume = mitmasRequest.getDouble("MMVOL3")
        weight = mitmasRequest.getDouble("MMGRWE")
      }
      logger.debug("OOLINE DATA UNMS:${baseUnit} SPUN:${spunOoline}")

      allocatedQuantityUB =  alqtOoline

      double lAlqt = new BigDecimal (allocatedQuantityUB).setScale(6, RoundingMode.HALF_EVEN).doubleValue()
      double lGrwe = new BigDecimal (allocatedQuantityUB * weight).setScale(6, RoundingMode.HALF_EVEN).doubleValue()
      double lVol3 = new BigDecimal (allocatedQuantityUB * volume).setScale(6, RoundingMode.HALF_EVEN).doubleValue()
      double lZaam = new BigDecimal (lnamOoline).setScale(6, RoundingMode.HALF_EVEN).doubleValue()

      totAlqt = totAlqt + lAlqt
      totGrwe = totGrwe + lGrwe
      totVol3 = totVol3 + lVol3
      totZaam = totZaam + lZaam
    }
  }

  /**
   * Get MITALO data
   */
  Closure <?> mitaloReader = { DBContainer mitaloResult ->
    String camu = mitaloResult.get("MQCAMU")
    logger.debug("CAMU = ${camu}")
    if(camu != "0" && !actualPals.contains(camu)){
      actualPals.add(camu)
    }

    logger.debug("In MITALO ActualPals size : ${actualPals.size()}")
  }
}
