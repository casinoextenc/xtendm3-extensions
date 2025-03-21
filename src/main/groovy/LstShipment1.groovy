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
  private final SessionAPI session
  private final TransactionAPI transaction
  private final MICallerAPI miCaller
  private final UtilityAPI utility
  private String parm
  private boolean validOrder
  private boolean sameWarehouse
  private boolean sameZNBC
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
  private double salesPrice
  private String baseUnit
  private String ItemNumber
  private double lnamOoline
  private double alqtOoline
  private double orqtOoline
  private int dmcsOoline
  private double cofsOoline
  private String spunOoline
  private String ltypOoline
  private double allocatedQuantity
  private double allocatedQuantityUB
  private double totALQT
  private double totGRWE
  private double totVOL3
  private double totZAAM
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
    ExpressionFactory expressionDradtr = database.getExpressionFactory("DRADTR")
    expressionDradtr = expressionDradtr.eq("DRUDE1", uca4Input)
    expressionDradtr = expressionDradtr.and(expressionDradtr.eq("DRUDE2", uca5Input))
    expressionDradtr = expressionDradtr.and(expressionDradtr.eq("DRUDE3", uca6Input))

    DBAction queryDradtr = database.table("DRADTR").index("00").matching(expressionDradtr).selection("DRCONN").build()
    DBContainer DRADTR = queryDradtr.getContainer()
    DRADTR.set("DRCONO", currentCompany)
    DRADTR.set("DRTLVL", 1)
    if(queryDradtr.readAll(DRADTR, 2, nbMaxRecord, DRADTRData)){
    }
  }

  // data DRADTR
  Closure<?> DRADTRData = { DBContainer ContainerDRADTR ->
    int conn = ContainerDRADTR.get("DRCONN")

    // get Shipment
    trca = ""
    ExpressionFactory expressionDconsi = database.getExpressionFactory("DCONSI")
    expressionDconsi = expressionDconsi.lt("DACSTL","60")

    DBAction queryDconsi = database.table("DCONSI").index("00").matching(expressionDconsi).selection("DACONN", "DATRCA").build()
    DBContainer DCONSI = queryDconsi.getContainer()
    DCONSI.set("DACONO", currentCompany)
    DCONSI.set("DACONN", conn)
    if(queryDconsi.read(DCONSI)){
      trca = DCONSI.get("DATRCA")

      // get Transportation equipments
      vol3 = 0
      tx15 = ""
      DBAction queryDcarri = database.table("DCARRI").index("00").selection("DCTRCA","DCTX15","DCVOL3","DCFRCP","DCGRWE").build()
      DBContainer DCARRI = queryDcarri.getContainer()
      DCARRI.set("DCCONO", currentCompany)
      DCARRI.set("DCTRCA", trca.trim())
      if(queryDcarri.read(DCARRI)){
        vol3 = DCARRI.get("DCVOL3")
        tx15 = DCARRI.get("DCTX15")
        frcp = DCARRI.get("DCGRWE")
      }
      logger.debug("CONN:${conn}")
      totALQT = 0
      totGRWE = 0
      totVOL3 = 0
      totZAAM = 0
      actualPals = new ArrayList<>()
      DBAction queryMhdish = database.table("MHDISH").index("20").selection("OQDLIX").build()
      DBContainer MHDISH = queryMhdish.getContainer()
      MHDISH.set("OQCONO", currentCompany)
      MHDISH.set("OQINOU", 1)
      MHDISH.set("OQCONN", conn)
      queryMhdish.readAll(MHDISH,3 , nbMaxRecord, MHDISHData)

      double ALQT = new BigDecimal (totALQT).setScale(6, RoundingMode.HALF_EVEN).doubleValue()
      double VOL3 = new BigDecimal (totVOL3).setScale(6, RoundingMode.HALF_EVEN).doubleValue()
      double GRWE = new BigDecimal (totGRWE).setScale(6, RoundingMode.HALF_EVEN).doubleValue()
      double ZAAM = new BigDecimal (totZAAM).setScale(6, RoundingMode.HALF_EVEN).doubleValue()

      mi.outData.put("CONN", conn.toString())
      mi.outData.put("TRCA", trca + "-" + tx15)
      mi.outData.put("ZVOL", vol3.toString())
      mi.outData.put("FRCP", frcp.toString())
      mi.outData.put("ALQT", ALQT.toString())
      mi.outData.put("VOL3", VOL3.toString())
      mi.outData.put("GRWE", GRWE.toString())
      mi.outData.put("ZAAM", ZAAM.toString())
      mi.outData.put("PALS", actualPals.size().toString())
      mi.write()
    }
  }

  // data MHSIDH
  Closure<?> MHDISHData = { DBContainer ContainerMHDISH ->

    Long dlix = ContainerMHDISH.get("OQDLIX")
    whlo = ContainerMHDISH.get("OQWHLO")

    DBAction queryMhdisl = database.table("MHDISL").index("00").selection("URDLIX","URRIDN","URRIDL","URRIDX").build()
    DBContainer MHDISL = queryMhdisl.getContainer()
    MHDISL.set("URCONO", currentCompany)
    MHDISL.set("URDLIX", dlix)
    MHDISL.set("URRORC", 3)
    queryMhdisl.readAll(MHDISL,3 , nbMaxRecord, MHDISLData)
  }

  // data MHSIDH
  Closure<?> MHDISLData = { DBContainer ContainerMHDISL ->
    commande = ContainerMHDISL.get("URRIDN")
    ligneCommande = ContainerMHDISL.get("URRIDL")
    suffixeCommande = ContainerMHDISL.get("URRIDX")

    DBAction queryMitalo = database.table("MITALO").index("20").selection("MQCAMU").build()
    DBContainer MITALO = queryMitalo.getContainer()
    MITALO.set("MQCONO",currentCompany)
    MITALO.set("MQTTYP",31)
    MITALO.set("MQRIDN",commande)

    logger.debug("prepare read MITALO")

    if(!queryMitalo.readAll(MITALO,3, nbMaxRecord, MITALOdata)){
    }

    // get OOLINE
    DBAction queryOoline = database.table("OOLINE").index("00").selection("OBCUNO","OBORNO","OBPONR","OBPOSX","OBSPUN","OBDMCS","OBCOFS","OBORQT","OBALQT","OBLNAM","OBITNO").build()
    DBContainer OOLINE = queryOoline.getContainer()
    OOLINE.set("OBCONO", currentCompany)
    OOLINE.set("OBORNO", commande)
    OOLINE.set("OBPONR",ligneCommande)
    OOLINE.set("OBPOSX", suffixeCommande)
    if(queryOoline.read(OOLINE)) {
      spunOoline = OOLINE.get("OBSPUN")
      cofsOoline = OOLINE.get("OBCOFS")
      orqtOoline = OOLINE.get("OBORQT")
      alqtOoline = OOLINE.get("OBALQT")
      lnamOoline = OOLINE.get("OBLNAM")
      dmcsOoline = OOLINE.get("OBDMCS")
      ItemNumber = OOLINE.get("OBITNO")
      logger.debug("OOLINE DATA ORNO:${commande} PONR:${ligneCommande} ITNO:${ItemNumber} DMCS:${dmcsOoline} COFS:${cofsOoline} ORQT:${orqtOoline} ALQT:${alqtOoline}")

      // get MITMAS
      baseUnit = ""
      volume = 0
      weight = 0
      DBAction queryMitmas = database.table("MITMAS").index("00").selection("MMPUUN","MMUNMS", "MMGRWE", "MMVOL3").build()
      DBContainer MITMAS = queryMitmas.getContainer()
      MITMAS.set("MMCONO", currentCompany)
      MITMAS.set("MMITNO", ItemNumber)
      if(queryMitmas.read(MITMAS)){
        baseUnit = MITMAS.get("MMUNMS")
        volume = MITMAS.getDouble("MMVOL3")
        weight = MITMAS.getDouble("MMGRWE")
      }
      logger.debug("OOLINE DATA UNMS:${baseUnit} SPUN:${spunOoline}")

      allocatedQuantityUB =  alqtOoline

      double ALQT = new BigDecimal (allocatedQuantityUB).setScale(6, RoundingMode.HALF_EVEN).doubleValue()
      double GRWE = new BigDecimal (allocatedQuantityUB * weight).setScale(6, RoundingMode.HALF_EVEN).doubleValue()
      double VOL3 = new BigDecimal (allocatedQuantityUB * volume).setScale(6, RoundingMode.HALF_EVEN).doubleValue()
      double ZAAM = new BigDecimal (lnamOoline).setScale(6, RoundingMode.HALF_EVEN).doubleValue()

      totALQT = totALQT + ALQT
      totGRWE = totGRWE + GRWE
      totVOL3 = totVOL3 + VOL3
      totZAAM = totZAAM + ZAAM
    }
  }

  /**
   * Get MITALO data
   */
  Closure <?> MITALOdata = { DBContainer ContainerMITALO ->
    String camu = ContainerMITALO.get("MQCAMU")
    logger.debug("CAMU = ${camu}")
    if(camu != "0" && !actualPals.contains(camu)){
      actualPals.add(camu)
    }

    logger.debug("In MITALO ActualPals size : ${actualPals.size()}")
  }
}
