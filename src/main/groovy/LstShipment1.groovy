/**
 * README
 * This extension is used by Mashup
 *
 * Name : EXT050MI.LstShipment1
 * Description : List shipment
 * Date         Changed By   Description
 * 20230511     SEAR         LOG28 - Creation of files and containers
 */
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.math.RoundingMode

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
  private String uca4_input
  private String uca5_input
  private String uca6_input
  private String trca
  private String tx15
  private double frcp
  private int currentCompany
  private double vol3
  private String commande
  private int LigneCommande
  private int SuffixeCommande
  private double volume
  private double weight
  private double salesPrice
  private String baseUnit
  private String ItemNumber
  private double lnam_OOLINE
  private double alqt_OOLINE
  private double orqt_OOLINE
  private int dmcs_OOLINE
  private double cofs_OOLINE
  private String spun_OOLINE
  private String ltyp_OOLINE
  private double allocatedQuantity
  private double allocatedQuantityUB
  private double totALQT
  private double totGRWE
  private double totVOL3
  private double totZAAM



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
    uca4_input = (mi.in.get("UCA4") != null ? (String)mi.in.get("UCA4") : "")
    uca5_input = (mi.in.get("UCA5") != null ? (String)mi.in.get("UCA5") : "")
    uca6_input = (mi.in.get("UCA6") != null ? (String)mi.in.get("UCA6") : "")

    // Get DRADTR
    ExpressionFactory expression_DRADTR = database.getExpressionFactory("DRADTR")
    expression_DRADTR = expression_DRADTR.eq("DRUDE1", uca4_input)
    expression_DRADTR = expression_DRADTR.and(expression_DRADTR.eq("DRUDE2", uca5_input))
    expression_DRADTR = expression_DRADTR.and(expression_DRADTR.eq("DRUDE3", uca6_input))

    DBAction query_DRADTR = database.table("DRADTR").index("00").matching(expression_DRADTR).selection("DRCONN").build()
    DBContainer DRADTR = query_DRADTR.getContainer()
    DRADTR.set("DRCONO", currentCompany)
    DRADTR.set("DRTLVL", 1)
    if(query_DRADTR.readAll(DRADTR, 2, DRADTRData)){
    }
  }

  // data DRADTR
  Closure<?> DRADTRData = { DBContainer ContainerDRADTR ->
    int conn = ContainerDRADTR.get("DRCONN")

    // get Shipment
    trca = ""
    DBAction query_DCONSI = database.table("DCONSI").index("00").selection("DACONN", "DATRCA").build()
    DBContainer DCONSI = query_DCONSI.getContainer()
    DCONSI.set("DACONO", currentCompany)
    DCONSI.set("DACONN", conn)
    if(query_DCONSI.read(DCONSI)){
      trca = DCONSI.get("DATRCA")
    }

    // get Transportation equipments
    vol3 = 0
    tx15 = ""
    DBAction query_DCARRI = database.table("DCARRI").index("00").selection("DCTRCA","DCTX15","DCVOL3","DCFRCP").build()
    DBContainer DCARRI = query_DCARRI.getContainer()
    DCARRI.set("DCCONO", currentCompany)
    DCARRI.set("DCTRCA", trca.trim())
    if(query_DCARRI.read(DCARRI)){
      vol3 = DCARRI.get("DCVOL3")
      tx15 = DCARRI.get("DCTX15")
      frcp = DCARRI.get("DCFRCP")
    }
    logger.debug("CONN:${conn}")
    totALQT = 0
    totGRWE = 0
    totVOL3 = 0
    totZAAM = 0
    DBAction query_MHDISH = database.table("MHDISH").index("20").selection("OQDLIX").build()
    DBContainer MHDISH = query_MHDISH.getContainer()
    MHDISH.set("OQCONO", currentCompany)
    MHDISH.set("OQINOU", 1)
    MHDISH.set("OQCONN", conn)
    query_MHDISH.readAll(MHDISH,3 , MHDISHData)

    mi.outData.put("CONN", conn.toString())
    mi.outData.put("TRCA", trca + "-" + tx15)
    mi.outData.put("ZVOL", vol3.toString())
    mi.outData.put("FRCP", frcp.toString())
    mi.outData.put("ALQT", totALQT.toString())
    mi.outData.put("VOL3", totVOL3.toString())
    mi.outData.put("GRWE", totGRWE.toString())
    mi.outData.put("ZAAM", totZAAM.toString())
    mi.write()
  }

  // data MHSIDH
  Closure<?> MHDISHData = { DBContainer ContainerMHDISH ->

    Long dlix = ContainerMHDISH.get("OQDLIX")

    DBAction query_MHDISL = database.table("MHDISL").index("00").selection("URDLIX","URRIDN","URRIDL","URRIDX").build()
    DBContainer MHDISL = query_MHDISL.getContainer()
    MHDISL.set("URCONO", currentCompany)
    MHDISL.set("URDLIX", dlix)
    MHDISL.set("URRORC", 3)
    query_MHDISL.readAll(MHDISL,3 , MHDISLData)
  }

  // data MHSIDH
  Closure<?> MHDISLData = { DBContainer ContainerMHDISL ->
    commande = ContainerMHDISL.get("URRIDN")
    LigneCommande = ContainerMHDISL.get("URRIDL")
    SuffixeCommande = ContainerMHDISL.get("URRIDX")

    // get OOLINE
    DBAction query_OOLINE = database.table("OOLINE").index("00").selection("OBCUNO","OBORNO","OBPONR","OBPOSX","OBSPUN","OBDMCS","OBCOFS","OBORQT","OBALQT","OBLNAM","OBITNO").build()
    DBContainer OOLINE = query_OOLINE.getContainer()
    OOLINE.set("OBCONO", currentCompany)
    OOLINE.set("OBORNO", commande)
    OOLINE.set("OBPONR",LigneCommande)
    OOLINE.set("OBPOSX", SuffixeCommande)
    if(query_OOLINE.read(OOLINE)) {
      spun_OOLINE = OOLINE.get("OBSPUN")
      cofs_OOLINE = OOLINE.get("OBCOFS")
      orqt_OOLINE = OOLINE.get("OBORQT")
      alqt_OOLINE = OOLINE.get("OBALQT")
      lnam_OOLINE = OOLINE.get("OBLNAM")
      dmcs_OOLINE = OOLINE.get("OBDMCS")
      ItemNumber = OOLINE.get("OBITNO")
      logger.debug("OOLINE DATA ORNO:${commande} PONR:${LigneCommande} ITNO:${ItemNumber} DMCS:${dmcs_OOLINE} COFS:${cofs_OOLINE} ORQT:${orqt_OOLINE} ALQT:${alqt_OOLINE}")

      // get MITMAS
      baseUnit = ""
      volume = 0
      weight = 0
      DBAction query_MITMAS = database.table("MITMAS").index("00").selection("MMPUUN","MMUNMS", "MMGRWE", "MMVOL3").build()
      DBContainer MITMAS = query_MITMAS.getContainer()
      MITMAS.set("MMCONO", currentCompany)
      MITMAS.set("MMITNO", ItemNumber)
      if(query_MITMAS.read(MITMAS)){
        baseUnit = MITMAS.get("MMUNMS")
        volume = MITMAS.getDouble("MMVOL3")
        weight = MITMAS.getDouble("MMGRWE")
      }
      logger.debug("OOLINE DATA UNMS:${baseUnit} SPUN:${spun_OOLINE}")
      
      allocatedQuantityUB =  alqt_OOLINE
      
      /*
      if (!baseUnit.equals(spun_OOLINE)) {
        if (dmcs_OOLINE.equals("1")) {
          allocatedQuantityUB = alqt_OOLINE * dmcs_OOLINE
        } else {
          allocatedQuantityUB = alqt_OOLINE / dmcs_OOLINE
        }
      } else {
        allocatedQuantityUB =  alqt_OOLINE
      }*/

      double ALQT = new BigDecimal (allocatedQuantityUB).setScale(6, RoundingMode.HALF_EVEN).doubleValue()
      double GRWE = new BigDecimal (allocatedQuantityUB * weight).setScale(6, RoundingMode.HALF_EVEN).doubleValue()
      double VOL3 = new BigDecimal (allocatedQuantityUB * volume).setScale(6, RoundingMode.HALF_EVEN).doubleValue()
      double ZAAM = new BigDecimal (lnam_OOLINE).setScale(6, RoundingMode.HALF_EVEN).doubleValue()

      totALQT = totALQT + ALQT
      totGRWE = totGRWE + GRWE
      totVOL3 = totVOL3 + VOL3
      totZAAM = totZAAM + ZAAM
    }
  }
}