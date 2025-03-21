/**
 * README
 * This extension is used by Mashup
 *
 * Name : EXT050MI.LstMastFilShip1
 * Description : batch template
 * Date         Changed By   Description
 * 20230511     SEAR         LOG28 - Creation of files and containers V1
 * 20240425     MLECLERCQ    LOG28 - Added week and year filters
 */
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.math.RoundingMode

public class LstMastFilOrdL1 extends ExtendM3Transaction {
  private final MIAPI mi
  private final LoggerAPI logger
  private final ProgramAPI program
  private final DatabaseAPI database
  private final SessionAPI session
  private final TransactionAPI transaction
  private final MICallerAPI miCaller
  private final UtilityAPI utility
  private String parm
  private String itnoOoline
  private String uca4Input
  private String uca5Input
  private String uca6Input
  private int currentCompany
  private double totConfirmqty
  private double confirmedQuantityUB
  private String baseUnit
  private double mitaunCofa
  private String dmcf
  private String masterFile
  private String week
  private String year
  private String commande
  private String refOrderNumber
  private String ItemNumber
  private String description
  private double orderedQuantity
  private double volume
  private double weight
  private double salesPrice
  private String customer
  private String custName

  private String jobNumber

  private Integer nbMaxRecord = 10000

  public LstMastFilOrdL1(LoggerAPI logger, MIAPI mi, DatabaseAPI database, ProgramAPI program, MICallerAPI miCaller, UtilityAPI utility) {
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
    uca4Input = (mi.in.get("UCA4") != null ? (String)mi.in.get("UCA4") : "")
    uca5Input = (mi.in.get("UCA5") != null ? (String)mi.in.get("UCA5") : "")
    uca6Input = (mi.in.get("UCA6") != null ? (String)mi.in.get("UCA6") : "")

    if (uca4Input.length() == 0) {
      mi.error("dossier maitre est obligatoire")
    }

    ExpressionFactory expressionOohead = database.getExpressionFactory("OOHEAD")
    expressionOohead = expressionOohead.eq("OAUCA4", uca4Input)
    expressionOohead = expressionOohead.and(expressionOohead.eq("OAUCA5", uca5Input))
    expressionOohead = expressionOohead.and(expressionOohead.eq("OAUCA6", uca6Input))

    DBAction queryOohead = database.table("OOHEAD").index("00").matching(expressionOohead).selection("OAORNO", "OAUCA4","OAUCA5","OAUCA6","OACUNO").build()
    DBContainer containerOOHEAD = queryOohead.getContainer()
    containerOOHEAD.set("OACONO", currentCompany)

    if (!queryOohead.readAll(containerOOHEAD, 1, nbMaxRecord, OOHEADData)){
      mi.error("aucune commande associée au dossier "+ uca4Input)
    }
  }

  /**
   * Retrieve OOHEAD data
   * @param containerOOHEAD
   */
  Closure<?> OOHEADData = { DBContainer containerOOHEAD ->

    commande = containerOOHEAD.get("OAORNO")
    masterFile = containerOOHEAD.get("OAUCA4")
    week = containerOOHEAD.get("OAUCA5")
    year = containerOOHEAD.get("OAUCA6")
    customer = containerOOHEAD.get("OACUNO")

    DBAction queryOcusma = database.table("OCUSMA").index("00").selection("OKCUNM").build()
    DBContainer OCUSMA = queryOcusma.getContainer()
    OCUSMA.set("OKCONO", currentCompany)
    OCUSMA.set("OKCUNO", customer)

    if(queryOcusma.readAll(OCUSMA, 2, nbMaxRecord, OCUSMAData)){
    }

    ExpressionFactory oolineExp = database.getExpressionFactory("OOLINE")
    oolineExp = oolineExp.lt("OBORST","44")

    // list OOLINE
    DBAction queryOoline = database.table("OOLINE").index("00").matching(oolineExp).selection("OBITNO", "OBRORC","OBORNO","OBRORN","OBRORL","OBORQT","OBSAPR").build()
    DBContainer OOLINE = queryOoline.getContainer()
    OOLINE.set("OBCONO", currentCompany)
    OOLINE.set("OBORNO", commande)
    if(queryOoline.readAll(OOLINE, 2, nbMaxRecord, OOLINEData)){
    }
  }

  /**
   * Retrieve OCUSMA data
   * @param containerOCUSMA
   */
  Closure<?> OCUSMAData = { DBContainer containerOCUSMA ->
    custName = containerOCUSMA.get("OKCUNM").toString().trim()
    logger.debug("Customer Name : ${custName}")
  }

  /**
   * Retrieve OOLINE data
   * @param containerOOLINE
   */
  Closure<?> OOLINEData = { DBContainer containerOOLINE ->
    ItemNumber = containerOOLINE.get("OBITNO")
    int refOrdCat = containerOOLINE.get("OBRORC")
    refOrderNumber = containerOOLINE.get("OBRORN")
    int refOrderNumberLine = containerOOLINE.get("OBRORL")
    orderedQuantity = containerOOLINE.getDouble("OBORQT")
    salesPrice = containerOOLINE.get("OBSAPR")

    // get MITMAS
    baseUnit = ""
    DBAction queryMitmas = database.table("MITMAS").index("00").selection("MMITDS", "MMPUUN","MMUNMS", "MMGRWE", "MMVOL3").build()
    DBContainer MITMAS = queryMitmas.getContainer()
    MITMAS.set("MMCONO", currentCompany)
    MITMAS.set("MMITNO", ItemNumber)
    if(queryMitmas.read(MITMAS)){
      description = MITMAS.get("MMITDS")
      baseUnit = MITMAS.get("MMUNMS")
      volume = MITMAS.getDouble("MMVOL3")
      weight = MITMAS.getDouble("MMGRWE")
    }

    totConfirmqty = 0
    confirmedQuantityUB = 0
    if(refOrdCat == 2) {
      DBAction queryMpline = database.table("MPLINE").index("00").selection("IBPUUN","IBITNO", "IBCFQA","IBPUNO").build()
      DBContainer MPLINE = queryMpline.getContainer()
      MPLINE.set("IBCONO", currentCompany)
      MPLINE.set("IBPUNO", refOrderNumber)
      MPLINE.set("IBPNLI", refOrderNumberLine)
      queryMpline.readAll(MPLINE, 3, nbMaxRecord, MPLINEData)
    }

    double ZGR1 =  new BigDecimal (orderedQuantity * weight).setScale(6, RoundingMode.HALF_EVEN).doubleValue()
    double ZGR2 =  new BigDecimal (totConfirmqty * weight).setScale(6, RoundingMode.HALF_EVEN).doubleValue()
    double ZVO1 =  new BigDecimal (orderedQuantity * volume).setScale(6, RoundingMode.HALF_EVEN).doubleValue()
    double ZVO2 =  new BigDecimal (totConfirmqty * volume).setScale(6, RoundingMode.HALF_EVEN).doubleValue()
    double ZAM1 =  new BigDecimal (orderedQuantity * salesPrice).setScale(6, RoundingMode.HALF_EVEN).doubleValue()
    double ZAM2 =  new BigDecimal (totConfirmqty * salesPrice).setScale(6, RoundingMode.HALF_EVEN).doubleValue()

    logger.debug("Before OutDatas Cust : ${customer} , ${custName}")

    mi.outData.put("UCA4", masterFile)
    mi.outData.put("ORNO", commande)
    mi.outData.put("RORN", refOrderNumber)
    mi.outData.put("ITNO", ItemNumber)
    mi.outData.put("ITDS", description)
    mi.outData.put("ORQT", orderedQuantity.toString())
    mi.outData.put("CFQA", totConfirmqty.toString())
    mi.outData.put("ZGR1", ZGR1.toString())
    mi.outData.put("ZGR2", ZGR2.toString())
    mi.outData.put("ZVO1", ZVO1.toString())
    mi.outData.put("ZVO2", ZVO2.toString())
    mi.outData.put("ZAM1", ZAM1.toString())
    mi.outData.put("ZAM2", ZAM2.toString())
    mi.outData.put("CUNO",customer)
    mi.outData.put("CUNM",custName)

    mi.write()
  }

  /**
   * Retrieve MPLINE data
   * @param containerMPLINE
   */
  Closure<?> MPLINEData = { DBContainer containerMPLINE ->
    String PurchaseItem = containerMPLINE.get("IBITNO")
    double confirmedQuantity = containerMPLINE.get("IBCFQA")
    String purchaseUnit = containerMPLINE.get("IBPUUN")
    if (!baseUnit.equals(purchaseUnit)) {
      DBAction queryMITAUN00 = database.table("MITAUN").index("00").selection(
        "MUCONO",
        "MUITNO",
        "MUAUTP",
        "MUALUN",
        "MUCOFA",
        "MUDMCF"
      ).build()

      DBContainer containerMITAUN = queryMITAUN00.getContainer()
      containerMITAUN.set("MUCONO", currentCompany)
      containerMITAUN.set("MUITNO", PurchaseItem)
      containerMITAUN.set("MUAUTP", 1)
      containerMITAUN.set("MUALUN", purchaseUnit)
      queryMITAUN00.readAll(containerMITAUN, 4, nbMaxRecord, readMITAUN)

      if (dmcf.equals("1")) {
        confirmedQuantityUB = confirmedQuantity * mitaunCofa
      } else {
        confirmedQuantityUB = confirmedQuantity / mitaunCofa
      }
      totConfirmqty =  totConfirmqty + confirmedQuantityUB
    } else {
      totConfirmqty =  totConfirmqty + confirmedQuantity
    }
  }

  /**
   * Retrieve MITAUN data
   * @param resultMITAUN
   */
  Closure<?> readMITAUN = { DBContainer resultMITAUN ->
    String alun = resultMITAUN.getString("MUALUN").toString()
    mitaunCofa = resultMITAUN.getDouble("MUCOFA")
    dmcf = resultMITAUN.getInt("MUDMCF")
  }
}

