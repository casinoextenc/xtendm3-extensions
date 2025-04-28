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

  private final MICallerAPI miCaller
  private final UtilityAPI utility

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

    ExpressionFactory ooheadExpression = database.getExpressionFactory("OOHEAD")
    ooheadExpression = ooheadExpression.eq("OAUCA4", uca4Input)
    ooheadExpression = ooheadExpression.and(ooheadExpression.eq("OAUCA5", uca5Input))
    ooheadExpression = ooheadExpression.and(ooheadExpression.eq("OAUCA6", uca6Input))

    DBAction ooheadQuery = database.table("OOHEAD").index("00").matching(ooheadExpression).selection("OAORNO", "OAUCA4","OAUCA5","OAUCA6","OACUNO").build()
    DBContainer ooheadRequest = ooheadQuery.getContainer()
    ooheadRequest.set("OACONO", currentCompany)

    if (!ooheadQuery.readAll(ooheadRequest, 1, nbMaxRecord, ooheadReader)){
      mi.error("aucune commande associée au dossier "+ uca4Input)
    }
  }

  /**
   * Retrieve OOHEAD data
   * @param containerOOHEAD
   */
  Closure<?> ooheadReader = { DBContainer ooheadResult ->

    commande = ooheadResult.get("OAORNO")
    masterFile = ooheadResult.get("OAUCA4")
    week = ooheadResult.get("OAUCA5")
    year = ooheadResult.get("OAUCA6")
    customer = ooheadResult.get("OACUNO")

    DBAction ocusmaQuery = database.table("OCUSMA").index("00").selection("OKCUNM").build()
    DBContainer ocusmaRequest = ocusmaQuery.getContainer()
    ocusmaRequest.set("OKCONO", currentCompany)
    ocusmaRequest.set("OKCUNO", customer)

    if(ocusmaQuery.read(ocusmaRequest)){
      custName = ocusmaRequest.get("OKCUNM").toString().trim()
    }

    ExpressionFactory oolineExpression = database.getExpressionFactory("OOLINE")
    oolineExpression = oolineExpression.lt("OBORST","44")

    // list OOLINE
    DBAction oolineQuery = database.table("OOLINE").index("00").matching(oolineExpression).selection("OBITNO", "OBRORC","OBORNO","OBRORN","OBRORL","OBORQT","OBSAPR").build()
    DBContainer oolineRequest = oolineQuery.getContainer()
    oolineRequest.set("OBCONO", currentCompany)
    oolineRequest.set("OBORNO", commande)
    if(oolineQuery.readAll(oolineRequest, 2, nbMaxRecord, oolineReader)){
    }
  }

  /**
   * Retrieve OOLINE data
   * @param containerOOLINE
   */
  Closure<?> oolineReader = { DBContainer oolineResult ->
    ItemNumber = oolineResult.get("OBITNO")
    int refOrdCat = oolineResult.get("OBRORC")
    refOrderNumber = oolineResult.get("OBRORN")
    int refOrderNumberLine = oolineResult.get("OBRORL")
    orderedQuantity = oolineResult.getDouble("OBORQT")
    salesPrice = oolineResult.get("OBSAPR")

    // get MITMAS
    baseUnit = ""
    DBAction mitmasQuery = database.table("MITMAS").index("00").selection("MMITDS", "MMPUUN","MMUNMS", "MMGRWE", "MMVOL3").build()
    DBContainer mitmasRequest = mitmasQuery.getContainer()
    mitmasRequest.set("MMCONO", currentCompany)
    mitmasRequest.set("MMITNO", ItemNumber)
    if(mitmasQuery.read(mitmasRequest)){
      description = mitmasRequest.get("MMITDS")
      baseUnit = mitmasRequest.get("MMUNMS")
      volume = mitmasRequest.getDouble("MMVOL3")
      weight = mitmasRequest.getDouble("MMGRWE")
    }

    totConfirmqty = 0
    confirmedQuantityUB = 0
    if(refOrdCat == 2) {
      DBAction mplineQuery = database.table("MPLINE").index("00").selection("IBPUUN","IBITNO", "IBCFQA","IBPUNO").build()
      DBContainer mplineRequest = mplineQuery.getContainer()
      mplineRequest.set("IBCONO", currentCompany)
      mplineRequest.set("IBPUNO", refOrderNumber)
      mplineRequest.set("IBPNLI", refOrderNumberLine)
      mplineQuery.readAll(mplineRequest, 3, nbMaxRecord, mplineReader)
    }

    double zgr1 =  new BigDecimal (orderedQuantity * weight).setScale(6, RoundingMode.HALF_EVEN).doubleValue()
    double zgr2 =  new BigDecimal (totConfirmqty * weight).setScale(6, RoundingMode.HALF_EVEN).doubleValue()
    double zvO1 =  new BigDecimal (orderedQuantity * volume).setScale(6, RoundingMode.HALF_EVEN).doubleValue()
    double zvO2 =  new BigDecimal (totConfirmqty * volume).setScale(6, RoundingMode.HALF_EVEN).doubleValue()
    double zam1 =  new BigDecimal (orderedQuantity * salesPrice).setScale(6, RoundingMode.HALF_EVEN).doubleValue()
    double zam2 =  new BigDecimal (totConfirmqty * salesPrice).setScale(6, RoundingMode.HALF_EVEN).doubleValue()

    logger.debug("Before OutDatas Cust : ${customer} , ${custName}")

    mi.outData.put("UCA4", masterFile)
    mi.outData.put("ORNO", commande)
    mi.outData.put("RORN", refOrderNumber)
    mi.outData.put("ITNO", ItemNumber)
    mi.outData.put("ITDS", description)
    mi.outData.put("ORQT", orderedQuantity.toString())
    mi.outData.put("CFQA", totConfirmqty.toString())
    mi.outData.put("ZGR1", zgr1.toString())
    mi.outData.put("ZGR2", zgr2.toString())
    mi.outData.put("ZVO1", zvO1.toString())
    mi.outData.put("ZVO2", zvO2.toString())
    mi.outData.put("ZAM1", zam1.toString())
    mi.outData.put("ZAM2", zam2.toString())
    mi.outData.put("CUNO",customer)
    mi.outData.put("CUNM",custName)

    mi.write()
  }

  /**
   * Retrieve MPLINE data
   * @param containerMPLINE
   */
  Closure<?> mplineReader = { DBContainer mplineResult ->
    String purchaseItem = mplineResult.get("IBITNO")
    double confirmedQuantity = mplineResult.get("IBCFQA")
    String purchaseUnit = mplineResult.get("IBPUUN")
    if (!baseUnit.equals(purchaseUnit)) {
      DBAction mitaunQuery = database.table("MITAUN").index("00").selection(
        "MUCONO",
        "MUITNO",
        "MUAUTP",
        "MUALUN",
        "MUCOFA",
        "MUDMCF"
      ).build()

      DBContainer mitaunRequest = mitaunQuery.getContainer()
      mitaunRequest.set("MUCONO", currentCompany)
      mitaunRequest.set("MUITNO", purchaseItem)
      mitaunRequest.set("MUAUTP", 1)
      mitaunRequest.set("MUALUN", purchaseUnit)

      if (mitaunQuery.read(mitaunRequest)){
        String alun = mitaunRequest.getString("MUALUN").toString()
        mitaunCofa = mitaunRequest.getDouble("MUCOFA")
        dmcf = mitaunRequest.getInt("MUDMCF")
      }

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


}

