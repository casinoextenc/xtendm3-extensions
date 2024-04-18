/**
 * README
 * This extension is used by Mashup
 *
 * Name : EXT050MI.LstMastFilOrdL1
 * Description : List master file order line
 * Date         Changed By   Description
 * 20230511     SEAR         LOG28 - Creation of files and containers
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
  private String itno_OOLINE
  private String uca4_input
  private int currentCompany
  private double tot_ConfirmQty
  private double confirmedQuantityUB
  private String baseUnit
  private double MITAUN_cofa
  private String dmcf
  private String MasterFile
  private String commande
  private String refOrderNumber
  private String ItemNumber
  private String description
  private double orderedQuantity
  private double volume
  private double weight
  private double salesPrice

  private String jobNumber

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
    uca4_input = (mi.in.get("UCA4") != null ? (String)mi.in.get("UCA4") : "")

    if (uca4_input.length() == 0) {
      mi.error("dossier maitre est obligatoire")
    }

    ExpressionFactory expression_OOHEAD = database.getExpressionFactory("OOHEAD")
    expression_OOHEAD = expression_OOHEAD.eq("OAUCA4", uca4_input)

    DBAction query_OOHEAD = database.table("OOHEAD").index("00").matching(expression_OOHEAD).selection("OAORNO", "OAUCA4").build()
    DBContainer containerOOHEAD = query_OOHEAD.getContainer()
    containerOOHEAD.set("OACONO", currentCompany)

    if (!query_OOHEAD.readAll(containerOOHEAD, 1, OOHEADData)){
      mi.error("aucune commande associée au dossier "+ uca4_input)
    }
  }

  // liste OOHEAD
  Closure<?> OOHEADData = { DBContainer containerOOHEAD ->

    commande = containerOOHEAD.get("OAORNO")
    MasterFile = containerOOHEAD.get("OAUCA4")

    // list OOLINE
    DBAction query_OOLINE = database.table("OOLINE").index("00").selection("OBITNO", "OBRORC","OBORNO","OBRORN","OBRORL","OBORQT","OBSAPR").build()
    DBContainer OOLINE = query_OOLINE.getContainer()
    OOLINE.set("OBCONO", currentCompany)
    OOLINE.set("OBORNO", commande)
    if(query_OOLINE.readAll(OOLINE, 2, OOLINEData)){
    }
  }

  // data OOLINE
  Closure<?> OOLINEData = { DBContainer ContainerOOLINE ->
    ItemNumber = ContainerOOLINE.get("OBITNO")
    int refOrdCat = ContainerOOLINE.get("OBRORC")
    refOrderNumber = ContainerOOLINE.get("OBRORN")
    int refOrderNumberLine = ContainerOOLINE.get("OBRORL")
    orderedQuantity = ContainerOOLINE.getDouble("OBORQT")
    salesPrice = ContainerOOLINE.get("OBSAPR")

    // get MITMAS
    baseUnit = ""
    DBAction query_MITMAS = database.table("MITMAS").index("00").selection("MMITDS", "MMPUUN","MMUNMS", "MMGRWE", "MMVOL3").build()
    DBContainer MITMAS = query_MITMAS.getContainer()
    MITMAS.set("MMCONO", currentCompany)
    MITMAS.set("MMITNO", ItemNumber)
    if(query_MITMAS.read(MITMAS)){
      description = MITMAS.get("MMITDS")
      baseUnit = MITMAS.get("MMUNMS")
      volume = MITMAS.getDouble("MMVOL3")
      weight = MITMAS.getDouble("MMGRWE")
    }

    tot_ConfirmQty = 0
    confirmedQuantityUB = 0
    if(refOrdCat == 2) {
      DBAction query_MPLINE = database.table("MPLINE").index("00").selection("IBPUUN","IBITNO", "IBCFQA","IBPUNO").build()
      DBContainer MPLINE = query_MPLINE.getContainer()
      MPLINE.set("IBCONO", currentCompany)
      MPLINE.set("IBPUNO", refOrderNumber)
      MPLINE.set("IBPNLI", refOrderNumberLine)
      query_MPLINE.readAll(MPLINE, 3, MPLINEData)
    }

    double ZGR1 =  new BigDecimal (orderedQuantity * weight).setScale(6, RoundingMode.HALF_EVEN).doubleValue()
    double ZGR2 =  new BigDecimal (tot_ConfirmQty * weight).setScale(6, RoundingMode.HALF_EVEN).doubleValue()
    double ZVO1 =  new BigDecimal (orderedQuantity * volume).setScale(6, RoundingMode.HALF_EVEN).doubleValue()
    double ZVO2 =  new BigDecimal (tot_ConfirmQty * volume).setScale(6, RoundingMode.HALF_EVEN).doubleValue()
    double ZAM1 =  new BigDecimal (orderedQuantity * salesPrice).setScale(6, RoundingMode.HALF_EVEN).doubleValue()
    double ZAM2 =  new BigDecimal (tot_ConfirmQty * salesPrice).setScale(6, RoundingMode.HALF_EVEN).doubleValue()
    mi.outData.put("UCA4", MasterFile)
    mi.outData.put("ORNO", commande)
    mi.outData.put("RORN", refOrderNumber)
    mi.outData.put("ITNO", ItemNumber)
    mi.outData.put("ITDS", description)
    mi.outData.put("ORQT", orderedQuantity.toString())
    mi.outData.put("CFQA", tot_ConfirmQty.toString())
    mi.outData.put("ZGR1", ZGR1.toString())
    mi.outData.put("ZGR2", ZGR2.toString())
    mi.outData.put("ZVO1", ZVO1.toString())
    mi.outData.put("ZVO2", ZVO2.toString())
    mi.outData.put("ZAM1", ZAM1.toString())
    mi.outData.put("ZAM2", ZAM2.toString())

    mi.write()
  }

  // data MPLINE
  Closure<?> MPLINEData = { DBContainer ContainerMPLINE ->
    String PurchaseItem = ContainerMPLINE.get("IBITNO")
    double confirmedQuantity = ContainerMPLINE.get("IBCFQA")
    String purchaseUnit = ContainerMPLINE.get("IBPUUN")
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
      queryMITAUN00.readAll(containerMITAUN, 4, readMITAUN)

      if (dmcf.equals("1")) {
        confirmedQuantityUB = confirmedQuantity * MITAUN_cofa
      } else {
        confirmedQuantityUB = confirmedQuantity / MITAUN_cofa
      }
      tot_ConfirmQty =  tot_ConfirmQty + confirmedQuantityUB
    } else {
      tot_ConfirmQty =  tot_ConfirmQty + confirmedQuantity
    }
    //mi.error("trouvé MPLINE avec données avec currentCompany : " + currentCompany + "  PurchaseItem:" + PurchaseItem + ", confirmedQuantity :" + confirmedQuantity + ", purchaseUnit : " + purchaseUnit+ ", baseUnit : " + baseUnit + " COFA : " + MITAUN_cofa + " dmcf " + dmcf + "tot : " + tot_ConfirmQty)
  }


  Closure<?> readMITAUN = { DBContainer resultMITAUN ->
    String alun = resultMITAUN.getString("MUALUN").toString()
    MITAUN_cofa = resultMITAUN.getDouble("MUCOFA")
    dmcf = resultMITAUN.getInt("MUDMCF")
  }
}

