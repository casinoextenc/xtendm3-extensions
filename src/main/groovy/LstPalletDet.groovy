/**
 * README
 * This extension is used by Mashup
 *
 * Name : EXT050MI.LstPalletDet
 * Description : List pallet detail
 * Date         Changed By   Description
 * 20230524     SEAR         LOG28 - Creation of files and containers
 */
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.math.RoundingMode

public class LstPalletDet extends ExtendM3Transaction {
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
  private String ornoInput
  private String camuInput
  private Integer currentCompany
  private String cunoOohead
  private double volume
  private double weight
  private double salesPrice
  private String baseUnit
  private String ItemNumber
  private String description
  private String dossier
  private String whslMitalo
  private int ttypMitalo
  private double saprOoline
  private double alqtOoline
  private int dmcsOoline
  private double cofsOoline
  private String spunOoline
  private String orstOoline
  private double allocatedQuantity
  private double allocatedQuantityUB
  private String commande
  private String faciOoline
  private String custName
  private String custNumber
  private Long lineIndex
  private int connMhdish
  private int sanitary
  private double free2
  private int dangerous

  private String ridnMitalo
  private int ridlMitalo
  private int ridxMitalo
  private String camuMitalo
  private Long ridiMitalo
  private double alqtMitalo
  private String whloMitalo
  private String banoMitalo
  private String csno
  private String orco
  private String popn
  private String suno
  private int chb9
  private int expi

  private String jobNumber
  private Integer nbMaxRecord = 10000

  public LstPalletDet(LoggerAPI logger, MIAPI mi, DatabaseAPI database, ProgramAPI program, MICallerAPI miCaller, UtilityAPI utility) {
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
    ornoInput = (mi.in.get("ORNO") != null ? (String)mi.in.get("ORNO") : "")
    camuInput = (mi.in.get("CAMU") != null ? (String)mi.in.get("CAMU") : "")

    // list MITALO
    ExpressionFactory expressionMitalo = database.getExpressionFactory("MITALO")
    expressionMitalo = (expressionMitalo.eq("MQCAMU", camuInput))

    DBAction queryMitalo = database.table("MITALO").index("10").matching(expressionMitalo).selection("MQRIDN","MQRIDL","MQRIDX","MQRIDI","MQCAMU","MQALQT","MQITNO","MQBANO","MQWHLO","MQWHSL","MQTTYP").build()
    DBContainer MITALO = queryMitalo.getContainer()
    MITALO.set("MQCONO", currentCompany)
    MITALO.set("MQTTYP", 31)
    MITALO.set("MQRIDN", ornoInput)
    if(queryMitalo.readAll(MITALO, 3, nbMaxRecord, MITALOData)){
    }
  }

  // data MITALO
  Closure<?> MITALOData = { DBContainer ContainerMITALO ->
    ridnMitalo = ContainerMITALO.get("MQRIDN")
    ridlMitalo = ContainerMITALO.get("MQRIDL")
    ridxMitalo = ContainerMITALO.get("MQRIDX")
    camuMitalo = ContainerMITALO.get("MQCAMU")
    ridiMitalo = ContainerMITALO.get("MQRIDI")
    alqtMitalo = ContainerMITALO.get("MQALQT")
    whloMitalo = ContainerMITALO.get("MQWHLO")
    ItemNumber = ContainerMITALO.get("MQITNO")
    whslMitalo = ContainerMITALO.get("MQWHSL")
    ttypMitalo = ContainerMITALO.get("MQTTYP")
    banoMitalo = ContainerMITALO.get("MQBANO")

    spunOoline = ""
    // Get OOLINE
    DBAction queryOoline = database.table("OOLINE").index("00").selection("OBCUNO","OBORNO","OBPONR","OBPOSX","OBWHLO","OBSPUN","OBDMCS","OBCOFS","OBALQT","OBSAPR","OBORST","OBITNO","OBFACI").build()
    DBContainer OOLINE = queryOoline.getContainer()
    OOLINE.set("OBCONO", currentCompany)
    OOLINE.set("OBORNO", ridnMitalo)
    OOLINE.set("OBPONR", ridlMitalo)
    OOLINE.set("OBPOSX", ridxMitalo)
    if(queryOoline.read(OOLINE)){
      spunOoline = OOLINE.get("OBSPUN")
      cofsOoline = OOLINE.get("OBCOFS")
      alqtOoline = OOLINE.get("OBALQT")
      saprOoline = OOLINE.get("OBSAPR")
      dmcsOoline = OOLINE.get("OBDMCS")
      orstOoline = OOLINE.get("OBORST")
      faciOoline = OOLINE.get("OBFACI")
    }

    // get MITMAS
    baseUnit = ""
    suno = ""
    free2 = 0
    dangerous = 0
    DBAction queryMitmas = database.table("MITMAS").index("00").selection("MMITDS", "MMPUUN","MMUNMS", "MMGRWE", "MMVOL3","MMHAZI","MMCFI2","MMSUNO").build()
    DBContainer MITMAS = queryMitmas.getContainer()
    MITMAS.set("MMCONO", currentCompany)
    MITMAS.set("MMITNO", ItemNumber)
    if(queryMitmas.read(MITMAS)){
      description = MITMAS.get("MMITDS")
      baseUnit = MITMAS.get("MMUNMS")
      volume = MITMAS.getDouble("MMVOL3")
      weight = MITMAS.getDouble("MMGRWE")
      dangerous = MITMAS.getInt("MMHAZI")
      free2 = MITMAS.getDouble("MMCFI2")
      suno = MITMAS.get("MMSUNO")
    }

    // get CUGEX1 CHB9
    chb9 = 0
    DBAction queryCUGEX100 = database.table("CUGEX1").index("00").selection("F1CONO",
      "F1FILE",
      "F1PK01",
      "F1PK02",
      "F1PK03",
      "F1PK04",
      "F1PK05",
      "F1PK06",
      "F1PK07",
      "F1PK08",
      "F1CHB9"
    ).build()

    DBContainer containerCUGEX1 = queryCUGEX100.getContainer()
    containerCUGEX1.set("F1CONO", currentCompany)
    containerCUGEX1.set("F1FILE", "MITMAS")
    containerCUGEX1.set("F1PK01", ItemNumber)

    if (queryCUGEX100.read(containerCUGEX1)) {
      chb9 = containerCUGEX1.get("F1CHB9")
    }

    expi = 0
    DBAction queryMILOMA = database.table("MILOMA").index("00").selection("LMCONO",
      "LMITNO",
      "LMBANO",
      "LMEXPI"
    ).build()

    DBContainer containerMILOMA = queryMILOMA.getContainer()
    containerMILOMA.set("LMCONO", currentCompany)
    containerMILOMA.set("LMITNO", ItemNumber)
    containerMILOMA.set("LMBANO", banoMitalo)

    if (queryMILOMA.read(containerMILOMA)) {
      expi = containerMILOMA.get("LMEXPI")
    }

    popn = ""
    ExpressionFactory expressionMitpop = database.getExpressionFactory("MITPOP")
    expressionMitpop = expressionMitpop.eq("MPREMK", "SIGMA6")
    DBAction mitpopQuery = database.table("MITPOP").index("00").matching(expressionMitpop).selection("MPPOPN").build()
    DBContainer MITPOP = mitpopQuery.getContainer()
    MITPOP.set("MPCONO", currentCompany)
    MITPOP.set("MPALWT", 1)
    MITPOP.set("MPALWQ", "")
    MITPOP.set("MPITNO", ItemNumber)
    if (!mitpopQuery.readAll(MITPOP, 4, nbMaxRecord, outdataMitpop)) {
    }

    csno =""
    orco = ""
    DBAction mitfacQuery = database.table("MITFAC").index("00").selection("M9CSNO","M9ORCO").build()
    DBContainer MITFAC = mitfacQuery.getContainer()
    MITFAC.set("M9CONO", currentCompany)
    MITFAC.set("M9FACI", faciOoline)
    MITFAC.set("M9ITNO", ItemNumber)
    if(mitfacQuery.read(MITFAC)){
      csno = MITFAC.get("M9CSNO")
      orco = MITFAC.get("M9ORCO")
    }

    sanitary = 0
    DBAction ext032Query = database.table("EXT032").index("00").selection("EXZSAN").build()
    DBContainer EXT032 = ext032Query.getContainer()
    EXT032.set("EXCONO", currentCompany)
    EXT032.set("EXPOPN", popn)
    EXT032.set("EXSUNO", suno)
    EXT032.set("EXORCO", orco)
    if(ext032Query.read(EXT032)){
      sanitary = EXT032.get("EXZSAN")
    }

    // convert to Basic unit
    allocatedQuantityUB =  alqtMitalo
    double ALQT = new BigDecimal (allocatedQuantityUB).setScale(6, RoundingMode.HALF_EVEN).doubleValue()
    double GRWE = new BigDecimal (allocatedQuantityUB * weight).setScale(6, RoundingMode.HALF_EVEN).doubleValue()
    double VOL3 = new BigDecimal (allocatedQuantityUB * volume).setScale(6, RoundingMode.HALF_EVEN).doubleValue()
    double ZAAM = new BigDecimal (allocatedQuantityUB * saprOoline).setScale(6, RoundingMode.HALF_EVEN).doubleValue()


    mi.outData.put("CAMU", camuMitalo)
    mi.outData.put("ORNO", ridnMitalo)
    mi.outData.put("PONR", ridlMitalo.toString())
    mi.outData.put("CAMU", camuMitalo)
    mi.outData.put("ORST", orstOoline)
    mi.outData.put("ITNO", ItemNumber)
    mi.outData.put("EXPI", expi.toString())
    mi.outData.put("ITDS", description)
    mi.outData.put("HAZI", dangerous.toString())
    mi.outData.put("CFI2", free2.toString())
    mi.outData.put("ZSAN", sanitary.toString())
    mi.outData.put("CHB9", chb9.toString())
    mi.outData.put("BANO", banoMitalo)
    mi.outData.put("ALQT", ALQT.toString())
    mi.outData.put("GRWE", GRWE.toString())
    mi.outData.put("VOL3", VOL3.toString())
    mi.outData.put("ZAAM", ZAAM.toString())
    mi.outData.put("WHSL", whslMitalo.toString())
    mi.write()
  }

  /**
   * Get MHDISL data
   */
  Closure<?> DataMHDISL = { DBContainer containerMHDISL ->
    lineIndex = containerMHDISL.getLong("URDLIX")
    sameIndex = true
    foundLineIndex = true
  }

  /**
   * Get MITPOP data
   */
  Closure<?> outdataMitpop = { DBContainer MITPOP ->
    popn = MITPOP.get("MPPOPN")
  }
}

