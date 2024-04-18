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
  private String orno_input
  private String camu_input
  private Integer currentCompany
  private String cuno_OOHEAD
  private double volume
  private double weight
  private double salesPrice
  private String baseUnit
  private String ItemNumber
  private String description
  private String dossier
  private String whsl_MITALO
  private int ttyp_MITALO
  private double sapr_OOLINE
  private double alqt_OOLINE
  private int dmcs_OOLINE
  private double cofs_OOLINE
  private String spun_OOLINE
  private String orst_OOLINE
  private double allocatedQuantity
  private double allocatedQuantityUB
  private String commande
  private String faci_OOLINE
  private String cust_name
  private String cust_number
  private Long lineIndex
  private int conn_MHDISH
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
  private String bano_MITALO
  private String csno
  private String orco
  private String popn
  private String suno
  private int chb9
  private int expi

  private String jobNumber

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
    orno_input = (mi.in.get("ORNO") != null ? (String)mi.in.get("ORNO") : "")
    camu_input = (mi.in.get("CAMU") != null ? (String)mi.in.get("CAMU") : "")

    // list MITALO
    ExpressionFactory expression_MITALO = database.getExpressionFactory("MITALO")
    expression_MITALO = (expression_MITALO.eq("MQCAMU", camu_input))

    DBAction query_MITALO = database.table("MITALO").index("10").matching(expression_MITALO).selection("MQRIDN","MQRIDL","MQRIDX","MQRIDI","MQCAMU","MQALQT","MQITNO","MQBANO","MQWHLO","MQWHSL","MQTTYP").build()
    DBContainer MITALO = query_MITALO.getContainer()
    MITALO.set("MQCONO", currentCompany)
    MITALO.set("MQTTYP", 31)
    MITALO.set("MQRIDN", orno_input)
    if(query_MITALO.readAll(MITALO, 3, MITALOData)){
    }
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
    whsl_MITALO = ContainerMITALO.get("MQWHSL")
    ttyp_MITALO = ContainerMITALO.get("MQTTYP")
    bano_MITALO = ContainerMITALO.get("MQBANO")

    spun_OOLINE = ""
    // Get OOLINE
    DBAction query_OOLINE = database.table("OOLINE").index("00").selection("OBCUNO","OBORNO","OBPONR","OBPOSX","OBWHLO","OBSPUN","OBDMCS","OBCOFS","OBALQT","OBSAPR","OBORST","OBITNO","OBFACI").build()
    DBContainer OOLINE = query_OOLINE.getContainer()
    OOLINE.set("OBCONO", currentCompany)
    OOLINE.set("OBORNO", ridn_MITALO)
    OOLINE.set("OBPONR", ridl_MITALO)
    OOLINE.set("OBPOSX", ridx_MITALO)
    if(query_OOLINE.read(OOLINE)){
      spun_OOLINE = OOLINE.get("OBSPUN")
      cofs_OOLINE = OOLINE.get("OBCOFS")
      alqt_OOLINE = OOLINE.get("OBALQT")
      sapr_OOLINE = OOLINE.get("OBSAPR")
      dmcs_OOLINE = OOLINE.get("OBDMCS")
      orst_OOLINE = OOLINE.get("OBORST")
      faci_OOLINE = OOLINE.get("OBFACI")
    }

    // get MITMAS
    baseUnit = ""
    suno = ""
    free2 = 0
    dangerous = 0
    DBAction query_MITMAS = database.table("MITMAS").index("00").selection("MMITDS", "MMPUUN","MMUNMS", "MMGRWE", "MMVOL3","MMHAZI","MMCFI2","MMSUNO").build()
    DBContainer MITMAS = query_MITMAS.getContainer()
    MITMAS.set("MMCONO", currentCompany)
    MITMAS.set("MMITNO", ItemNumber)
    if(query_MITMAS.read(MITMAS)){
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
    containerMILOMA.set("LMBANO", bano_MITALO)

    if (queryMILOMA.read(containerMILOMA)) {
      expi = containerMILOMA.get("LMEXPI")
    }

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

    sanitary = 0
    DBAction EXT032_query = database.table("EXT032").index("00").selection("EXZSAN").build()
    DBContainer EXT032 = EXT032_query.getContainer()
    EXT032.set("EXCONO", currentCompany)
    EXT032.set("EXPOPN", popn)
    EXT032.set("EXSUNO", suno)
    EXT032.set("EXORCO", orco)
    if(EXT032_query.read(EXT032)){
      sanitary = EXT032.get("EXZSAN")
    }

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
    double ZAAM = new BigDecimal (allocatedQuantityUB * sapr_OOLINE).setScale(6, RoundingMode.HALF_EVEN).doubleValue()


    mi.outData.put("CAMU", camu_MITALO)
    mi.outData.put("ORNO", ridn_MITALO)
    mi.outData.put("PONR", ridl_MITALO.toString())
    mi.outData.put("CAMU", camu_MITALO)
    mi.outData.put("ORST", orst_OOLINE)
    mi.outData.put("ITNO", ItemNumber)
    mi.outData.put("EXPI", expi.toString())
    mi.outData.put("ITDS", description)
    mi.outData.put("HAZI", dangerous.toString())
    mi.outData.put("CFI2", free2.toString())
    mi.outData.put("ZSAN", sanitary.toString())
    mi.outData.put("CHB9", chb9.toString())
    mi.outData.put("BANO", bano_MITALO)
    mi.outData.put("ALQT", ALQT.toString())
    mi.outData.put("GRWE", GRWE.toString())
    mi.outData.put("VOL3", VOL3.toString())
    mi.outData.put("ZAAM", ZAAM.toString())
    mi.outData.put("WHSL", whsl_MITALO.toString())
    mi.write()
  }

  Closure<?> DataMHDISL = { DBContainer containerMHDISL ->
    lineIndex = containerMHDISL.getLong("URDLIX")
    sameIndex = true
    foundLineIndex = true
  }

  Closure<?> outData_MITPOP = { DBContainer MITPOP ->
    popn = MITPOP.get("MPPOPN")
  }
}

