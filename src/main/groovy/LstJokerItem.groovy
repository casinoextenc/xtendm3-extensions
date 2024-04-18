/**
 * README
 * This extension is used by Mashup
 *
 * Name : EXT050MI.LstJokerItem
 * Description : List Joker items
 * Date         Changed By   Description
 * 20230602     SEAR         LOG28 - Creation of files and containers
 */
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.math.RoundingMode
import java.text.DecimalFormat

public class LstJokerItem extends ExtendM3Transaction {
  private final MIAPI mi
  private final LoggerAPI logger
  private final ProgramAPI program
  private final DatabaseAPI database
  private final SessionAPI session
  private final TransactionAPI transaction
  private final MICallerAPI miCaller
  private final UtilityAPI utility
  private String orno_input
  private Integer currentCompany
  private String baseUnit
  private String ItemNumber
  private String description
  private double totQuantity
  private double palQuantity
  private String supplier
  private String faci_OOLINE
  private String cuno_OOLINE
  private String whlo_OOLINE
  private int sanitary
  private double free2
  private int dangerous
  private double aval
  private double MITAUN_cofa
  private int dmcf
  private double Volume
  private double Weight
  private boolean exclude_line
  private String orco
  private String popn
  private String suno
  private String ascd
  private String currentDate

  private String jobNumber

  public LstJokerItem(LoggerAPI logger, MIAPI mi, DatabaseAPI database, ProgramAPI program, MICallerAPI miCaller, UtilityAPI utility) {
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
    currentDate = utility.call("DateUtil", "currentDateY8AsString")

    if (mi.in.get("CONO") == null) {
      currentCompany = (Integer)program.getLDAZD().CONO
    } else {
      currentCompany = mi.in.get("CONO")
    }

    //Get mi inputs
    orno_input = (mi.in.get("ORNO") != null ? (String)mi.in.get("ORNO") : "")

    // check order number
    DBAction query_OOHEAD = database.table("OOHEAD").index("00").selection("OAORNO").build()
    DBContainer OOHEAD = query_OOHEAD.getContainer()
    OOHEAD.set("OACONO", currentCompany)
    OOHEAD.set("OAORNO", orno_input)
    if(!query_OOHEAD.read(OOHEAD)){
      mi.error("Numéro de commande " + orno_input + " n'existe pas")
      return
    }

    // Retrieve informations from first order line
    DBAction query_OOLINE = database.table("OOLINE").index("00").selection("OBORNO","OBCUNO","OBWHLO","OBFACI","OBITNO").build()
    DBContainer OOLINE = query_OOLINE.getContainer()
    OOLINE.set("OBCONO", currentCompany)
    OOLINE.set("OBORNO", orno_input)
    if(query_OOLINE.readAll(OOLINE, 2, 1, OOLINEData)){
    }

    // Write selected customer assortment items in EXT055
    ascd = ""
    ExpressionFactory expression_OASCUS = database.getExpressionFactory("OASCUS")
    expression_OASCUS = expression_OASCUS.le("OCFDAT", currentDate)
    expression_OASCUS = expression_OASCUS.and((expression_OASCUS.ge("OCTDAT", currentDate)).or(expression_OASCUS.eq("OCTDAT", "0")))
    DBAction OASCUS_query = database.table("OASCUS").index("10").matching(expression_OASCUS).selection("OCFDAT","OCTDAT","OCASCD").build()
    DBContainer OASCUS = OASCUS_query.getContainer()
    OASCUS.set("OCCONO", currentCompany)
    OASCUS.set("OCCUNO", cuno_OOLINE)
    if (!OASCUS_query.readAll(OASCUS, 2, OASCUSData)) {
    }

    // Read EXT055 for  out data
    DBAction ListqueryEXT055 = database.table("EXT055")
        .index("00")
        .selection(
        "EXBJNO",
        "EXCONO",
        "EXITNO",
        "EXITDS",
        "EXZAV1",
        "EXZAV2",
        "EXZQUV",
        "EXZPQA",
        "EXGRWE",
        "EXVOL3",
        "EXCOFA"
        )
        .build()

    DBContainer ListContainerEXT055 = ListqueryEXT055.getContainer()
    ListContainerEXT055.set("EXBJNO", jobNumber)

    //Record exists
    if (!ListqueryEXT055.readAll(ListContainerEXT055, 1, outData)){
    }
  }

  // data OOLINE
  Closure<?> OOLINEData = { DBContainer ContainerOOLINE ->
    cuno_OOLINE = ContainerOOLINE.get("OBCUNO")
    faci_OOLINE = ContainerOOLINE.get("OBFACI")
    whlo_OOLINE = ContainerOOLINE.get("OBWHLO")
    logger.debug("cuno_OOLINE = " + cuno_OOLINE)
    logger.debug("faci_OOLINE = " + faci_OOLINE)
    logger.debug("whlo_OOLINE = " + whlo_OOLINE)
  }

  Closure<?> outData_MITPOP = { DBContainer MITPOP ->
    popn = MITPOP.get("MPPOPN")
  }

  Closure<?> OASCUSData = { DBContainer OASCUS ->
    ascd = OASCUS.get("OCASCD")
    // get OASITN
    ItemNumber = ""
    ExpressionFactory expression_OASITN = database.getExpressionFactory("OASITN")
    expression_OASITN = expression_OASITN.le("OIFDAT", currentDate)
    expression_OASITN = expression_OASITN.and((expression_OASITN.ge("OITDAT", currentDate)).or(expression_OASITN.eq("OITDAT", "0")))
    DBAction OASITN_query = database.table("OASITN").index("00").matching(expression_OASITN).selection("OIFDAT","OITDAT","OIITNO").build()
    DBContainer OASITN = OASITN_query.getContainer()
    OASITN.set("OICONO", currentCompany)
    OASITN.set("OIASCD", ascd)
    if (!OASITN_query.readAll(OASITN, 2, OASITNData)) {
    }
  }

  Closure<?> OASITNData = { DBContainer OASITN ->
    ItemNumber = OASITN.get("OIITNO")
    logger.debug("ItemNumber------------------------------------- = " + ItemNumber)

    exclude_line = false

    // get MITMAS
    dangerous = 0
    free2 = 0
    description = ""
    supplier = ""
    Volume = 0
    Weight = 0
    DBAction query_MITMAS = database.table("MITMAS").index("00").selection("MMITDS","MMHAZI","MMCFI2","MMSUNO","MMVOL3","MMGRWE").build()
    DBContainer MITMAS = query_MITMAS.getContainer()
    MITMAS.set("MMCONO", currentCompany)
    MITMAS.set("MMITNO", ItemNumber)
    if(query_MITMAS.read(MITMAS)){
      dangerous = MITMAS.getInt("MMHAZI")
      free2 = MITMAS.getDouble("MMCFI2")
      description = MITMAS.get("MMITDS")
      supplier = MITMAS.get("MMSUNO")
      Volume = MITMAS.getDouble("MMVOL3")
      Weight = MITMAS.getDouble("MMGRWE")
    }
    logger.debug("dangerous = " + dangerous)
    logger.debug("free2 = " + free2)
    logger.debug("description = " + description)
    logger.debug("supplier = " + supplier)
    logger.debug("Volume = " + Volume)
    logger.debug("Weight = " + Weight)

    // get MITBAL
    aval = 0
    DBAction MITBAL_query = database.table("MITBAL").index("00").selection("MBAVAL").build()
    DBContainer MITBAL = MITBAL_query.getContainer()
    MITBAL.set("MBCONO", currentCompany)
    MITBAL.set("MBWHLO", whlo_OOLINE)
    MITBAL.set("MBITNO", ItemNumber)
    if(MITBAL_query.read(MITBAL)){
      aval = MITBAL.getDouble("MBAVAL")
    }
    logger.debug("aval = " + aval)
    
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
    logger.debug("popn = " + popn)

    orco = ""
    DBAction MITFAC_query = database.table("MITFAC").index("00").selection("M9CSNO","M9ORCO").build()
    DBContainer MITFAC = MITFAC_query.getContainer()
    MITFAC.set("M9CONO", currentCompany)
    MITFAC.set("M9FACI", faci_OOLINE)
    MITFAC.set("M9ITNO", ItemNumber)
    if(MITFAC_query.read(MITFAC)){
      orco = MITFAC.get("M9ORCO")
    }
    logger.debug("orco = " + orco)

    sanitary = 0
    DBAction EXT032_query = database.table("EXT032").index("00").selection("EXZSAN").build()
    DBContainer EXT032 = EXT032_query.getContainer()
    EXT032.set("EXCONO", currentCompany)
    EXT032.set("EXPOPN", popn)
    EXT032.set("EXSUNO", supplier)
    EXT032.set("EXORCO", orco)
    if(EXT032_query.read(EXT032)){
      sanitary = EXT032.get("EXZSAN")
    }
    logger.debug("sanitary = " + sanitary)

    if (sanitary > 0 || aval <= 0 || free2 > 0 || dangerous != 0) {
      exclude_line = true
    }
    
    logger.debug("exclude_line----------- = " + exclude_line)
    
    // write in EXT055
    if (!exclude_line) {

      DBAction queryMITAUN00 = database.table("MITAUN").index("00").selection(
          "MUCONO",
          "MUITNO",
          "MUAUTP",
          "MUALUN",
          "MUCOFA",
          "MUDMCF"
          ).build()

      palQuantity = aval
      logger.debug("palQuantity : " + palQuantity)
      DBContainer containerMITAUN = queryMITAUN00.getContainer()
      containerMITAUN.set("MUCONO", currentCompany)
      containerMITAUN.set("MUITNO", ItemNumber)
      containerMITAUN.set("MUAUTP", 1)
      containerMITAUN.set("MUALUN", "UPA")
      if (queryMITAUN00.read(containerMITAUN)) {
        MITAUN_cofa = containerMITAUN.getDouble("MUCOFA")
        dmcf = containerMITAUN.getInt("MUDMCF")
        if (dmcf == 1) {
          palQuantity = aval / MITAUN_cofa
        } else {
          palQuantity = aval * MITAUN_cofa
        }
      }


      logger.debug("after palQuantity : " + palQuantity)
      DBAction queryEXT055 = database.table("EXT055")
          .index("00")
          .selection(
          "EXBJNO",
          "EXCONO",
          "EXITNO",
          "EXITDS",
          "EXZAV1",
          "EXZAV2",
          "EXZQUV",
          "EXZPQA",
          "EXGRWE",
          "EXVOL3",
          "EXRGDT",
          "EXRGTM",
          "EXLMDT",
          "EXCHNO",
          "EXCHID"
          )
          .build()

      DBContainer containerEXT055 = queryEXT055.getContainer()
      containerEXT055.set("EXBJNO", jobNumber)
      containerEXT055.set("EXCONO", currentCompany)
      containerEXT055.set("EXITNO", ItemNumber)

      //Record exists
      if (queryEXT055.read(containerEXT055)) {
        Closure<?> updateEXT055 = { LockedResult lockedResultEXT055 ->
          lockedResultEXT055.set("EXITDS", description)
          lockedResultEXT055.set("EXZAV1", aval)
          lockedResultEXT055.set("EXZAV2", palQuantity)
          lockedResultEXT055.set("EXZQUV", 0)
          lockedResultEXT055.set("EXZPQA", 0)
          lockedResultEXT055.set("EXGRWE", Weight)
          lockedResultEXT055.set("EXVOL3", Volume)
          lockedResultEXT055.set("EXCOFA", MITAUN_cofa)
          logger.debug("MITAUN_cofa : " + MITAUN_cofa)
          lockedResultEXT055.set("EXLMDT", utility.call("DateUtil", "currentDateY8AsInt"))
          lockedResultEXT055.setInt("EXCHNO", ((Integer)lockedResultEXT055.get("EXCHNO") + 1))
          lockedResultEXT055.set("EXCHID", program.getUser())
          lockedResultEXT055.update()
        }
        queryEXT055.readLock(containerEXT055, updateEXT055)
      } else {
        containerEXT055.set("EXBJNO", jobNumber)
        containerEXT055.set("EXCONO", currentCompany)
        containerEXT055.set("EXITNO", ItemNumber)
        containerEXT055.set("EXITDS", description)
        containerEXT055.set("EXZAV1", aval)
        containerEXT055.set("EXZAV2", palQuantity)
        containerEXT055.set("EXZQUV", 0)
        containerEXT055.set("EXZPQA", 0)
        containerEXT055.set("EXGRWE", Weight)
        containerEXT055.set("EXVOL3", Volume)
        logger.debug("MITAUN_cofa : " + MITAUN_cofa)
        containerEXT055.set("EXCOFA", MITAUN_cofa)
        containerEXT055.set("EXRGDT", utility.call("DateUtil", "currentDateY8AsInt"))
        containerEXT055.set("EXRGTM", utility.call("DateUtil", "currentTimeAsInt"))
        containerEXT055.set("EXLMDT", utility.call("DateUtil", "currentDateY8AsInt"))
        containerEXT055.set("EXCHNO", 1)
        containerEXT055.set("EXCHID", program.getUser())
        queryEXT055.insert(containerEXT055)
      }
    }
  }


  Closure<?> outData = { DBContainer containerEXT055 ->
    String ItemNumberEXT055 = containerEXT055.get("EXITNO")
    String descriptionEXT055 = containerEXT055.get("EXITDS")
    String dispoUvcEXT055 = containerEXT055.get("EXZAV1")
    String dispoPalEXT055 = containerEXT055.get("EXZAV2")
    String quantiteUvcEXT055 = containerEXT055.get("EXZQUV")
    String quantitePalEXT055 = containerEXT055.get("EXZPQA")
    String poidsEXT055 = containerEXT055.get("EXGRWE")
    String volumeEXT055 = containerEXT055.get("EXVOL3")
    String jobEXT055 = containerEXT055.get("EXBJNO")
    String cofaEXT055 = containerEXT055.get("EXCOFA")

    mi.outData.put("ITNO", ItemNumberEXT055)
    mi.outData.put("ITDS", descriptionEXT055)
    mi.outData.put("ZAV1", dispoUvcEXT055)
    mi.outData.put("ZAV2", dispoPalEXT055)
    mi.outData.put("ZQUV", quantiteUvcEXT055)
    mi.outData.put("ZPQA", quantitePalEXT055)
    mi.outData.put("GRWE", poidsEXT055)
    mi.outData.put("VOL3", volumeEXT055)
    mi.outData.put("BJNO", jobEXT055)
    mi.outData.put("COFA", cofaEXT055)
    mi.write()
  }
}

