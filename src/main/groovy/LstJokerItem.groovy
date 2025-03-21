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

public class LstJokerItem extends ExtendM3Transaction {
  private final MIAPI mi
  private final LoggerAPI logger
  private final ProgramAPI program
  private final DatabaseAPI database
  private final SessionAPI session
  private final TransactionAPI transaction
  private final MICallerAPI miCaller
  private final UtilityAPI utility
  private Integer currentCompany
  private String baseUnit
  private String itnoInput
  private String cunoInput
  private String whloInput

  private String faci

  private Integer nrOfRecords

  private String currentItno
  private String lastReadItno
  private Integer lastSendCount

  private String description
  private double totQuantity
  private double palQuantity
  private String supplier
  private int sanitary
  private double free2
  private int dangerous
  private double aval
  private double mitaunCofa
  private int dmcf
  private double volume
  private double weight
  private String orco
  private String popn
  private String suno
  private String ascd
  private String expi
  private String currentDate
  private String jobNumber
  private LinkedHashMap<String, HashMap<String,String>> outmap

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
    currentDate = utility.call("DateUtil", "currentDateY8AsString")

    if(mi.in.get("BJNO") == null){
      jobNumber = program.getJobNumber() + timeOfCreation.format(DateTimeFormatter.ofPattern("yyMMdd")) + timeOfCreation.format(DateTimeFormatter.ofPattern("HHmmss"))
    }else{
      jobNumber = mi.in.get("BJNO")
    }

    if (mi.in.get("CONO") == null) {
      currentCompany = (Integer)program.getLDAZD().CONO
    } else {
      currentCompany = mi.in.get("CONO")
    }

    //Get mi inputs
    cunoInput = (mi.in.get("CUNO") != null ? (String)mi.in.get("CUNO") : "")
    itnoInput = (mi.in.get("ITNO") != null ? (String)mi.in.get("ITNO") : "")
    whloInput = (mi.in.get("WHLO") != null ? (String)mi.in.get("WHLO") : "")

    // check Customer
    DBAction queryOcusma = database.table("OCUSMA").index("00").selection("OKCUNO").build()
    DBContainer OCUSMA = queryOcusma.getContainer()
    OCUSMA.set("OKCONO", currentCompany)
    OCUSMA.set("OKCUNO", cunoInput)
    if(!queryOcusma.read(OCUSMA)){
      mi.error("Client " + cunoInput + " n'existe pas")
      return
    }

    // check ItemNumber
    if(!itnoInput.isEmpty()){
      DBAction queryMitmas = database.table("MITMAS").index("00").selection("MMITNO").build()
      DBContainer MITMAS = queryMitmas.getContainer()
      MITMAS.set("MMCONO", currentCompany)
      MITMAS.set("MMITNO", itnoInput)
      if(!queryMitmas.read(MITMAS)){
        mi.error("Article " + itnoInput + " n'existe pas")
        return
      }
    }

    // check wharehouse and get Facility if wharehouse exists
    DBAction queryMitwhl = database.table("MITWHL").index("00").selection("MWWHLO","MWFACI").build()
    DBContainer MITWHL = queryMitwhl.getContainer()
    MITWHL.set("MWCONO", currentCompany)
    MITWHL.set("MWWHLO", whloInput)
    if(!queryMitwhl.read(MITWHL)){
      mi.error("Dépôt " + whloInput + " n'existe pas")
      return
    }else{
      faci = MITWHL.get("MWFACI").toString().trim()
    }

    outmap = new LinkedHashMap()
    ascd = cunoInput + "0"
    lastSendCount = 0
    nrOfRecords = 500

    ExpressionFactory expressionOasitn = database.getExpressionFactory("OASITN")
    expressionOasitn = expressionOasitn.le("OIFDAT", currentDate)
    expressionOasitn = expressionOasitn.and((expressionOasitn.ge("OITDAT", currentDate)).or(expressionOasitn.eq("OITDAT", "0")))
    expressionOasitn = expressionOasitn.and((expressionOasitn.gt("OIITNO", itnoInput)))

    DBAction oasitnQuery = database.table("OASITN").index("00").matching(expressionOasitn).selection("OIFDAT","OITDAT","OIITNO").build()
    DBContainer OASITN = oasitnQuery.getContainer()
    OASITN.set("OICONO", currentCompany)
    OASITN.set("OIASCD", ascd)

    logger.debug("NbOfRecord : ${nrOfRecords}")
    if (!oasitnQuery.readAll(OASITN, 2,nrOfRecords, OASITNData)) {
    }
    Integer count = 0
    for(e in outmap){
      count += 1
      mi.outData.put("ITNO",e.key)

      for(p in e.value){
        mi.outData.put(p.key,p.value)
      }
      logger.debug("mi output count = ${count}")
      mi.write()
    }
    logger.debug("lastITNO : ${lastReadItno}")
    mi.outData.put("LAST",lastReadItno)
    mi.write()
  }

  /**
   * Retrieve MITPOP data
   */
  Closure<?> outDataMitpop = { DBContainer MITPOP ->
    popn = MITPOP.get("MPPOPN")
  }

  /**
   * Retrieve OASITN data
   */
  Closure<?> OASITNData = { DBContainer OASITN ->
    logger.debug("In closure lastSendCount  = ${lastSendCount} and input lastITNO : ${itnoInput}")
    if(lastSendCount <= 100){
      currentItno = OASITN.get("OIITNO")


      logger.debug("ItemNumber------------------------------------- = " + currentItno)

      // get MITMAS
      dangerous = 0
      free2 = 0
      description = ""
      supplier = ""
      volume = 0
      weight = 0
      ExpressionFactory expressionMitmas = database.getExpressionFactory("OASITN")
      expressionMitmas = expressionMitmas.eq("MMSTAT", "20")
      DBAction queryMitmas = database.table("MITMAS").index("00").matching(expressionMitmas).selection("MMITDS","MMHAZI","MMCFI2","MMSUNO","MMVOL3","MMGRWE").build()
      DBContainer MITMAS = queryMitmas.getContainer()
      MITMAS.set("MMCONO", currentCompany)
      MITMAS.set("MMITNO", currentItno)
      if(queryMitmas.read(MITMAS)){
        dangerous = MITMAS.getInt("MMHAZI")
        free2 = MITMAS.getDouble("MMCFI2")
        description = MITMAS.get("MMITDS")
        supplier = MITMAS.get("MMSUNO")
        volume = MITMAS.getDouble("MMVOL3")
        weight = MITMAS.getDouble("MMGRWE")
      }
      logger.debug("dangerous = " + dangerous)
      logger.debug("free2 = " + free2)
      logger.debug("description = " + description)
      logger.debug("supplier = " + supplier)
      logger.debug("volume = " + volume)
      logger.debug("weight = " + weight)

      if(dangerous != 0 || free2 != 0){
        lastReadItno = currentItno
        return
      }

      // get MITBAL
      aval = 0
      DBAction mitbalQuery = database.table("MITBAL").index("00").selection("MBAVAL","MBALQT").build()
      DBContainer MITBAL = mitbalQuery.getContainer()
      MITBAL.set("MBCONO", currentCompany)
      MITBAL.set("MBWHLO", whloInput)
      MITBAL.set("MBITNO", currentItno)
      if(mitbalQuery.read(MITBAL)){
        double alqt = MITBAL.getDouble("MBALQT")
        double mbaval = MITBAL.getDouble("MBAVAL")

        aval = mbaval - alqt
      }
      logger.debug("aval = " + aval)

      if(aval <= 0){
        lastReadItno = currentItno
        return
      }

      nrOfRecords = 10000
      popn = ""
      ExpressionFactory expressionMitpop = database.getExpressionFactory("MITPOP")
      expressionMitpop = expressionMitpop.eq("MPREMK", "SIGMA6")
      DBAction mitpopQuery = database.table("MITPOP").index("00").matching(expressionMitpop).selection("MPPOPN").build()
      DBContainer MITPOP = mitpopQuery.getContainer()
      MITPOP.set("MPCONO", currentCompany)
      MITPOP.set("MPALWT", 1)
      MITPOP.set("MPALWQ", "")
      MITPOP.set("MPITNO", currentItno)
      if (!mitpopQuery.readAll(MITPOP, 4, nrOfRecords, outDataMitpop)) {
      }
      logger.debug("popn = " + popn)

      orco = ""
      DBAction mitfacQuery = database.table("MITFAC").index("00").selection("M9CSNO","M9ORCO").build()
      DBContainer MITFAC = mitfacQuery.getContainer()
      MITFAC.set("M9CONO", currentCompany)
      MITFAC.set("M9FACI", faci)
      MITFAC.set("M9ITNO", currentItno)
      if(mitfacQuery.read(MITFAC)){
        orco = MITFAC.get("M9ORCO")
      }
      logger.debug("orco = " + orco)

      sanitary = 0
      DBAction ext032Query = database.table("EXT032").index("00").selection("EXZSAN").build()
      DBContainer EXT032 = ext032Query.getContainer()
      EXT032.set("EXCONO", currentCompany)
      EXT032.set("EXPOPN", popn)
      EXT032.set("EXSUNO", supplier)
      EXT032.set("EXORCO", orco)
      if(ext032Query.read(EXT032)){
        sanitary = EXT032.get("EXZSAN")
      }
      logger.debug("sanitary = " + sanitary)

      if(sanitary > 0){
        lastReadItno = currentItno
        return
      }

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
      containerMITAUN.set("MUITNO", currentItno)
      containerMITAUN.set("MUAUTP", 1)
      containerMITAUN.set("MUALUN", "UPA")
      if (queryMITAUN00.read(containerMITAUN)) {
        mitaunCofa = containerMITAUN.getDouble("MUCOFA")
        dmcf = containerMITAUN.getInt("MUDMCF")
        if (dmcf == 1) {
          palQuantity = aval / mitaunCofa
        } else {
          palQuantity = aval * mitaunCofa
        }
      }

      logger.debug("after palQuantity : " + palQuantity)

      expi = ''
      getLot()

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
          "EXEXPI",
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
      containerEXT055.set("EXITNO", currentItno)

      //Record exists
      if (queryEXT055.read(containerEXT055)) {
        Closure<?> updateEXT055 = { LockedResult lockedResultEXT055 ->
          lockedResultEXT055.set("EXITDS", description)
          lockedResultEXT055.set("EXZAV1", aval)
          lockedResultEXT055.set("EXZAV2", palQuantity)
          lockedResultEXT055.set("EXZQUV", 0)
          lockedResultEXT055.set("EXZPQA", 0)
          lockedResultEXT055.set("EXGRWE", weight)
          lockedResultEXT055.set("EXVOL3", volume)
          lockedResultEXT055.set("EXCOFA", mitaunCofa)
          lockedResultEXT055.set("EXEXPI", expi)
          logger.debug("mitaunCofa : " + mitaunCofa)
          lockedResultEXT055.set("EXLMDT", utility.call("DateUtil", "currentDateY8AsInt"))
          lockedResultEXT055.setInt("EXCHNO", ((Integer)lockedResultEXT055.get("EXCHNO") + 1))
          lockedResultEXT055.set("EXCHID", program.getUser())
          lockedResultEXT055.update()
        }
        queryEXT055.readLock(containerEXT055, updateEXT055)
      } else {
        containerEXT055.set("EXBJNO", jobNumber)
        containerEXT055.set("EXCONO", currentCompany)
        containerEXT055.set("EXITNO", currentItno)
        containerEXT055.set("EXITDS", description)
        containerEXT055.set("EXZAV1", aval)
        containerEXT055.set("EXZAV2", palQuantity)
        containerEXT055.set("EXZQUV", 0)
        containerEXT055.set("EXZPQA", 0)
        containerEXT055.set("EXGRWE", weight)
        containerEXT055.set("EXVOL3", volume)
        containerEXT055.set("EXCOFA", mitaunCofa)
        containerEXT055.set("EXEXPI", expi)
        containerEXT055.set("EXRGDT", utility.call("DateUtil", "currentDateY8AsInt"))
        containerEXT055.set("EXRGTM", utility.call("DateUtil", "currentTimeAsInt"))
        containerEXT055.set("EXLMDT", utility.call("DateUtil", "currentDateY8AsInt"))
        containerEXT055.set("EXCHNO", 1)
        containerEXT055.set("EXCHID", program.getUser())
        queryEXT055.insert(containerEXT055)
      }

      HashMap<String,String> params = new HashMap<>()

      double palQty = new BigDecimal(Double.toString(palQuantity)).setScale(3, RoundingMode.HALF_UP).doubleValue()

      logger.debug("before outDatas, expi = ${expi}")

      //outmap.put("ITNO", currentItno)
      params.put("ITDS", description)
      params.put("ZAV1", aval.toString())
      params.put("ZAV2", palQty.toString())
      params.put("ZQUV", 0.toString())
      params.put("ZPQA", 0.toString())
      params.put("GRWE", weight.toString())
      params.put("VOL3", volume.toString())
      params.put("COFA", mitaunCofa.toString())
      params.put("EXPI", expi)
      params.put("BJNO", jobNumber)

      outmap.put(currentItno,params)

      lastReadItno = currentItno
      lastSendCount++
      logger.debug("Added, with last Sens Count = ${lastSendCount}")
    }
  }

  /**
   * Get lot
   */
  public void getLot(){
    DBAction queryMITLOC = database.table("MITLOC").index("00").selection("MLBANO").build()
    DBContainer MITLOC = queryMITLOC.getContainer()
    MITLOC.set("MLCONO", currentCompany)
    MITLOC.set("MLWHLO", whloInput)
    MITLOC.set("MLITNO", currentItno)

    if(queryMITLOC.readAll(MITLOC, 2, 1000, MITLOCData)){
    }
  }

  /**
   * Retrieve MITLOC data
   */
  Closure<?> MITLOCData = { DBContainer MITLOC ->
    String previousBano = ''
    String currentBano = MITLOC.get("MLBANO").toString().trim()

    logger.debug("MITLOC bano = ${currentBano}")

    if(!currentBano.equals(previousBano)){
      logger.debug("currentBano: ${currentBano} not same as previousBano : ${previousBano} , previousBano will become ${currentBano}")
      previousBano = currentBano

      getExpiration(currentBano)
    }
  }

  /**
   * Get expiration
   */
  public void getExpiration(bano){
    DBAction queryMILOMA = database.table("MILOMA").index("00").selection("LMEXPI").build()
    DBContainer MILOMA = queryMILOMA.getContainer()
    MILOMA.set("LMCONO", currentCompany)
    MILOMA.set("LMITNO", currentItno)
    MILOMA.set("LMBANO", bano)

    if(queryMILOMA.read(MILOMA)){
      if(expi.equals("")){
        expi = MILOMA.get('LMEXPI').toString().trim()
      }else{

        logger.debug("before check, expi = ${expi}")

        String expiMiloma = MILOMA.get('LMEXPI').toString().trim()
        if(!expiMiloma.equals(expi) && !expiMiloma.equals("0")){
          if(expiMiloma.compareTo(expi) < 0 || expi.equals("0")){
            expi = expiMiloma

          }
          logger.debug("expi and expiMiloma were different, now expi is : ${expi}")
        }
      }
    }
  }
}
