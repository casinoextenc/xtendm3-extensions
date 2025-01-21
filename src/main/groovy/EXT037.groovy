/**
 * README
 * This extension is submitted by an API
 *
 * Name : EXT037
 * Description : batch template
 * Date         Changed By   Description
 * 20241213     YJANNIN      QUAX02 - Constraint engine
 */
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

public class EXT037 extends ExtendM3Batch {
  private final LoggerAPI logger
  private final DatabaseAPI database
  private final ProgramAPI program
  private final BatchAPI batch
  private final MICallerAPI miCaller
  private final TextFilesAPI textFiles
  private final UtilityAPI utility


  private Integer nbMaxRecord = 10000
  private int currentCompany

  //Objects to store informations
  //Used to store order infos
  private Map<String, String> datasORDER
  //Used to store order line infos
  private Map<String, String> datasLINE
  //Used to store dlix infos
  private Map<String, String> datasDLIX
  //Used to store MITTRA infos
  private Map<String, String> datasITRA
  //Used to store item infos
  private Map<String, String> datasITEM
  //Used to store list of documents for order line infos
  private Map<String, String> documents
  private Map<String, String> documentsEXT037
  private List<String> oolines
  private List<String> ext037s

  public EXT037(LoggerAPI logger, DatabaseAPI database, ProgramAPI program, BatchAPI batch, MICallerAPI miCaller, TextFilesAPI textFiles, UtilityAPI utility) {
    this.logger = logger
    this.database = database
    this.program = program
    this.batch = batch
    this.miCaller = miCaller
    this.textFiles = textFiles
    this.utility = utility
  }

  /**
   * Main method
   * retrieve job datas then call performActualJob
   */
  public void main() {
    // Get job number
    LocalDateTime timeOfCreation = LocalDateTime.now()
    String jobNumber = program.getJobNumber() + timeOfCreation.format(DateTimeFormatter.ofPattern("yyMMdd")) + timeOfCreation.format(DateTimeFormatter.ofPattern("HHmmss"))

    if (batch.getReferenceId().isPresent()) {
      Optional<String> data = getJobData(batch.getReferenceId().get())
      performActualJob(data)
    } else {
    }

    deleteEXTJOB()
  }
  /**
   * Get job datas in EXTJOB table
   * @param referenceId
   * @return
   */
  private Optional<String> getJobData(String referenceId) {
    DBAction query = database.table("EXTJOB").index("00").selection("EXDATA").build()
    DBContainer container = query.createContainer()

    container.set("EXRFID", referenceId)
    if (query.read(container)) {
      return Optional.of(container.getString("EXDATA"))
    } else {
    }
    return Optional.empty()
  }
  /**
   * @param data
   * @return
   */
  private performActualJob(Optional<String> data) {
    currentCompany = (Integer) program.getLDAZD().CONO
    //Read job inputs
    if (!data.isPresent()) {
      return
    }
    String rawData = data.get()
    String[] rowParms = rawData.split(";")


    String inORNO = rowParms[0]
    String inADLIX = rowParms.length > 1 ? rowParms[1] : ""
    String inACONN = rowParms.length > 2 ? rowParms[2] : ""
    long inDLIX = 0
    int inCONN = 0
    try {
      inDLIX = Long.parseLong(inADLIX)
    } catch (NumberFormatException e) {
      inDLIX = 0
    }
    try {
      inCONN = Integer.parseInt(inACONN)
    } catch (NumberFormatException e) {
      inCONN = 0
    }
    logger.debug("#YJ in ORNO = " + inORNO)
    logger.debug("#YJ in CONN = " + inCONN)
    logger.debug("#YJ in DLIX = " + inDLIX)

    oolines = new ArrayList<String>()
    ext037s = new ArrayList<String>()
    documents = new LinkedHashMap<String, String>()
    documentsEXT037 = new LinkedHashMap<String, String>()


    if (inCONN != 0) {

      //Get infos from MHDISH
      logger.debug("#YJ CONN = " + inCONN)
      DBAction mhdishQuery20 = database.table("MHDISH")
        .index("20")
        .selection("OQDLIX", "OQCONN", "OQWHLO", "OQPGRS").build()

      DBContainer mhdishRequest = mhdishQuery20.getContainer()
      mhdishRequest.set("OQCONO", currentCompany)
      mhdishRequest.set("OQINOU", 1)
      mhdishRequest.set("OQCONN", inCONN)
      if (!mhdishQuery20.readAll(mhdishRequest, 3, nbMaxRecord, mhdishReader)) {
      }

    }

    if (inDLIX != 0) {


      //Get infos from MHDISH
      logger.debug("#YJ DLIX = " + inDLIX)
      DBAction mhdishQuery00 = database.table("MHDISH")
        .index("00")
        .selection("OQDLIX", "OQCONN", "OQWHLO", "OQPGRS").build()

      DBContainer mhdishRequest = mhdishQuery00.getContainer()
      mhdishRequest.set("OQCONO", currentCompany)
      mhdishRequest.set("OQINOU", 1)
      mhdishRequest.set("OQDLIX", inDLIX)
      if (mhdishQuery00.read(mhdishRequest)) {
        long dlix = mhdishRequest.get("OQDLIX") as long
        Integer conn = mhdishRequest.get("OQCONN") as Integer
        String whlo = mhdishRequest.get("OQWHLO")
        String pgrs = mhdishRequest.get("OQPGRS")

        boolean found = false
        if (pgrs.trim() == "90") {
          DBAction ext037Query20 = database.table("EXT037")
            .index("20")
            .selection(
              "EXORNO",
              "EXBANO"
            ).build()

          DBContainer ext037Request = ext037Query20.getContainer()
          ext037Request.set("EXCONO", currentCompany)
          ext037Request.set("EXDLIX", dlix)

          Closure<?> ext037Reader20 = { DBContainer ext037Result ->
            String bano = ext037Result.get("EXBANO")
            if(bano.trim()!=""){
              found = true
            }

          }

          if (!ext037Query20.readAll(ext037Request, 2, 1, ext037Reader20)) {
          }

        }

        if (!found) {

            // clear EXT037
            DBAction ext037Query20 = database.table("EXT037")
              .index("20")
              .selection("EXORNO").build()

            DBContainer ext037Request = ext037Query20.getContainer()
            ext037Request.set("EXCONO", currentCompany)
            ext037Request.set("EXDLIX", dlix)

            Closure<?> ext037Updater20 = { LockedResult ext037LockedResult ->
              ext037LockedResult.delete()
            }

            if (!ext037Query20.readAllLock(ext037Request, 2, ext037Updater20)) {
            }

          logger.debug("#YJ DLIX = " + dlix)
          logger.debug("#YJ CONN = " + conn)
          logger.debug("#YJ WHLO = " + whlo)

          datasDLIX = [
            "DLIX"  : "0"
            , "CONN": "0"
            , "WHLO": ""
          ]

          datasDLIX["DLIX"] = dlix as String
          datasDLIX["CONN"] = conn as String
          datasDLIX["WHLO"] = whlo

          //Get infos from MHDISL
          DBAction mhdislQuery = database.table("MHDISL")
            .index("00")
            .selection("URRIDX", "URRORC", "URRIDN", "URRIDL", "URRIDX", "URTRQT").build()

          DBContainer mhdislRequest = mhdislQuery.getContainer()
          mhdislRequest.set("URCONO", currentCompany)
          mhdislRequest.set("URDLIX", dlix)
          if (!mhdislQuery.readAll(mhdislRequest, 2, nbMaxRecord, mhdislReader)) {
          }
        }

      }
    }

    if (inORNO.trim() != "" && inORNO != null) {
      // clear EXT037
      DBAction ext037Query20 = database.table("EXT037")
        .index("20")
        .selection("EXORNO").build()

      DBContainer ext037Request = ext037Query20.getContainer()
      ext037Request.set("EXCONO", currentCompany)
      ext037Request.set("EXDLIX", 0)
      ext037Request.set("EXORNO", inORNO)

      Closure<?> ext037Updater20 = { LockedResult ext037LockedResult ->
        ext037LockedResult.delete()
      }

      if (!ext037Query20.readAllLock(ext037Request, 3, ext037Updater20)) {
      }

      //Read OOLINE
      logger.debug("#YJ ORNO = " + inORNO)
      DBAction oolineQuery = database.table("OOLINE")
        .index("00")
        .selection("OBORNO"
          , "OBPONR"
          , "OBPOSX"
          , "OBWHLO"
          , "OBCUNO"
          , "OBITNO"
          , "OBADID"
          , "OBFACI"
          , "OBWHLO"
          , "OBORQT"
          , "OBLNAM"
          , "OBORST"
          , "OBROUT"
          , "OBCHID")
        .build()
      DBContainer oolineRequest = oolineQuery.getContainer()
      oolineRequest.set("OBCONO", currentCompany)
      oolineRequest.set("OBORNO", inORNO)
      if (!oolineQuery.readAll(oolineRequest, 2, nbMaxRecord, performOOLINEJob)) {
      }

    }

  }


  /**
   * Perform treatment per OOLINE
   */
  Closure<?> performOOLINEJob = { DBContainer oolineResult ->
    String orno = oolineResult.get("OBORNO")
    int ponr = oolineResult.get("OBPONR") as Integer
    int posx = oolineResult.get("OBPOSX") as Integer
    String whlo = oolineResult.get("OBWHLO")
    String cuno = oolineResult.get("OBCUNO")
    String itno = oolineResult.get("OBITNO")
    String faci = oolineResult.get("OBFACI")
    double orqt = oolineResult.get("OBORQT") as Double
    String orst = oolineResult.get("OBORST")
    String chid = oolineResult.get("OBCHID")
    logger.debug("#YJ ORNO = " + orno)
    logger.debug("#YJ PONR = " + ponr)
    logger.debug("#YJ POSX = " + posx)
    logger.debug("#YJ WHLO = " + whlo)
    logger.debug("#YJ CUNO = " + cuno)
    logger.debug("#YJ ITNO = " + itno)
    logger.debug("#YJ FACI = " + faci)

    datasLINE = [
      "ORNO"  : ""
      , "PONR": ""
      , "POSX": ""
      , "FACI": ""
      , "WHLO": ""
      , "ITNO": ""
      , "POPN": ""
      , "ORQT": ""
      , "ORST": ""
      , "CHID": ""
      , "ZCLI": "0"
      , "DLIX": "0"
      , "CONN": "0"
    ]

    datasLINE["ORNO"] = orno
    datasLINE["PONR"] = ponr as String
    datasLINE["POSX"] = posx as String
    datasLINE["CUNO"] = cuno
    datasLINE["ITNO"] = itno
    datasLINE["WHLO"] = whlo
    datasLINE["FACI"] = faci
    datasLINE["ORQT"] = orqt as String
    datasLINE["ORST"] = orst
    datasLINE["CHID"] = chid
    datasLINE["ZCLI"] = ""

    // Read delivery lines
    datasDLIX = [
      "DLIX"  : "0"
      , "CONN": "0"
      , "WHLO": ""
    ]

    datasITRA = [
      "BANO"  : ""
      , "CAMU": ""
      , "TRQT": "0"
    ]

    manageLine()

    oolines.add(orno + "#" + ponr + "#" + posx)
  }

  Closure<?> mhdishReader = { DBContainer mhdishResult ->
    long dlix = mhdishResult.get("OQDLIX") as long
    Integer conn = mhdishResult.get("OQCONN") as Integer
    String whlo = mhdishResult.get("OQWHLO")
    String pgrs = mhdishResult.get("OQPGRS")

    boolean found = false
    if (pgrs.trim() == "90") {
      DBAction ext037Query20 = database.table("EXT037")
        .index("20")
        .selection(
          "EXORNO",
          "EXBANO"
        ).build()

      DBContainer ext037Request = ext037Query20.getContainer()
      ext037Request.set("EXCONO", currentCompany)
      ext037Request.set("EXDLIX", dlix)

      Closure<?> ext037Reader20 = { DBContainer ext037Result ->
        String bano = ext037Result.get("EXBANO")
        if(bano.trim()!=""){
          found = true
        }

      }

      if (!ext037Query20.readAll(ext037Request, 2, 1, ext037Reader20)) {
      }

    }

    if (!found) {

      // clear EXT037
      DBAction ext037Query20 = database.table("EXT037")
        .index("20")
        .selection("EXORNO").build()

      DBContainer ext037Request = ext037Query20.getContainer()
      ext037Request.set("EXCONO", currentCompany)
      ext037Request.set("EXDLIX", dlix)

      Closure<?> ext037Updater20 = { LockedResult ext037LockedResult ->
        ext037LockedResult.delete()
      }

      if (!ext037Query20.readAllLock(ext037Request, 2, ext037Updater20)) {
      }

      logger.debug("#YJ DLIX = " + dlix)
      logger.debug("#YJ CONN = " + conn)
      logger.debug("#YJ WHLO = " + whlo)

      datasDLIX = [
        "DLIX"  : "0"
        , "CONN": "0"
        , "WHLO": ""
      ]

      datasDLIX["DLIX"] = dlix as String
      datasDLIX["CONN"] = conn as String
      datasDLIX["WHLO"] = whlo

      //Get infos from MHDISL
      DBAction mhdislQuery = database.table("MHDISL")
        .index("00")
        .selection("URRIDX", "URRORC", "URRIDN", "URRIDL", "URRIDX", "URTRQT").build()

      DBContainer mhdislRequest = mhdislQuery.getContainer()
      mhdislRequest.set("URCONO", currentCompany)
      mhdislRequest.set("URDLIX", dlix)
      if (!mhdislQuery.readAll(mhdislRequest, 2, nbMaxRecord, mhdislReader)) {
      }
    }

  }

  Closure<?> mhdislReader = { DBContainer mhdislResult ->

    String orno = mhdislResult.get("URRIDN")
    int ponr = mhdislResult.get("URRIDL") as Integer
    int posx = mhdislResult.get("URRIDX") as Integer
    long dlix = mhdislResult.get("URDLIX") as long
    double trqt = mhdislResult.get("URTRQT") as double
    logger.debug("#YJ DLIX = " + dlix)
    logger.debug("#YJ ORNO = " + orno)
    logger.debug("#YJ PONR = " + ponr)
    logger.debug("#YJ POSX = " + posx)

    datasLINE = [
      "ORNO"  : ""
      , "PONR": ""
      , "POSX": ""
      , "FACI": ""
      , "WHLO": ""
      , "ITNO": ""
      , "POPN": ""
      , "ORQT": ""
      , "ORST": ""
      , "TEPY": ""
      , "CHID": ""
      , "ZCLI": "0"
    ]


    //Get infos from OOLINE
    DBAction oolineQuery = database.table("OOLINE")
      .index("00")
      .selection("OBORNO"
        , "OBPONR"
        , "OBPOSX"
        , "OBCUNO"
        , "OBITNO"
        , "OBADID"
        , "OBFACI"
        , "OBWHLO"
        , "OBORQT"
        , "OBLNAM"
        , "OBORST"
        , "OBTEPY"
        , "OBROUT"
        , "OBCHID")
      .build()
    DBContainer oolineRequest = oolineQuery.getContainer()
    oolineRequest.set("OBCONO", currentCompany)
    oolineRequest.set("OBORNO", orno)
    oolineRequest.set("OBPONR", ponr)
    oolineRequest.set("OBPOSX", posx)
    if (oolineQuery.read(oolineRequest)) {
      String cuno = oolineRequest.get("OBCUNO")
      String itno = oolineRequest.get("OBITNO")
      String faci = oolineRequest.get("OBFACI")
      String whlo = oolineRequest.get("OBWHLO")
      double orqt = oolineRequest.get("OBORQT") as Double
      String orst = oolineRequest.get("OBORST")
      String tepy = oolineRequest.get("OBTEPY")
      String chid = oolineRequest.get("OBCHID")

      datasLINE["ORNO"] = orno
      datasLINE["PONR"] = ponr as String
      datasLINE["POSX"] = posx as String
      datasLINE["CUNO"] = cuno
      datasLINE["ITNO"] = itno
      datasLINE["FACI"] = faci
      datasLINE["WHLO"] = whlo
      datasLINE["ORQT"] = orqt as String
      datasLINE["ORST"] = orst
      datasLINE["TEPY"] = tepy
      datasLINE["CHID"] = chid
      datasLINE["ZCLI"] = ""
    } else {
      logger.debug("#YJ pas de OOLINE = ")
      return
    }

    datasLINE["ORQT"] = trqt as String

    //Get infos from MITTRA
    DBAction mittraQuery = database.table("MITTRA")
      .index("30")
      .selection("MTBANO", "MTCAMU", "MTTRQT").build()

    DBContainer mittraRequest = mittraQuery.getContainer()
    mittraRequest.set("MTCONO", currentCompany)
    mittraRequest.set("MTTTYP", 31)
    mittraRequest.set("MTRIDN", orno)
    mittraRequest.set("MTRIDL", ponr)
    mittraRequest.set("MTRIDX", posx)
    mittraRequest.set("MTRIDI", dlix)
    mittraRequest.set("MTITNO", datasLINE["ITNO"])
    if (!mittraQuery.readAll(mittraRequest, 7, nbMaxRecord, mittraReader)) {
      logger.debug("#YJ pas de MITTRA = ")
      datasITRA = [
        "BANO"  : ""
        , "CAMU": ""
        , "TRQT": "0"
      ]

      manageLine()
    }

    oolines.add(orno + "#" + ponr + "#" + posx)
  }

  Closure<?> mittraReader = { DBContainer mittraResult ->

    String bano = mittraResult.get("MTBANO")
    String camu = mittraResult.get("MTCAMU")
    double trqt = mittraResult.get("MTTRQT") as double

    trqt = trqt * (-1)

    logger.debug("#YJ BANO = " + bano)
    logger.debug("#YJ CAMU = " + camu)
    logger.debug("#YJ TRQT = " + trqt)

    datasITRA = [
      "BANO"  : ""
      , "CAMU": ""
      , "TRQT": "0"
    ]

    datasITRA["BANO"] = bano
    datasITRA["CAMU"] = camu
    datasITRA["TRQT"] = trqt as String

    manageLine()

  }

  // Manage Line
  private void manageLine() {
    logger.debug("#YJ Manage line ")

    documents = new LinkedHashMap<String, String>()
    documentsEXT037 = new LinkedHashMap<String, String>()

    getLineCriterias()

    getLineEXT030()

    getLineEXT037()

    mngEXT037()
  }

  // Get line criterias
  private void getLineCriterias() {
    getOrderDatas()
    getItemDatas()
  }

  /**
   * Get Order Informations and load them into orderDatas map object
   */
  public void getOrderDatas() {

    datasORDER = [
      "ORNO"  : ""
      , "CUNO": ""
      , "CSCD": ""
      , "UCA4": ""
    ]

    //Get country code from OOHEAD
    String orno = datasLINE["ORNO"] as String
    String torno = datasORDER == null ? "" : datasORDER["ORNO"]

    if (orno == torno) {
      return
    }

    DBAction ooheadQuery = database.table("OOHEAD")
      .index("00")
      .selection("OAADID"
        , "OACUNO"
        , "OAUCA4"
      )
      .build()
    DBContainer ooheadRequest = ooheadQuery.getContainer()
    ooheadRequest.set("OACONO", currentCompany)
    ooheadRequest.set("OAORNO", orno)
    if (ooheadQuery.read(ooheadRequest)) {
      datasORDER["UCA4"] = ooheadRequest["OAUCA4"] as String
      datasORDER["CUNO"] = ooheadRequest["OACUNO"] as String
      if (ooheadRequest.getString("OAADID") != "") {
        DBAction ocusadQuery = database.table("OCUSAD")
          .index("00")
          .selection("OPCSCD")
          .build()
        DBContainer ocusadRequest = ocusadQuery.getContainer()
        ocusadRequest.set("OPCONO", currentCompany)
        ocusadRequest.set("OPCUNO", datasORDER["CUNO"])
        ocusadRequest.set("OPADRT", 1)
        ocusadRequest.set("OPADID", ooheadRequest.get("OAADID"))
        if (ocusadQuery.read(ocusadRequest)) {
          datasORDER["CSCD"] = ocusadRequest.getString("OPCSCD")
        }
      } else {
        DBAction ocusmaQuery = database.table("OCUSMA")
          .index("00")
          .selection("OKCSCD").build()
        DBContainer ocusmaRequest = ocusmaQuery.getContainer()
        ocusmaRequest.set("OKCONO", currentCompany)
        ocusmaRequest.set("OKCUNO", datasORDER["CUNO"])
        if (ocusmaQuery.read(ocusmaRequest)) {
          datasORDER["CSCD"] = ocusmaRequest.getString("OKCSCD")
        }
      }
    }
  }


  /**
   * Get Item Informations and load them into itemDatas map object
   */
  public void getItemDatas() {
    //GetItem informations
    String itno = datasLINE["ITNO"] as String
    String faci = datasLINE["FACI"] as String
    String bano = datasITRA["BANO"] as String
    double orqt

    datasITEM = [
      "HAZI"  : ""
      , "HIE5": ""
      , "CFI4": ""
      , "SUNO": ""
      , "PROD": ""
      , "SIG6": ""
      , "GRWE": ""
      , "NEWE": ""
      , "ZTGR": ""
      , "ZTNW": ""
      , "CSNO": ""
      , "ORCO": ""
      , "ZALI": ""
      , "ZALC": ""
      , "ZSAN": ""
      , "ZCAP": ""
      , "ZCA1": ""
      , "ZCA2": ""
      , "ZCA3": ""
      , "ZCA4": ""
      , "ZCA5": ""
      , "ZCA6": ""
      , "ZCA7": ""
      , "ZCA8": ""
      , "ZORI": ""
      , "ZPHY": ""
      , "ZAGR": ""
      , "ZNAG": ""
      , "ZQCO": ""
      , "ZSTY": ""
    ]

    if (bano.trim() != "") {
      orqt = datasITRA["TRQT"] as Double
    } else {
      orqt = datasLINE["ORQT"] as Double
    }

    String hazi = ""
    String hie5 = ""
    String cfi4 = ""
    String suno = ""
    String prod = ""
    String sig6 = ""
    double grwe = 0
    double newe = 0
    double ztgr = 0
    double ztnw = 0
    String csno = ""
    String orco = ""
    int zali = 0
    int zalc = 0
    int zsan = 0
    String zcap = ""
    String zca1 = ""
    String zca2 = ""
    String zca3 = ""
    String zca4 = ""
    String zca5 = ""
    String zca6 = ""
    String zca7 = ""
    String zca8 = ""
    int zori = 0
    int zphy = 0
    int zagr = 0
    String znag = ""

    //Get infos from MITMAS
    DBAction mitmasQuery = database.table("MITMAS").index("00").selection("MMHAZI", "MMHIE5", "MMCFI4", "MMSUNO", "MMPROD", "MMITGR", "MMGRWE", "MMNEWE").build()
    DBContainer MITMAS = mitmasQuery.getContainer()
    MITMAS.set("MMCONO", currentCompany)
    MITMAS.set("MMITNO", itno)
    if (mitmasQuery.read(MITMAS)) {
      hazi = MITMAS.get("MMHAZI")
      hie5 = MITMAS.get("MMHIE5")
      cfi4 = MITMAS.get("MMCFI4")
      suno = MITMAS.get("MMSUNO")
      prod = MITMAS.get("MMPROD")
      grwe = MITMAS.get("MMGRWE") as Double
      newe = MITMAS.get("MMNEWE") as Double
      if (grwe != 0 && orqt != 0) {
        ztgr = grwe * orqt
      }
      if (newe != 0 && orqt != 0) {
        ztnw = newe * orqt
      }
    }
    //Get infos from MITPOP
    ExpressionFactory mitpopExpression = database.getExpressionFactory("MITPOP")
    mitpopExpression = mitpopExpression.eq("MPREMK", "SIGMA6")
    DBAction mitpopQuery = database.table("MITPOP")
      .index("00")
      .matching(mitpopExpression)
      .selection("MPPOPN").build()

    DBContainer mitpopRequest = mitpopQuery.getContainer()
    mitpopRequest.set("MPCONO", currentCompany)
    mitpopRequest.set("MPALWT", 1)
    mitpopRequest.set("MPALWQ", "")
    mitpopRequest.set("MPITNO", itno)

    Closure<?> mitpopReader = { DBContainer mitpopResult ->
      sig6 = mitpopResult.getString("MPPOPN").trim()
    }

    if (!mitpopQuery.readAll(mitpopRequest, 4, mitpopReader)) {
    }

    //Get infos from MITFAC
    DBAction mitfacQuery = database.table("MITFAC")
      .index("00")
      .selection("M9CSNO"
        , "M9ORCO")
      .build()
    DBContainer MITFAC = mitfacQuery.getContainer()
    MITFAC.set("M9CONO", currentCompany)
    MITFAC.set("M9FACI", faci)
    MITFAC.set("M9ITNO", itno)
    if (mitfacQuery.read(MITFAC)) {
      csno = MITFAC.get("M9CSNO")
      orco = MITFAC.getString("M9ORCO").trim()
    }

    //Get infos from EXT032
    DBAction ext032Query = database
      .table("EXT032")
      .index("00")
      .selection("EXZALC"
        , "EXZSAN"
        , "EXZCA1"
        , "EXZCA1"
        , "EXZCA2"
        , "EXZCA3"
        , "EXZCA4"
        , "EXZCA5"
        , "EXZCA6"
        , "EXZCA7"
        , "EXZCA8"
        , "EXZORI"
        , "EXZPHY"
        , "EXZAGR"
        , "EXZALI"
      ).build()

    DBContainer ext032Request = ext032Query.getContainer()
    ext032Request.set("EXCONO", currentCompany)
    ext032Request.set("EXPOPN", sig6)
    ext032Request.set("EXSUNO", suno)
    ext032Request.set("EXORCO", orco)
    if (ext032Query.read(ext032Request)) {
      zalc = ext032Request.get("EXZALC") as Integer
      zsan = ext032Request.get("EXZSAN") as Integer
      zcap = ext032Request.get("EXZCA1")
      zca1 = ext032Request.get("EXZCA1")
      zca2 = ext032Request.get("EXZCA2")
      zca3 = ext032Request.get("EXZCA3")
      zca4 = ext032Request.get("EXZCA4")
      zca5 = ext032Request.get("EXZCA5")
      zca6 = ext032Request.get("EXZCA6")
      zca7 = ext032Request.get("EXZCA7")
      zca8 = ext032Request.get("EXZCA8")
      zori = ext032Request.get("EXZORI") as Integer
      zphy = ext032Request.get("EXZPHY") as Integer
      zagr = ext032Request.get("EXZAGR") as Integer
      zali = ext032Request.get("EXZALI") as Integer
    }

    DBAction cugex1CidmasQuery = database.table("CUGEX1").index("00").selection("F1A030").build()
    DBContainer cugex1Cidmas = cugex1CidmasQuery.getContainer()
    cugex1Cidmas.set("F1CONO", currentCompany)
    cugex1Cidmas.set("F1FILE", "CIDMAS")
    if (prod.trim() != "") {
      cugex1Cidmas.set("F1PK01", prod)
    } else {
      cugex1Cidmas.set("F1PK01", suno)
    }
    cugex1Cidmas.set("F1PK02", "")
    cugex1Cidmas.set("F1PK03", "")
    cugex1Cidmas.set("F1PK04", "")
    cugex1Cidmas.set("F1PK05", "")
    cugex1Cidmas.set("F1PK06", "")
    cugex1Cidmas.set("F1PK07", "")
    cugex1Cidmas.set("F1PK08", "")
    if (cugex1CidmasQuery.read(cugex1Cidmas)) {
      znag = cugex1Cidmas.get("F1A030")
    }
    double cofa = 0
    double zqco = 0
    DBAction mitaunQuery = database.table("MITAUN").index("00").selection("MUCOFA").build()
    DBContainer MITAUN = mitaunQuery.getContainer()
    MITAUN.set("MUCONO", currentCompany)
    MITAUN.set("MUITNO", itno)
    MITAUN.set("MUAUTP", 1)
    MITAUN.set("MUALUN", "COL")
    if (mitaunQuery.read(MITAUN)) {
      cofa = MITAUN.get("MUCOFA") as Double
      if (orqt != 0 && cofa != 0)
        zqco = orqt / cofa
    }

    //Store datas
    datasITEM["HAZI"] = hazi
    datasITEM["HIE5"] = hie5
    datasITEM["CFI4"] = cfi4
    datasITEM["SUNO"] = suno
    datasITEM["PROD"] = prod
    datasITEM["SIG6"] = sig6
    datasITEM["GRWE"] = grwe as String
    datasITEM["NEWE"] = newe as String
    datasITEM["ZTGR"] = ztgr as String
    datasITEM["ZTNW"] = ztnw as String
    datasITEM["ZQCO"] = zqco as String
    datasITEM["CSNO"] = csno
    datasITEM["ORCO"] = orco
    datasITEM["ZALI"] = zali as String
    datasITEM["ZALC"] = zalc as String
    datasITEM["ZSAN"] = zsan as String
    datasITEM["ZCAP"] = zcap
    datasITEM["ZCA1"] = zca1
    datasITEM["ZCA2"] = zca2
    datasITEM["ZCA3"] = zca3
    datasITEM["ZCA4"] = zca4
    datasITEM["ZCA5"] = zca5
    datasITEM["ZCA6"] = zca6
    datasITEM["ZCA7"] = zca7
    datasITEM["ZCA8"] = zca8
    datasITEM["ZORI"] = zori as String
    datasITEM["ZPHY"] = zphy as String
    datasITEM["ZAGR"] = zagr as String
    datasITEM["ZNAG"] = znag
  }

  /**
   * Query on EXT030 (Constraint matrix)
   */
  public void getLineEXT030() {
    String orno = datasLINE["ORNO"]
    int ponr = datasLINE["PONR"] as Integer
    int posx = datasLINE["POSX"] as Integer

    String cuno = datasORDER["CUNO"]
    String cscd = datasORDER["CSCD"]

    String hazi = datasITEM["HAZI"]
    String hie5 = datasITEM["HIE5"]
    String cfi4 = datasITEM["CFI4"]
    String suno = datasITEM["SUNO"]
    String prod = datasITEM["PROD"]
    String sig6 = datasITEM["SIG6"]
    String grwe = datasITEM["GRWE"]
    String newe = datasITEM["NEWE"]
    String ztgr = datasITEM["ZTGR"]
    String ztnw = datasITEM["ZTNW"]
    String csno = datasITEM["CSNO"]
    String orco = datasITEM["ORCO"]
    String zali = datasITEM["ZALI"]
    String zalc = datasITEM["ZALC"]
    String zsan = datasITEM["ZSAN"]
    String zcap = datasITEM["ZCAP"]
    String zca1 = datasITEM["ZCA1"]
    String zca2 = datasITEM["ZCA2"]
    String zca3 = datasITEM["ZCA3"]
    String zca4 = datasITEM["ZCA4"]
    String zca5 = datasITEM["ZCA5"]
    String zca6 = datasITEM["ZCA6"]
    String zca7 = datasITEM["ZCA7"]
    String zca8 = datasITEM["ZCA8"]
    String zori = datasITEM["ZORI"]
    String zphy = datasITEM["ZPHY"]
    String zagr = datasITEM["ZAGR"]
    String znag = datasITEM["ZNAG"]

    ExpressionFactory ext030Expression = database.getExpressionFactory("EXT030")
    ext030Expression = (ext030Expression.eq("EXCUNO", cuno)).or(ext030Expression.eq("EXCUNO", ""))

    ext030Expression = ext030Expression.and((ext030Expression.eq("EXCSCD", cscd)).or(ext030Expression.eq("EXCSCD", "")))

    ext030Expression = ext030Expression.and((ext030Expression.eq("EXHAZI", hazi as String)).or(ext030Expression.eq("EXHAZI", "2")))

    if (hie5 != "") {
      ext030Expression = ext030Expression.and((ext030Expression.eq("EXHIE0", hie5)).or(ext030Expression.eq("EXHIE0", hie5.substring(0, 2) + "*")).or(ext030Expression.eq("EXHIE0", hie5.substring(0, 4) + "*")).or(ext030Expression.eq("EXHIE0", hie5.substring(0, 7) + "*")).or(ext030Expression.eq("EXHIE0", hie5.substring(0, 9) + "*")).or(ext030Expression.eq("EXHIE0", hie5.substring(0, 11) + "*")).or(ext030Expression.eq("EXHIE0", "")))
    } else {
      ext030Expression = ext030Expression.and((ext030Expression.eq("EXHIE0", hie5)).or(ext030Expression.eq("EXHIE0", "")))
    }

    ext030Expression = ext030Expression.and((ext030Expression.eq("EXCFI4", cfi4)).or(ext030Expression.eq("EXCFI4", "")))


    ext030Expression = ext030Expression.and((ext030Expression.eq("EXPOPN", sig6)).or(ext030Expression.eq("EXPOPN", "")))

    if (csno != "") {
      ext030Expression = ext030Expression.and((ext030Expression.eq("EXCSNO", csno)).or(ext030Expression.eq("EXCSNO", csno.substring(0, 1) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 2) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 3) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 4) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 5) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 6) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 7) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 8) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 9) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 10) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 11) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 12) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 13) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 14) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 15) + "*")).or(ext030Expression.eq("EXCSNO", csno.substring(0, 16) + "*")).or(ext030Expression.eq("EXCSNO", "")))
    } else {
      ext030Expression = ext030Expression.and((ext030Expression.eq("EXCSNO", csno)).or(ext030Expression.eq("EXCSNO", "")))
    }

    ext030Expression = ext030Expression.and((ext030Expression.eq("EXORCO", orco)).or(ext030Expression.eq("EXORCO", "")))

    ext030Expression = ext030Expression.and((ext030Expression.eq("EXZALC", zalc as String)).or(ext030Expression.eq("EXZALC", "2")))

    ext030Expression = ext030Expression.and((ext030Expression.eq("EXZSAN", zsan as String)).or(ext030Expression.eq("EXZSAN", "2")))

    ext030Expression = ext030Expression.and((ext030Expression.eq("EXZCAP", zca1)).or(ext030Expression.eq("EXZCAP", "")))

    ext030Expression = ext030Expression.and((ext030Expression.eq("EXZCAS", zca1)).or(ext030Expression.eq("EXZCAS", zca2)).or(ext030Expression.eq("EXZCAS", zca3)).or(ext030Expression.eq("EXZCAS", zca4)).or(ext030Expression.eq("EXZCAS", zca5)).or(ext030Expression.eq("EXZCAS", zca6)).or(ext030Expression.eq("EXZCAS", zca7)).or(ext030Expression.eq("EXZCAS", zca8)).or(ext030Expression.eq("EXZCAS", "")))

    if (znag != "") {
      ext030Expression = ext030Expression.and((ext030Expression.eq("EXZNAG", znag)).or(ext030Expression.eq("EXZNAG", znag.substring(0, 4) + "*")).or(ext030Expression.eq("EXZNAG", "")))
    } else {
      ext030Expression = ext030Expression.and((ext030Expression.eq("EXZNAG", znag)).or(ext030Expression.eq("EXZNAG", "")))
    }

    ext030Expression = ext030Expression.and((ext030Expression.eq("EXZALI", zali as String)).or(ext030Expression.eq("EXZALI", "2")))

    ext030Expression = ext030Expression.and((ext030Expression.eq("EXZORI", zori as String)).or(ext030Expression.eq("EXZORI", "2")))

    ext030Expression = ext030Expression.and((ext030Expression.eq("EXZPHY", zphy as String)).or(ext030Expression.eq("EXZPHY", "2")))

    if (orco == "FR") {
      ext030Expression = ext030Expression.and((ext030Expression.eq("EXZOHF", "0")).or(ext030Expression.eq("EXZOHF", "2")))
    } else {
      ext030Expression = ext030Expression.and((ext030Expression.eq("EXZOHF", "1")).or(ext030Expression.eq("EXZOHF", "2")))
    }

    DBAction ext030Query = database.table("EXT030").index("20").matching(ext030Expression).selection("EXZCID", "EXZCOD").build()
    DBContainer EXT030 = ext030Query.getContainer()
    EXT030.set("EXCONO", currentCompany)
    EXT030.set("EXSTAT", "20")

    if (!ext030Query.readAll(EXT030, 2, ext030Reader)) {
      logger.debug("#YJ EXT030 non trouvé ")
    }
  }


  /**
   * Read EXT030 constraint
   */
  Closure<?> ext030Reader = { DBContainer ext030Result ->
    logger.debug("#YJ EXT030 ")
    boolean constraintFound = true
    String zcid = ext030Result.get("EXZCID")
    String zcod = ext030Result.get("EXZCOD")
    String zcty = ""
    String zsty = ""
    logger.debug("#YJ zcid = " + zcid)
    logger.debug("#YJ zcod = " + zcod)

    DBAction ext034Query = database
      .table("EXT034")
      .index("00")
      .selection("EXZCTY"
        , "EXZSTY")
      .build()
    DBContainer ext034Request = ext034Query.getContainer()
    ext034Request.set("EXCONO", currentCompany)
    ext034Request.set("EXZCOD", zcod)
    if (ext034Query.read(ext034Request)) {
      logger.debug("#YJ EXT034 ")
      zcty = ext034Request.get("EXZCTY")
      zsty = ext034Request.get("EXZSTY") as String
      logger.debug("#YJ zcty = " + zcty)
      logger.debug("#YJ zsty = " + zsty)
    }
    queryEXT035(zcid, zcty, zcod, zsty)

  }

  /**
   * Query on EXT035 (Documentation) and insert found documents in EXT037
   * @return
   */
  public void queryEXT035(String zcid, String zcty, String zcod, zsty) {
    //Document search: write in EXT037 for each document found
    DBAction ext035Query = database.table("EXT035").index("00").selection("EXZCOD").build()
    DBContainer ext035Request = ext035Query.getContainer()
    ext035Request.set("EXCONO", currentCompany)
    ext035Request.set("EXZCOD", zcod)
    ext035Request.set("EXCSCD", datasORDER["CSCD"])
    ext035Request.set("EXCUNO", datasORDER["CUNO"])


    String doid = ""
    String ads1 = ""

    Closure<?> ext035Reader = { DBContainer ext035Result ->
      logger.debug("#YJ EXT035 ")
      doid = ext035Result.get("EXDOID") as String
      DBAction mpddocQuery = database.table("MPDDOC").index("00").selection("DOADS1").build()
      DBContainer MPDDOC = mpddocQuery.getContainer()
      MPDDOC.set("DOCONO", currentCompany)
      MPDDOC.set("DODOID", doid)
      if (mpddocQuery.read(MPDDOC)) {
        ads1 = MPDDOC.get("DOADS1")
      }
      logger.debug("#YJ DOID " + doid)
      logger.debug("#YJ ADS1 " + ads1)
      String key = zcid.trim() + "#" + zcty.trim() + "#" + zcod.trim() + "#" + doid.trim() + "#" + ads1.trim()
      String value = zsty
      logger.debug("#YJ key " + key)
      logger.debug("#YJ value " + value)
      documents.put(key, value)
    }

    if (!ext035Query.readAll(ext035Request, 4, ext035Reader)) {
      ext035Request.set("EXCSCD", datasORDER["CSCD"])
      ext035Request.set("EXCUNO", "")
      if (!ext035Query.readAll(ext035Request, 4, ext035Reader)) {
        ext035Request.set("EXCSCD", "")
        ext035Request.set("EXCUNO", "")
        if (!ext035Query.readAll(ext035Request, 4, ext035Reader)) {
          String key = zcid.trim() + "#" + zcty.trim() + "#" + zcod.trim() + "#" + doid.trim() + "#" + ads1.trim()
          String value = zsty
          logger.debug("#YJ key " + key)
          logger.debug("#YJ value " + value)
          documents.put(key, value)
        }
      }
    }

  }

  /**
   *
   */
  public void getLineEXT037() {
    logger.debug("#YJ get line EXT037 ")
    String orno = datasLINE["ORNO"]
    int ponr = datasLINE["PONR"] as Integer
    int posx = datasLINE["POSX"] as Integer
    String whlo = datasLINE["WHLO"]
    long dlix = datasDLIX["DLIX"] as long
    String bano = datasITRA["BANO"]
    String camu = datasITRA["CAMU"]
    int constraintLine = 0

    logger.debug("#YJ ORNO " + orno)
    logger.debug("#YJ PONR " + ponr)
    logger.debug("#YJ POSX " + posx)
    logger.debug("#YJ WHLO " + orno)
    logger.debug("#YJ DLIX " + dlix)
    logger.debug("#YJ BANO " + bano)
    logger.debug("#YJ CAMU " + camu)

    // Retrieve next constraint line available
    DBAction ext037Query = database.table("EXT037")
      .index("00")
      .selection(
        "EXORNO"
        , "EXPONR"
        , "EXPOSX"
        , "EXDLIX"
        , "EXWHLO"
        , "EXBANO"
        , "EXCAMU"
        , "EXZCLI"
        , "EXZCID"
        , "EXZCTY"
        , "EXZCOD"
        , "EXDOID"
        , "EXADS1"
        , "EXSTAT"
      )
      .build()
    DBContainer ext037Request = ext037Query.getContainer()
    ext037Request.set("EXCONO", currentCompany)
    ext037Request.set("EXORNO", orno)
    ext037Request.set("EXPONR", ponr)
    ext037Request.set("EXPOSX", posx)
    ext037Request.set("EXDLIX", dlix)
    ext037Request.set("EXWHLO", whlo)
    ext037Request.set("EXBANO", bano)
    ext037Request.set("EXCAMU", camu)

    Closure<?> ext037Reader = { DBContainer ext037Result ->
      constraintLine = ext037Result.get("EXZCLI") as Integer
      String zcid = ext037Result.get("EXZCID") as String
      String zcty = ext037Result.get("EXZCTY") as String
      String zcod = ext037Result.get("EXZCOD") as String
      String doid = ext037Result.get("EXDOID") as String
      String ads1 = ext037Result.get("EXADS1") as String
      String key = zcid.trim() + "#" + zcty.trim() + "#" + zcod.trim() + "#" + doid.trim() + "#" + ads1.trim()
      String value = ext037Result.get("EXZCLI") as String
      value += "#" + ext037Result.get("EXSTAT") as String
      value += "#" + ext037Result.get("EXZCID") as String
      documentsEXT037.put(key, value)
    }
    if (!ext037Query.readAll(ext037Request, 8, nbMaxRecord, ext037Reader)) {
    }
    datasLINE["ZCLI"] = constraintLine as String
    logger.debug("#YJ ZCLI " + constraintLine)
  }

  /**
   *
   */
  public void mngEXT037() {
    logger.debug("#YJ manage line EXT037 ")
    String orno = datasLINE["ORNO"]
    int ponr = datasLINE["PONR"] as Integer
    int posx = datasLINE["POSX"] as Integer
    String cuno = datasLINE["CUNO"] as String
    String itno = datasLINE["ITNO"]
    String faci = datasLINE["FACI"]
    String whlo = datasLINE["WHLO"]
    double orqt = datasLINE["ORQT"] as Double
    String orst = datasLINE["ORST"]
    String chid = datasLINE["CHID"]
    int zcli = datasLINE["ZCLI"] as Integer

    logger.debug("#YJ ORN = " + orno)
    logger.debug("#YJ PONR " + ponr)
    logger.debug("#YJ POSX " + posx)
    logger.debug("#YJ WHLO " + whlo)

    long dlix = datasDLIX["DLIX"] as Long
    long conn = datasDLIX["CONN"] as Integer

    logger.debug("#YJ DLIX " + dlix)

    String bano = datasITRA["BANO"]
    String camu = datasITRA["CAMU"]

    logger.debug("#YJ BANO " + bano)
    logger.debug("#YJ CAMU " + camu)

    if (bano.trim() != "") {
      double trqt = datasITRA["TRQT"] as Double
      logger.debug("#YJ trqt " + trqt)
      orqt = trqt
    }

    logger.debug("#YJ ORQT " + orqt)

    String cscd = datasORDER["CSCD"]
    String uca4 = datasORDER["UCA4"]

    String hazi = datasITEM["HAZI"]
    String hie5 = datasITEM["HIE5"]
    String cfi4 = datasITEM["CFI4"]
    String suno = datasITEM["SUNO"]
    String prod = datasITEM["PROD"]
    String sig6 = datasITEM["SIG6"]
    double grwe = datasITEM["GRWE"] as Double
    double newe = datasITEM["NEWE"] as Double
    double ztgr = datasITEM["ZTGR"] as Double
    logger.debug("#YJ ztgr " + ztgr)
    double ztnw = datasITEM["ZTNW"] as Double
    logger.debug("#YJ ztnw " + ztnw)
    String csno = datasITEM["CSNO"]
    String orco = datasITEM["ORCO"]
    String zali = datasITEM["ZALI"]
    String zalc = datasITEM["ZALC"]
    String zsan = datasITEM["ZSAN"]
    String zcap = datasITEM["ZCAP"]
    String zca1 = datasITEM["ZCA1"]
    String zca2 = datasITEM["ZCA2"]
    String zca3 = datasITEM["ZCA3"]
    String zca4 = datasITEM["ZCA4"]
    String zca5 = datasITEM["ZCA5"]
    String zca6 = datasITEM["ZCA6"]
    String zca7 = datasITEM["ZCA7"]
    String zca8 = datasITEM["ZCA8"]
    String zori = datasITEM["ZORI"]
    String zphy = datasITEM["ZPHY"]
    String zagr = datasITEM["ZAGR"]
    logger.debug("#YJ zagr " + zagr)
    String znag = datasITEM["ZNAG"]
    double zqco = datasITEM["ZQCO"] as Double
    logger.debug("#YJ zqco " + zqco)

    for (key in documentsEXT037.keySet()) {
      logger.debug("#YJ documents EXT037 ")
      String value = documentsEXT037.get(key)
      String[] vt = value.split("#")
      int tcli = vt[0] as Integer
      String stat = vt[1]
      long zcid = vt[2] as long
      logger.debug("#YJ TCLI " + tcli)
      logger.debug("#YJ ZCID " + zcid)
      if (!documents.containsKey(key) && stat != "90") {
        updateext037(orno, ponr, posx, dlix, whlo, bano, camu, zcid, tcli, "90", null)
      } else if (documents.containsKey(key)) {
        String zsty = documents.get(key)
        updateext037(orno, ponr, posx, dlix, whlo, bano, camu, zcid, tcli, "20", zsty)
      }
    }
    for (key in documents.keySet()) {
      logger.debug("#YJ documents ")
      if (!documentsEXT037.containsKey(key)) {
        zcli++
        String[] ks = key.split("#")
        double zcid = ks[0] as Double
        String zcty = ks[1]
        String zcod = ks[2]
        String doid = ks.length > 3 ? ks[3] : ""
        String ads1 = ks.length > 4 ? ks[4] : ""
        String zsty = documents.get(key)

        logger.debug("#YJ ZCLI " + zcli)
        logger.debug("#YJ ZCID " + zcid)

        DBAction ext037Query = database.table("EXT037").index("00").build()

        DBContainer ext037Request = ext037Query.getContainer()
        ext037Request.set("EXCONO", currentCompany)
        ext037Request.set("EXORNO", orno)
        ext037Request.set("EXPONR", ponr)
        ext037Request.set("EXPOSX", posx)
        ext037Request.set("EXDLIX", dlix)
        ext037Request.set("EXWHLO", whlo)
        ext037Request.set("EXBANO", bano)
        ext037Request.set("EXCAMU", camu)
        ext037Request.set("EXZCID", zcid)
        ext037Request.set("EXZCLI", zcli)
        if (!ext037Query.read(ext037Request)) {
          ext037Request.set("EXORST", orst)
          ext037Request.set("EXSTAT", "20")
          ext037Request.set("EXCONN", conn)
          ext037Request.set("EXDLIX", dlix)
          ext037Request.set("EXUCA4", uca4)
          ext037Request.set("EXCUNO", cuno)
          ext037Request.set("EXITNO", itno)
          ext037Request.set("EXZAGR", zagr)
          ext037Request.set("EXZNAG", znag)
          ext037Request.set("EXORQT", orqt)
          ext037Request.set("EXZQCO", zqco)
          ext037Request.set("EXZTGR", ztgr)
          ext037Request.set("EXZTNW", ztnw)
          ext037Request.set("EXZCID", zcid)
          ext037Request.set("EXZCOD", zcod)
          ext037Request.set("EXZCTY", zcty)
          ext037Request.set("EXDOID", doid)
          ext037Request.set("EXADS1", ads1)
          //ext037Request.set("EXZSTY", zsty)
          LocalDateTime timeOfCreation = LocalDateTime.now()
          ext037Request.setInt("EXRGDT", timeOfCreation.format(DateTimeFormatter.ofPattern("yyyyMMdd")) as Integer)
          ext037Request.setInt("EXRGTM", timeOfCreation.format(DateTimeFormatter.ofPattern("HHmmss")) as Integer)
          ext037Request.setInt("EXLMDT", timeOfCreation.format(DateTimeFormatter.ofPattern("yyyyMMdd")) as Integer)
          ext037Request.setInt("EXCHNO", 1)
          ext037Request.set("EXCHID", chid)
          ext037Query.insert(ext037Request)
        }

      }
    }
    logger.debug("#YJ fin manage line EXT037 ")
  }

  public void updateext037(String orno, int ponr, int posx, long dlix, String whlo, String bano, String camu, long zcid, int zcli, String stat, String zsty) {
    String cuno = datasLINE["CUNO"] as String
    String itno = datasLINE["ITNO"]
    String faci = datasLINE["FACI"]
    double orqt = datasLINE["ORQT"] as Double
    String orst = datasLINE["ORST"]
    String chid = datasLINE["CHID"]

    long conn = datasDLIX["CONN"] as Long

    if (bano.trim() != "") {
      double trqt = datasITRA["TRQT"] as Double
      orqt = trqt
    }

    String cscd = datasORDER["CSCD"]
    String uca4 = datasORDER["UCA4"]

    String hazi = datasITEM["HAZI"]
    String hie5 = datasITEM["HIE5"]
    String cfi4 = datasITEM["CFI4"]
    String suno = datasITEM["SUNO"]
    String prod = datasITEM["PROD"]
    String sig6 = datasITEM["SIG6"]
    double grwe = datasITEM["GRWE"] as Double
    double newe = datasITEM["NEWE"] as Double
    double ztgr = datasITEM["ZTGR"] as Double
    double ztnw = datasITEM["ZTNW"] as Double
    String csno = datasITEM["CSNO"]
    String orco = datasITEM["ORCO"]
    String zali = datasITEM["ZALI"]
    String zalc = datasITEM["ZALC"]
    String zsan = datasITEM["ZSAN"]
    String zcap = datasITEM["ZCAP"]
    String zca1 = datasITEM["ZCA1"]
    String zca2 = datasITEM["ZCA2"]
    String zca3 = datasITEM["ZCA3"]
    String zca4 = datasITEM["ZCA4"]
    String zca5 = datasITEM["ZCA5"]
    String zca6 = datasITEM["ZCA6"]
    String zca7 = datasITEM["ZCA7"]
    String zca8 = datasITEM["ZCA8"]
    String zori = datasITEM["ZORI"]
    String zphy = datasITEM["ZPHY"]
    String zagr = datasITEM["ZAGR"]
    String znag = datasITEM["ZNAG"]
    double zqco = datasITEM["ZQCO"] as Double

    DBAction query = database.table("ext037").index("00").build()
    DBContainer ext037Request = query.getContainer()
    ext037Request.set("EXCONO", currentCompany)
    ext037Request.set("EXORNO", orno)
    ext037Request.set("EXPONR", ponr)
    ext037Request.set("EXPOSX", posx)
    ext037Request.set("EXDLIX", dlix)
    ext037Request.set("EXWHLO", whlo)
    ext037Request.set("EXBANO", bano)
    ext037Request.set("EXCAMU", camu)
    ext037Request.set("EXZCID", zcid)
    ext037Request.set("EXZCLI", zcli)

    Closure<?> updateCallBack = { LockedResult lockedResult ->
      LocalDateTime timeOfCreation = LocalDateTime.now()
      int changeNumber = lockedResult.get("EXCHNO") as Integer

      lockedResult.set("EXORST", orst)
      lockedResult.set("EXCONN", conn)
      lockedResult.set("EXDLIX", dlix)
      lockedResult.set("EXUCA4", uca4)
      lockedResult.set("EXCUNO", cuno)
      lockedResult.set("EXITNO", itno)
      lockedResult.set("EXZAGR", zagr)
      lockedResult.set("EXZNAG", znag)
      lockedResult.set("EXORQT", orqt)
      lockedResult.set("EXZQCO", zqco)
      lockedResult.set("EXZTGR", ztgr)
      lockedResult.set("EXZTNW", ztnw)
      if (zsty != null)
      //lockedResult.set("EXZSTY", zsty)
        if (orst == "99") {
          lockedResult.set("EXSTAT", "90")
        } else {
          lockedResult.set("EXSTAT", stat)
        }
      lockedResult.setInt("EXLMDT", timeOfCreation.format(DateTimeFormatter.ofPattern("yyyyMMdd")) as Integer)
      lockedResult.setInt("EXCHNO", changeNumber + 1)
      lockedResult.set("EXCHID", program.getUser())
      lockedResult.update()
    }

    if (!query.readLock(ext037Request, updateCallBack)) {
    }
  }

  /**
   * Delete job informations in EXTJOB
   * @param data
   * @return
   */
  public void deleteEXTJOB() {
    LocalDateTime timeOfCreation = LocalDateTime.now()
    DBAction query = database.table("EXTJOB").index("00").build()
    DBContainer EXTJOB = query.getContainer()
    EXTJOB.set("EXRFID", batch.getReferenceId().get())

    Closure<?> extjobUpdateCallBack = { LockedResult lockedResult ->
      lockedResult.delete()
    }

    if (!query.readAllLock(EXTJOB, 1, extjobUpdateCallBack)) {
    }
  }
}
