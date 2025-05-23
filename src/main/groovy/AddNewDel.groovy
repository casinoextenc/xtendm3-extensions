/**
 * README
 * This extension is used by Mashup
 *
 * Name : EXT050MI.AddNewDel
 * Description : Adds new delivery.
 * Date         Changed By   Description
 * 20230504     ARENARD      LOG28 - Creation of files and containers
 * 20230504     FLEBARS      LOG28 - Correction - adapatations
 * */
import java.math.RoundingMode
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

public class AddNewDel extends ExtendM3Transaction {
  private final MIAPI mi
  private final IonAPI ion
  private final DatabaseAPI database
  private final LoggerAPI logger
  private final MICallerAPI miCaller
  private final ProgramAPI program
  private final UtilityAPI utility
  private final ExceptionAPI exception

  private Integer currentCompany

  private HashMap<String, String> blockedIndexes
  private String newDelivery

  private String oolineOrno
  private String oolineItno
  private double oolineOrqt
  private String oolineAlun
  private String oolineLtyp
  private String oolineWhlo
  private String oolineDwdz
  private String oolineDwhz
  private String oolineAdid
  private String oolinePide
  private String oolineDip4
  private String oolineDwdt
  private String oolinePldt
  private String oolineSapr
  private Integer oolinePonr
  private Integer oolinePosx
  private String oolineRorc
  private String oolineRorn
  private Integer oolineRorl
  private Integer oolineRorx

  private String mitmasItno
  private String mitmasUnms
  private String mitbalWhlo
  private int mitbalAlmt

  private double ext057Alqt
  private int newPonr
  private int newPosx
  private LinkedHashMap<String, Map<String, String>> itemUnits


  private boolean completeLineProcess

  private String bjno
  private boolean in60 = false
  private String msgd

  public AddNewDel(MIAPI mi, DatabaseAPI database, LoggerAPI logger, ProgramAPI program, UtilityAPI utility, MICallerAPI miCaller, IonAPI ion, ExceptionAPI exception) {
    this.mi = mi
    this.ion = ion
    this.database = database
    this.logger = logger
    this.program = program
    this.utility = utility
    this.miCaller = miCaller
    this.exception = exception
  }


  /**
   * Main Method
   */
  public void main() {
    // Check company
    if (mi.in.get("CONO") == null) {
      currentCompany = (Integer) program.getLDAZD().CONO
    } else {
      currentCompany = mi.in.get("CONO") as Integer
    }

    // Check job number
    bjno = mi.in.get("BJNO")
    if (bjno == null) {
      mi.error("Numéro de job est obligatoire")
      return
    }
    logger.debug("EXT050MI.AddNewDel bjno:${bjno}")
    //if (true)
    //  return

    // Check process
    String ztrt = mi.in.get("ZTRT")
    if (ztrt == null) {
      mi.error("Traitement est obligatoire")
      return
    } else {
      if (ztrt != "LIN" && ztrt != "PAL") {
        mi.error("Traitement " + ztrt + " est invalide")
        return
      }
    }

    logger.debug("EXT050MI/AddNewDel bjno=${bjno} trt=${ztrt}")

    // Execute order line process
    if (ztrt == "LIN") {
      lineProcess()
    }

    if (ztrt == "PAL") {
      palletProcess()
      lineProcess()
    }

    deleteExt057()
    deleteExt059()

    if (in60) {
      exception.throwErrorMIResponseException(msgd)
    }


    mi.outData.put("DLIX", newDelivery)
    mi.write()
  }

  /**
   *
   * @param itno
   * @param alun
   * @param orqt unms qty
   * @param orqa alun qty
   * @return ortqt converted from unms to alun if orqt != 0 else ortqa converted from alun to unms if orqt != 0
   */
  public double convertQty(String itno, String alun, double orqt, double orqa) {
    logger.debug("convertQty itno=${itno} alun=${alun} orqt=${orqt} orqa=${orqa}")

    alun = alun.trim() == "" ? "UVC" : alun

    boolean load = false
    String key = "${itno}_${alun}"
    if (itemUnits == null) {
      load = true
      itemUnits = new LinkedHashMap<String, Map<String, String>>()
    } else if (!itemUnits.containsKey(key)) {
      load = true
    }
    if (load)
      itemUnits.put(key, loadItemUnitData(itno, alun))

    Map<String, String> currentItemUnit = itemUnits.get(key)

    int dmcf = currentItemUnit["DMCF"] as Integer
    double cofa = currentItemUnit["COFA"] as Double
    int dccd = currentItemUnit["DCCD"] as Integer
    double result = 0

    //convert from base unms
    if (orqt != 0)
      result = dmcf == 1 ? orqt / cofa : orqt * cofa
    //convert to base unms
    else
      result = dmcf == 1 ? orqa * cofa : orqa / cofa

    //Rounding
    double fres = new BigDecimal(result).setScale(dccd, RoundingMode.HALF_EVEN).doubleValue()
    return fres
  }


  /**
   * Treatment type LIN
   */
  public void lineProcess() {
    // Complete lines processing
    logger.debug("Method lineProcess complete lines complete")
    completeLineProcess = true
    lineTransfer()

    // Partial lines processing
    logger.debug("Method lineProcess complete lines partiel")
    completeLineProcess = false
    if (!completeLineProcess) {
      prepareTransfer()
      //lineTransfer()
    }
  }

  /**
   *
   */
  public void palletProcess() {
    logger.debug("Debut splitPalletIntoLines")
    double currentAlqt = 0
    String currentUCA4 = ""
    String currentUCA5 = ""
    String currentUCA6 = ""
    String currentCamu = ""

    String orno = ""
    String ponr = ""
    String posx = ""



    // Read MITALO and add order lines into EXT057
    DBAction ext059Query = database.table("EXT059").index("00").selection(
      "EXCONO"
      , "EXCAMU"
      , "EXTLIX"
      , "EXUCA4"
      , "EXUCA5"
      , "EXUCA6"
    ).build()

    DBContainer ext059Request = ext059Query.getContainer()
    ext059Request.set("EXBJNO", bjno)
    ext059Request.set("EXCONO", currentCompany)


    // Update EXT057
    Closure<?> ext057Updater = { LockedResult ext057Lockedresult ->
      LocalDateTime timeOfCreation = LocalDateTime.now()
      int changeNumber = ext057Lockedresult.get("EXCHNO") as Integer
      String cams = ext057Lockedresult.get("EXCAMS") as String
      double allocatedQuantity = ext057Lockedresult.get("EXALQT") as Double

      allocatedQuantity = allocatedQuantity + currentAlqt

      logger.debug("palletProcess update EXT057 orno:${orno} ponr:${ponr} posx:${posx} qtyUPA:=${allocatedQuantity}")
      ext057Lockedresult.set("EXALQT", allocatedQuantity)
      ext057Lockedresult.setInt("EXLMDT", timeOfCreation.format(DateTimeFormatter.ofPattern("yyyyMMdd")) as Integer)
      ext057Lockedresult.setInt("EXCHNO", changeNumber + 1)

      if (!cams.contains(currentCamu))
        ext057Lockedresult.set("EXCAMS", cams + ";" + currentCamu)

      ext057Lockedresult.set("EXCHID", program.getUser())
      ext057Lockedresult.update()
    }


    // Retrieve MITALO
    Closure<?> mitaloReader = { DBContainer mitaloResult ->
      currentAlqt = mitaloResult.get("MQALQT") as double
      currentCamu = mitaloResult.get("MQCAMU") as String
      orno = mitaloResult.get("MQRIDN")
      ponr = mitaloResult.get("MQRIDL")
      posx = mitaloResult.get("MQRIDX")

      logger.debug("found MITALO - orno:${orno} ponr:${ponr} posx:${posx} camu:${currentCamu} alqt:${currentAlqt}")
      LocalDateTime timeOfCreation = LocalDateTime.now()

      if (checkOohead(mitaloResult.get("MQRIDN") as String, currentUCA4, currentUCA5, currentUCA6)) {

        getoolineData(mitaloResult.get("MQRIDN") as String, mitaloResult.get("MQRIDL") as String, mitaloResult.get("MQRIDX") as String)
        DBAction ext057Query = database.table("EXT057").index("00").selection("EXCAMS", "EXALQT", "EXALUN").build()
        DBContainer ext057Request = ext057Query.getContainer()

        ext057Request.set("EXBJNO", bjno)
        ext057Request.set("EXCONO", currentCompany)
        ext057Request.set("EXORNO", orno)
        ext057Request.set("EXPONR", ponr as Integer)
        ext057Request.set("EXPOSX", posx as Integer)
        if (!ext057Query.read(ext057Request)) {
          logger.debug("palletProcess create EXT057 orno:${orno} ponr:${ponr} posx:${posx} currentALQT:=${currentAlqt}")
          ext057Request.set("EXALQT", currentAlqt)
          ext057Request.set("EXALUN", "")
          ext057Request.setInt("EXRGDT", timeOfCreation.format(DateTimeFormatter.ofPattern("yyyyMMdd")) as Integer)
          ext057Request.setInt("EXRGTM", timeOfCreation.format(DateTimeFormatter.ofPattern("HHmmss")) as Integer)
          ext057Request.setInt("EXLMDT", timeOfCreation.format(DateTimeFormatter.ofPattern("yyyyMMdd")) as Integer)
          ext057Request.setInt("EXCHNO", 1)
          ext057Request.set("EXCAMS", currentCamu)
          ext057Request.set("EXCHID", program.getUser())
          ext057Request.set("EXDLIX", getDlix(3, mitaloResult.get("MQRIDN") as String, mitaloResult.get("MQRIDL") as int, mitaloResult.get("MQRIDX") as int))
          ext057Query.insert(ext057Request)
        } else {
          ext057Request.set("EXBJNO", bjno)
          ext057Request.set("EXCONO", currentCompany)
          ext057Request.set("EXORNO", mitaloResult.get("MQRIDN"))
          ext057Request.set("EXPONR", mitaloResult.get("MQRIDL"))
          ext057Request.set("EXPOSX", mitaloResult.get("MQRIDX"))
          if (ext057Query.readLock(ext057Request, ext057Updater)) {
          }
        }
      }
    }

    //Reader EXT059 record
    Closure<?> ext059Reader = { DBContainer ext059Result ->
      String camu = ext059Result.get("EXCAMU")
      logger.debug("ext059Result " + camu)
      currentUCA4 = ext059Result.get("EXUCA4") as String
      currentUCA5 = ext059Result.get("EXUCA5") as String
      currentUCA6 = ext059Result.get("EXUCA6") as String

      //find corresponding allocation in MITALO by CONO, TTYP = 31 and CAMU
      ExpressionFactory mitaloExpression = database.getExpressionFactory("MITALO")
      mitaloExpression = mitaloExpression.eq("MQCAMU", camu)
      DBAction mitaloQuery = database.table("MITALO")
        .index("10").matching(mitaloExpression)
        .selection(
          "MQCAMU",
          "MQRIDN",
          "MQRIDL",
          "MQRIDX", "" +
          "MQALQT").build()
      DBContainer mitaloRequest = mitaloQuery.getContainer()
      mitaloRequest.set("MQCONO", currentCompany)
      mitaloRequest.set("MQTTYP", 31)
      if (mitaloQuery.readAll(mitaloRequest, 2, 10000, mitaloReader)) {
      }
    }

    if (!ext059Query.readAll(ext059Request, 2, 10000, ext059Reader)) {
      in60 = true
      msgd = "Aucune données pour le job " + bjno
    }
  }

  /**
   * Get delivery index for order line
   * @param rorc
   * @param ridn
   * @param ridl
   * @param ridx
   */
  public long getDlix(int rorc, String ridn, int ridl, int ridx) {
    long dlix = 0

    DBAction mhdislQuery = database.table("MHDISL").index("10").build()
    DBContainer mhdislRequest = mhdislQuery.getContainer()
    mhdislRequest.set("URCONO", currentCompany)
    mhdislRequest.set("URRORC", rorc)
    mhdislRequest.set("URRIDN", ridn)
    mhdislRequest.set("URRIDL", ridl)
    mhdislRequest.set("URRIDX", ridx)

    Closure<?> mhdislReader = { DBContainer mhdislResult ->
      if (dlix == 0)
        dlix = mhdislResult.get("URDLIX") as long
    }
    if (!mhdislQuery.readAll(mhdislRequest, 5, 10000, mhdislReader)) {
      mi.error("L'enregistrement n'existe pas")
      return 0
    }

    return dlix
  }


  /**
   *
   */
  public void lineTransfer() {
    logger.debug("Method lineTransfer")
    newDelivery = ""
    ExpressionFactory ext057Expression = database.getExpressionFactory("EXT057")


    DBAction ext057Query = database.table("EXT057").index("10")
      .matching(ext057Expression)
      .selection("EXCONO",
        "EXORNO",
        "EXPONR",
        "EXPOSX",
        "EXDLIX",
        "EXALQT",
        "EXALUN",
        "EXTLIX").build()
    DBContainer ext057request = ext057Query.getContainer()
    ext057request.set("EXBJNO", bjno)
    if (!ext057Query.readAll(ext057request, 1, 10000,ext057ReaderLineTransfer)) {
    }
  }

  /**
   * TODO Comment
   */
  public void prepareTransfer() {
    ExpressionFactory ext057Expression = database.getExpressionFactory("EXT057")
    DBAction ext057Query = database
      .table("EXT057")
      .index("10")
      .matching(ext057Expression)
      .selection("EXCONO",
        "EXORNO",
        "EXPONR",
        "EXPOSX",
        "EXDLIX",
        "EXALQT",
        "EXTLIX",
        "EXCAMS",
        "EXALUN")
      .build()
    DBContainer ext057Request = ext057Query.getContainer()
    ext057Request.set("EXBJNO", bjno)
    if (!ext057Query.readAll(ext057Request, 1, 10000,ext057ReaderPrepareTransfer)) {
    }
  }

  /**
   * Load relative indexes
   * An existing delivery is considered available if the following fields are the same or similar:
   *   CONO - Division
   *   AGKY - Aggregation key
   *   CONA - Consignee
   *   COAA - Consignee address
   *
   *   The aggregation key AGKY field is a concatenation of these fields:
   *   INOU - Direction
   *   RORC - Reference order category
   *   MWLO - Departure warehouse
   *   DPOL - Dispatch policy
   *   MODL - Delivery method
   *   TEDL - Delivery terms
   *   SROT - Requested route
   *
   * When checking if the delivery line can be moved to an existing delivery head, M3 will also check:
   *   If the delivery to move to is not at PGRS status 90
   *   If the delivery to move to is blocked for new lines (BLOP=1) NOT IN OUR CASE
   *   If the dispatch policy allows several orders to be managed with the same delivery (MWS010 parm 20)
   *   If the delivery consolidation code DCC1 is the same on from and to delivery
   *   If  weight limitations have not been exceeded
   *   If one of the delivery is a direct delivery with a delivery note ref
   *
   * @param dlix
   */
  public void blockRelativeIndexes(String dlix, String tlix) {
    blockedIndexes = new LinkedHashMap()

    if (tlix == "")
      tlix = 0
    String cona = ""
    String coaa = ""
    String agky = ""
    String dcc1 = ""


    //
    //  Get datas from original MHDISH
    //
    DBAction mhdishQuery = database.table("MHDISH")
      .index("00")
      .selection(
        "OQCONO"
        , "OQINOU"
        , "OQDLIX"
        , "OQCONA"
        , "OQCOAA"
        , "OQAGKY"
        , "OQDCC1"
      )
      .build()

    DBContainer mhdishRequest = mhdishQuery.getContainer()
    mhdishRequest.set("OQCONO", currentCompany)
    mhdishRequest.set("OQINOU", 1)
    mhdishRequest.set("OQDLIX", dlix as Long)

    if (mhdishQuery.read(mhdishRequest)) {
      cona = mhdishRequest.get("OQCONA") as String
      coaa = mhdishRequest.get("OQCOAA") as String
      agky = mhdishRequest.get("OQAGKY") as String
      dcc1 = mhdishRequest.get("OQDCC1") as String
    }

    //
    //  Load relative indexes
    //
    ExpressionFactory mhdish2Expression = database.getExpressionFactory("MHDISH")
    mhdish2Expression = mhdish2Expression.eq("OQCOAA", coaa)
    mhdish2Expression = mhdish2Expression.and(mhdish2Expression.eq("OQAGKY", agky))
    mhdish2Expression = mhdish2Expression.and(mhdish2Expression.eq("OQDCC1", dcc1))
    mhdish2Expression = mhdish2Expression.and(mhdish2Expression.lt("OQPGRS", '90'))

    DBAction mhdish2Query = database.table("MHDISH")
      .matching(mhdish2Expression)
      .index("50")
      .selection(
        "OQCONO"
        , "OQCONA"
        , "OQPUSN"
        , "OQPUTP"
        , "OQINOU"
        , "OQDLIX"
      ).build()
    logger.debug("loadRelativeIndexes dlix:${dlix} tlix:${tlix}")
    DBContainer mhdish2Request = mhdishQuery.getContainer()
    mhdish2Request.set("OQCONO", currentCompany)
    mhdishRequest.set("OQINOU", 1)
    mhdishRequest.set("OQCONA", cona)


    Closure<?> mhdish2Reader = { DBContainer mhdish2Result ->
      String curdlix = mhdish2Result.get("OQDLIX") as String
      logger.debug("curdlix:${curdlix}")
      if (curdlix != tlix) {
        blockedIndexes.put(curdlix, curdlix)
        blopunblopIndex(curdlix, 1)
      }
    }

    //Launch query
    if (!mhdish2Query.readAll(mhdish2Request, 3, 10000, mhdish2Reader)) {
    }
  }

  /**
   * Loop on blockedIndexes call blopunblopIndex to set blop=0
   */
  public void unblockingIndexes() {
    Iterator<String> it = blockedIndexes.keySet().iterator()
    while (it.hasNext()) {
      String dlix = it.next()
      blopunblopIndex(dlix, 0)
    }
  }


  /**
   * Read EXT057 data
   * and call MWS411MI/MoveDelLn
   */
  Closure<?> ext057ReaderLineTransfer = { DBContainer ext057Result ->
    String dlix = ext057Result.get("EXDLIX")
    String tlix = ext057Result.get("EXTLIX")
    String rorc = "3"
    String ridn = ext057Result.get("EXORNO")
    String ridl = ext057Result.get("EXPONR")
    String ridx = ext057Result.get("EXPOSX")
    double alqt = ext057Result.get("EXALQT") as double
    String alun = ext057Result.get("EXALUN")



    if (getoolineData(ridn, ridl, ridx)) {
      logger.debug("closure outData_EXT057_lineTransfer : DLIX=${dlix}, TLIX=${tlix} RIDN=${ridn} RIDL=${ridl} RIDX=${ridx}")
      if (alqt != 0)
        alqt = convertQty(this.oolineOrno, alun, 0, alqt)

      logger.debug("TETS DDD alqt:${alqt} orqt:${oolineOrqt}")

      if (alqt == oolineOrqt || alqt == 0) {
        String theDLIX = tlix.length() > 0 && tlix != "0" ? tlix : newDelivery
        blockRelativeIndexes(dlix, theDLIX)
        executeMWS411MIMoveDelLn(dlix, rorc, ridn, ridl, ridx, theDLIX)
        unblockingIndexes()
      }
    }
  }

  /**
   *
   * @param orno
   * @param ucA4
   * @param uca5
   * @param uca6
   * @return
   */
  public checkOohead(String orno, String uca4, String uca5, String uca6) {
    logger.debug("check OOHEAD orno=${orno} uca4=${uca4} uca5=${uca5} uca6=${uca6}")
    DBAction ooheadQuery = database.table("OOHEAD").index("00").selection(
      "OAOBLC",
      "OAHOCD",
      "OAJNA",
      "OAJNU",
      "OAUCA4",
      "OAUCA5",
      "OAUCA6",
      "OACHID").build()
    DBContainer ooheadRequest = ooheadQuery.getContainer()
    ooheadRequest.set("OACONO", currentCompany)
    ooheadRequest.set("OAORNO", orno)
    if (ooheadQuery.read(ooheadRequest)) {
      String oaoblc = ooheadRequest.get("OAOBLC") as String
      String oahocd = ooheadRequest.get("OAHOCD") as String
      String oajna = ooheadRequest.get("OAJNA") as String
      String oajnu = ooheadRequest.get("OAJNU") as String
      String oachid = ooheadRequest.get("OACHID") as String
      String oauca4 = ooheadRequest.get("OAUCA4") as String
      String oauca5 = ooheadRequest.get("OAUCA5") as String
      String oauca6 = ooheadRequest.get("OAUCA6") as String
      if (oaoblc.equals("1") || oahocd.equals("1")) {
        in60 = true
        msgd = "La commande ${orno} est bloquée par ${oachid} programme ${oajna} / ${oajnu}"
        return false
      }

      if (oauca4.trim() != uca4.trim() || oauca5.trim() != uca5.trim() || oauca6.trim() != uca6.trim()) {
        return false
      }
    }
    return true
  }

  /**
   * Load OOLINE Informations
   */
  public boolean getoolineData(String orno, String ponr, String posx) {
    logger.debug(String.format("method getoolineDATA : ORNO=%s, PONR=%s, POSX=%s ", orno, ponr, posx))
    if (oolineOrno == orno && oolinePonr && ponr && oolinePosx == posx) {
      return true
    }


    logger.debug(String.format("method getoolineDATA : ORNO=%s, PONR=%s, POSX=%s ", orno, ponr, posx))
    DBAction ooheadQuery = database.table("OOHEAD").index("00").selection(
      "OAOBLC",
      "OAHOCD",
      "OAJNA",
      "OAJNU",
      "OACHID").build()
    DBContainer ooheadRequest = ooheadQuery.getContainer()
    ooheadRequest.set("OACONO", currentCompany)
    ooheadRequest.set("OAORNO", orno)
    if (ooheadQuery.read(ooheadRequest)) {
      String oaoblc = ooheadRequest.get("OAOBLC") as String
      String oahocd = ooheadRequest.get("OAHOCD") as String
      String oajna = (ooheadRequest.get("OAJNA") as String).trim()
      String oajnu = (ooheadRequest.get("OAJNU") as String).trim()
      String oachid = (ooheadRequest.get("OACHID") as String).trim()
      if (oaoblc.equals("1") || oahocd.equals("1")) {
        in60 = true
        msgd = "La commande ${orno} est bloquée par ${oachid} programme ${oajna} / ${oajnu}"
        logger.debug(msgd)
        return false
      }
    }

    logger.debug(String.format("method read line getoolineDATA : ORNO=%s, PONR=%s, POSX=%s ", orno, ponr, posx))
    DBAction oolinequery = database.table("OOLINE").index("00").selection(
      "OBORNO",
      "OBPONR",
      "OBPOSX",
      "OBRORC",
      "OBRORN",
      "OBRORL",
      "OBRORX",
      "OBITNO",
      "OBORQT",
      "OBALUN",
      "OBLTYP",
      "OBWHLO",
      "OBDWDZ",
      "OBDWHZ",
      "OBADID",
      "OBPIDE",
      "OBDIP4",
      "OBDWDT",
      "OBPLDT",
      "OBSAPR").build()

    DBContainer oolineRequest = oolinequery.getContainer()
    oolineRequest.set("OBCONO", currentCompany)
    oolineRequest.set("OBORNO", orno)
    oolineRequest.set("OBPONR", Integer.parseInt(ponr))
    oolineRequest.set("OBPOSX", Integer.parseInt(posx))
    if (oolinequery.read(oolineRequest)) {
      oolineOrno = oolineRequest.get("OBORNO")
      oolinePonr = oolineRequest.get("OBPONR") as Integer
      oolinePosx = oolineRequest.get("OBPOSX") as Integer
      oolineRorc = oolineRequest.get("OBRORC")
      oolineRorn = oolineRequest.get("OBRORN")
      oolineRorl = oolineRequest.get("OBRORL") as Integer
      oolineRorx = oolineRequest.get("OBRORX") as Integer
      this.oolineOrno = oolineRequest.get("OBITNO")
      oolineOrqt = oolineRequest.get("OBORQT") as Double
      oolineAlun = oolineRequest.get("OBALUN")
      oolineLtyp = oolineRequest.get("OBLTYP")
      oolineWhlo = oolineRequest.get("OBWHLO")
      oolineDwdz = oolineRequest.get("OBDWDZ")
      oolineDwhz = oolineRequest.get("OBDWHZ")
      oolineAdid = oolineRequest.get("OBADID")
      oolinePide = oolineRequest.get("OBPIDE")
      oolineDip4 = oolineRequest.get("OBDIP4")
      oolineDwdt = oolineRequest.get("OBDWDT")
      oolinePldt = oolineRequest.get("OBPLDT")
      oolineSapr = oolineRequest.get("OBSAPR")
      logger.debug("oolineRORL:${oolineRorl} oolineRORC:${oolineRorc}")
      return true
    }
    logger.debug(String.format("closure getoolineDATA NOT FOUND : cono=%s ORNO=%s, PONR=%s, POSX=%s ", currentCompany + "", orno, ponr, posx))
    return false
  }
  /**
   * Execute MWS411MI.MoveDelLn
   */
  private executeMWS411MIMoveDelLn(String dlix, String rorc, String ridn, String ridl, String ridx, String tlix) {
    Map<String, String> parameters = ["DLIX": dlix, "RORC": rorc, "RIDN": ridn, "RIDL": ridl, "RIDX": ridx, "TLIX": tlix]
    logger.debug("call executeMWS411MIMoveDelLn : DLIX:${dlix}, RORC=${rorc}, RIDN:${ridn}, RIDL=${ridl}, RIDX=${ridx}, TLIX=${tlix}")
    Closure<?> handler = { Map<String, String> response ->
      if (response.TLIX != null)
        newDelivery = response.TLIX.trim()

      //logger.debug(String.format("call after executeMWS411MIMoveDelLn : DLIX=%s, RORC=%s, RIDN=%s, RIDL=%s, RIDX=%s, TLIX=%s", dlix, rorc, RIDN, RIDL, ridx, tlix))

      if (response.error != null) {
        return mi.error("Failed MWS411MI.MoveDelLn: " + response.errorMessage)
      }
    }
    miCaller.call("MWS411MI", "MoveDelLn", parameters, handler)

    //blopunblopIndex(dlix, 0)
  }
  /**
   *
   */
  public void blopunblopIndex(String index, int blop) {
    /*
      We have to update MHDISH reocrd in order to lock the delivery
      then to have ourt new delivery lines on a new delivery heazd
      it works like interractive program MWS410 option 51 & 52
      Support Case  : 17334686
      EHR           : #98882
    */
    logger.debug("blopunblopIndex dlix:${index} blop:${blop}")
    if (index == 0 || !(blop == 0 || blop == 1)) {
      return
    }

    DBAction mhdishQuery = database.table("MHDISH").index("00").selection("OQDLIX", "OQBLOP").build()
    DBContainer mhdishRequest = mhdishQuery.getContainer()
    mhdishRequest.set("OQCONO", currentCompany)
    mhdishRequest.set("OQINOU", 1)
    mhdishRequest.set("OQDLIX", index as Long)

    Closure<?> mhdishUpdater = { LockedResult mdishLockedResult ->
      mdishLockedResult.set("OQBLOP", blop)
      mdishLockedResult.update()
    }

    if (mhdishQuery.readLock(mhdishRequest, mhdishUpdater)) {
    }
  }

  /**
   * Loop ON EXT057
   *
   */
  Closure<?> ext057ReaderPrepareTransfer = { DBContainer ext057Result ->
    logger.debug(String.format("closure outData_EXT057_prepareTransfer : ORNO=%s, PONR=%s, POSX=%s ", ext057Result.get("EXORNO"), ext057Result.get("EXPONR"), ext057Result.get("EXPOSX")))
    String orno = ext057Result.get("EXORNO") as String
    String ponr = ext057Result.get("EXPONR") as String
    String posx = ext057Result.get("EXPOSX") as String
    double alqt = ext057Result.get("EXALQT") as Double
    String alun = ext057Result.get("EXALUN") as String


    if (getoolineData(ext057Result.getString("EXORNO"), "" + ext057Result.get("EXPONR"), "" + ext057Result.get("EXPOSX"))) {
      // CONVERT CONVERSION PAL to UMNS
      ext057Alqt = convertQty(this.oolineOrno, alun, 0, alqt)
      logger.debug("RETEST DDD alqt:${ext057Alqt} orqt:${oolineOrqt}")

      if (oolineOrqt != ext057Alqt)
        transferLinePartial(ext057Result.get("EXDLIX") as String, ext057Result.get("EXTLIX") as String, ext057Result.get("EXCAMS") as String)
    }
  }


  /**
   * break link between CO & PO
   */
  public void transferLinePartial(String dlix, String tlix, String cams) {
    logger.debug("method breakLinkedOrders")

    //Save MITPLO and MITALO RECORDS
    LinkedList<Object> mitplos = new LinkedList<Object>()
    DBAction mitploQuery = database.table("MITPLO").index("10").selection(
      "MOWHLO"
      , "MOITNO"
      , "MOPLDT"
      , "MOTIHM"
      , "MOORCA"
      , "MORIDN"
      , "MORIDL"
      , "MORIDX"
      , "MORIDI"
      , "MOSTAT"
      , "MOALQT").build()
    DBContainer mitploRequest = mitploQuery.getContainer()
    mitploRequest.set("MOCONO", currentCompany)
    mitploRequest.set("MOORCA", "311")
    mitploRequest.set("MORIDN", oolineOrno)
    mitploRequest.set("MORIDL", oolinePonr)
    mitploRequest.set("MORIDX", oolinePosx)

    Closure<?> mitploReader = { DBContainer mitploResult ->
      //Define return object structure
      Map<String, String> mitploData = [
        "MOWHLO"  : "" + mitploResult.get("MOWHLO")
        , "MOITNO": "" + mitploResult.get("MOITNO")
        , "MOPLDT": "" + mitploResult.get("MOPLDT")
        , "MOTIHM": "" + mitploResult.get("MOTIHM")
        , "MOORCA": "" + mitploResult.get("MOORCA")
        , "MORIDN": "" + mitploResult.get("MORIDN")
        , "MORIDL": "" + mitploResult.get("MORIDL")
        , "MORIDX": "" + mitploResult.get("MORIDX")
        , "MORIDI": "" + mitploResult.get("MORIDI")
        , "MOSTAT": "" + mitploResult.get("MOSTAT")
        , "MOALQT": "" + mitploResult.get("MOALQT")
      ]
      mitplos.add(mitploData)
    }
    mitploQuery.readAll(mitploRequest, 5, 10000, mitploReader)


    //Save MITALO RECORDS
    LinkedList<Map<String, String>> mitalos = new LinkedList<Map<String, String>>()
    DBAction mitaloQuery = database.table("MITALO").index("10").selection(
      "MQWHLO",
      "MQITNO",
      "MQWHSL",
      "MQBANO",
      "MQCAMU",
      "MQREPN",
      "MQALQT").build()
    DBContainer mitaloQueryrequest = mitaloQuery.getContainer()
    mitaloQueryrequest.set("MQCONO", currentCompany)
    mitaloQueryrequest.set("MQTTYP", 31)
    mitaloQueryrequest.set("MQRIDN", oolineOrno)
    mitaloQueryrequest.set("MQRIDO", 0)
    mitaloQueryrequest.set("MQRIDL", oolinePonr)
    mitaloQueryrequest.set("MQRIDX", oolinePosx)

    Closure<?> mitaloReader = { DBContainer mitaloResult ->
      //Define return object structure
      Map<String, String> mitaloData = [
        "MQWHLO"  : "" + mitaloResult.get("MQWHLO")
        , "MQITNO": "" + mitaloResult.get("MQITNO")
        , "MQWHSL": "" + mitaloResult.get("MQWHSL")
        , "MQBANO": "" + mitaloResult.get("MQBANO")
        , "MQCAMU": "" + mitaloResult.get("MQCAMU")
        , "MQTTYP": "" + mitaloResult.get("MQTTYP")
        , "MQRIDN": "" + mitaloResult.get("MQRIDN")
        , "MQRIDO": "" + mitaloResult.get("MQRIDO")
        , "MQRIDL": "" + mitaloResult.get("MQRIDL")
        , "MQRIDX": "" + mitaloResult.get("MQRIDX")
        , "MQRIDI": "" + mitaloResult.get("MQRIDI")
        , "MQPLSX": "" + mitaloResult.get("MQPLSX")
        , "MQSOFT": "" + mitaloResult.get("MQSOFT")
        , "MQALQT": "" + mitaloResult.get("MQALQT")
      ]
      mitalos.add(mitaloData)
    }

    mitaloQuery.readAll(mitaloQueryrequest, 6, 10000, mitaloReader)


    logger.debug("oolineRORL:${oolineRorl} oolineRORC:${oolineRorc}")
    // Break link with purchase order
    if (oolineRorc.trim() == "2") {
      // Remove link from purchase order line
      executePPS200MIUpdLine(oolineRorn, "" + oolineRorl, "" + oolineRorx, "0", "0", "0", "0")
    }

    //deallocation
    if (mitbalAlmt != 6 && mitbalAlmt != 7)
      executeMMS120MIDeAllocateOrLne("31", oolineOrno, "" + oolinePonr, "" + oolinePosx, "M")

    //Reduce Qty on original line
    logger.debug("calc new orqa ")
    double newOrqa = convertQty(this.oolineOrno, oolineAlun, oolineOrqt - ext057Alqt, 0)
    logger.debug("calc new orqa ${newOrqa}")
    blockRelativeIndexes(dlix, dlix)
    executeOIS100MIChgLineBatchEnt(oolineOrno, "" + oolinePonr, "" + oolinePosx, oolineRorn, newOrqa as String)
    unblockingIndexes()

    //recreate link with po line
    if (oolineRorc.trim() == "2") {
      executePPS200MIUpdLine(oolineRorn, "" + oolineRorl, "" + oolineRorx, "3", oolineOrno, "" + oolinePonr, "" + oolinePosx)
    }
    //Create new co line
    if (ext057Alqt > 0) {
      String theDlix = tlix.length() > 0 && tlix != "0" ? tlix : newDelivery
      blockRelativeIndexes(dlix, theDlix)
      newPonr = 0
      newPosx = 0
      logger.debug("calc new line orqa ")
      executeOIS100MIAddOrderLine(oolineOrno, this.oolineOrno, ext057Alqt as String, 'UVC', "0", oolineWhlo, oolineDwdz, oolineDwhz, oolineAdid, oolinePide, oolineDip4, oolineDwdt, oolinePldt, oolineSapr)
      unblockingIndexes()
    }

    //realoc
    double qteOriginal = oolineOrqt
    double qteNewLine = ext057Alqt
    double qteOldLine = oolineOrqt - ext057Alqt

    if (mitbalAlmt != 6 && mitbalAlmt != 7) {
      for (Map<String, String> mitalo in mitalos) {
        String mitaloItno = mitalo["MQITNO"] as String
        String mitaloBano = mitalo["MQBANO"] as String
        String mitaloCamu = mitalo["MQCAMU"] as String
        double mitaloAlqt = mitalo["MQALQT"] as Double

        double alqt = 0
        logger.debug("affectation itno:${mitaloItno} bano:${mitaloBano} camu:${mitaloCamu} qte_old_line:${qteOldLine} qte_new_line:${qteNewLine} cams:${cams}")
        if (qteOldLine > 0 && (!cams.contains(mitaloCamu) || cams.trim() == "")) {
          alqt = qteOldLine <= mitaloAlqt ? qteOldLine : mitaloAlqt
          logger.debug("affectation oldline itno:${mitaloItno} bano:${mitaloBano} camu:${mitaloCamu} qte_old_line:${qteOldLine} qte_new_line:${qteNewLine} cams:${cams}")
          executeMMS120MIUpdDetAlloc(
            mitalo["MQWHLO"] as String
            , mitalo["MQITNO"] as String
            , mitalo["MQWHSL"] as String
            , mitalo["MQBANO"] as String
            , mitalo["MQCAMU"] as String
            , alqt as String
            , "31"
            , oolineOrno
            , oolinePonr as String
            , oolinePosx as String)
          qteOldLine -= alqt
          mitaloAlqt -= alqt
        }
        if (qteNewLine > 0 &&  (cams.contains(mitaloCamu) || cams.trim() == "")) {
          alqt = qteNewLine <= mitaloAlqt ? qteNewLine : mitaloAlqt
          logger.debug("affectation newline itno:${mitaloItno} bano:${mitaloBano} camu:${mitaloCamu} qte_old_line:${qteOldLine} qte_new_line:${qteNewLine} cams:${cams}")
          if (alqt > 0) {
            executeMMS120MIUpdDetAlloc(
              mitalo["MQWHLO"] as String
              , mitalo["MQITNO"] as String
              , mitalo["MQWHSL"] as String
              , mitalo["MQBANO"] as String
              , mitalo["MQCAMU"] as String
              , alqt as String
              , "31"
              , oolineOrno
              , newPonr as String
              , newPosx as String)
            mitaloAlqt -= alqt
          }
        }
      }
    }
  }

  /**
   *
   * @param TTYP
   * @param RIDN
   * @param RIDL
   * @param RIDX
   * @param MAAL
   * @return
   */
  private executeMMS120MIDeAllocateOrLne(String TTYP, String RIDN, String RIDL, String RIDX, String MAAL) {
    logger.debug("executeMMS120MIDeAllocateOrLne ${RIDN} ${RIDL} ${RIDX}")
    Map<String, String> parameters = ["TTYP": TTYP, "RIDN": RIDN, "RIDL": RIDL, "RIDX": RIDX, "MAAL": MAAL]
    Closure<?> handler = { Map<String, String> response ->
      if (response.error != null) {
        logger.debug("executeMMS120MIDeAllocateOrLne error TTYP:${TTYP}, RIDN:${RIDN}, RIDL:${RIDL}, RIDX:${RIDX}, MAAL:${MAAL}")
        return mi.error("Failed MMS120MI.DeAllocateOrLne: " + response.errorMessage)
      }
    }
    miCaller.call("MMS120MI", "DeAllocateOrLne", parameters, handler)
  }

  /**
   *
   * @param PUNO
   * @param PNLI
   * @param PNLS
   * @param RORC
   * @param RORN
   * @param RORL
   * @param RORX
   * @return
   */
  private executePPS200MIUpdLine(String PUNO, String PNLI, String PNLS, String RORC, String RORN, String RORL, String RORX) {
    //CALL PPS200MI tru IONAPI
    logger.debug("executePPS200MIUpdLine : ligne OA ${PUNO} ${PNLI} ${PNLS} ligne CDV ${RORC} ${RORL} ${RORX}")
    String endpoint = "/M3/m3api-rest/v2/execute/PPS200MI/UpdLine"
    Map<String, String> headers = ["Accept": "application/json"]
    Map<String, String> queryParameters = [
      "cono": "" + currentCompany,
      "PUNO": PUNO,
      "PNLI": PNLI,
      "PNLS": PNLS,
      "RORC": RORC,
      "RORN": RORN,
      "RORL": RORL,
      "RORX": RORX
    ]
    IonResponse response = ion.get(endpoint, headers, queryParameters)
    if (response.getError()) {
      logger.debug("Failed calling ION API, detailed error message: ${response.getErrorMessage()}")
      return
    }
    if (response.getStatusCode() != 200) {
      logger.debug("Expected status 200 but got ${response.getStatusCode()} instead")
      return
    }

    logger.debug("response content ${response.getContent()}")

  }

  /**
   *
   * @param ORNO
   * @param PONR
   * @param POSX
   * @param UCA2
   * @param ORQA
   * @return
   */
  private executeOIS100MIChgLineBatchEnt(String ORNO, String PONR, String POSX, String UCA2, String ORQA) {
    Map<String, String> parameters = ["ORNO": ORNO, "PONR": PONR, "POSX": POSX, "UCA2": UCA2, "ORQA": ORQA]
    Closure<?> handler = { Map<String, String> response ->
      if (response.error != null) {
        return mi.error("Failed OIS100MI.ChgLineBatchEnt: " + response.errorMessage)
      }
    }
    miCaller.call("OIS100MI", "ChgLineBatchEnt", parameters, handler)
  }

  /**
   *
   * @param ORNO
   * @param ITNO
   * @param ORQT
   * @param ALUN
   * @param LTYP
   * @param WHLO
   * @param DWDZ
   * @param DWHZ
   * @param ADID
   * @param PIDE
   * @param DIP4
   * @param DWDT
   * @param PLDT
   * @param SAPR
   * @return
   */
  private executeOIS100MIAddOrderLine(String ORNO, String ITNO, String ORQT, String ALUN, String LTYP, String WHLO, String DWDZ, String DWHZ, String ADID, String PIDE, String DIP4, String DWDT, String PLDT, String SAPR) {
    Map<String, String> parameters = [
      "ORNO": ORNO,
      "ITNO": ITNO,
      "ORQT": ORQT,
      "ALUN": ALUN,
      "LTYP": LTYP,
      "WHLO": WHLO,
      "DWDZ": DWDZ,
      "DWHZ": DWHZ,
      "ADID": ADID,
      "PIDE": PIDE,
      "DIP4": DIP4,
      "DWDT": DWDT,
      "PLDT": PLDT,
      "SAPR": SAPR,
      "OATP": "1",
      "IGWA": "1",
      "OSPM": "1"]
    Closure<?> handler = { Map<String, String> response ->
      logger.debug("OIS100.AddOrderLine " + response)
      if (response.error != null) {
        return mi.error("Failed OIS100MI.AddOrderLine: " + response.errorMessage)
      } else {
        newPonr = response.PONR as int
        newPosx = response.POSX as int
        logger.debug("OIS100.AddOrderLine " + newPonr)
      }
    }
    miCaller.call("OIS100MI", "AddOrderLine", parameters, handler)
  }

  /**
   *
   * @param whlo
   * @param itno
   * @param whsl
   * @param bano
   * @param CAMU
   * @param alqt
   * @param ttyp
   * @param ridn
   * @param ridl
   * @param ridx
   * @return
   */
  private executeMMS120MIUpdDetAlloc(String whlo, String itno, String whsl, String bano, String camu, String alqt, String ttyp, String ridn, String ridl, String ridx) {
    Map<String, String> parameters = ["WHLO": whlo, "ITNO": itno, "WHSL": whsl, "BANO": bano, "CAMU": camu, "ALQT": alqt, "TTYP": ttyp, "RIDN": ridn, "RIDL": ridl, "RIDX": ridx]
    logger.debug("executeMMS120MIUpdDetAlloc ORNO=${ridn} PONR=${ridl} POSX=${ridx} ALQT=${alqt} WHLO=${whlo} BANO=${bano} CAMU=${camu}")

    Closure<?> handler = { Map<String, String> response ->
      if (response.error != null) {
        return mi.error("Failed MMS120MI.UpdDetAlloc: " + response.errorMessage)
      }
    }
    miCaller.call("MMS120MI", "UpdDetAlloc", parameters, handler)
  }

  /**
   *
   */
  public void deleteExt057() {
    LocalDateTime timeOfCreation = LocalDateTime.now()
    DBAction ext057Query = database.table("EXT057").index("00").build()
    DBContainer ext057Request = ext057Query.getContainer()
    ext057Request.set("EXBJNO", bjno)

    Closure<?> ext057LReader = { DBContainer ext057Result ->
      Closure<?> ext057Deleter = { LockedResult ext057Lockresult ->
        ext057Lockresult.delete()
      }
      ext057Query.readLock(ext057Result, ext057Deleter)
    }
    if (!ext057Query.readAll(ext057Request, 1, 10000, ext057LReader)) {//todo wtf
    }
  }

  /**
   *
   */
  public void deleteExt059() {
    LocalDateTime timeOfCreation = LocalDateTime.now()
    DBAction ext059Query = database.table("EXT059").index("00").build()
    DBContainer ext059Request = ext059Query.getContainer()
    ext059Request.set("EXBJNO", bjno)

    Closure<?> ext059LReader = { DBContainer ext059Result ->
      Closure<?> ext059deleter = { LockedResult ext059LockResult ->
        ext059LockResult.delete()
      }
      ext059Query.readLock(ext059Result, ext059deleter)
    }

    if (!ext059Query.readAll(ext059Request, 1, 10000, ext059LReader)) {
    }
  }
  /**
   *
   */
  Map<String, String> loadItemUnitData(String itno, String alun) {
    Map<String, String> returnValue = [
      "ITNO"  : itno
      , "ALUN": alun
      , "DMCF": ""
      , "COFA": ""
      , "DCCD": ""
    ]

    if (itno != mitmasItno) {
      String unms = ""
      DBAction mitmasQuery = database.table("MITMAS")
        .index("00")
        .selection(
          "MMCONO"
          , "MMITNO"
          , "MMUNMS"
        )
        .build()

      DBContainer mitmasRequest = mitmasQuery.getContainer()
      mitmasRequest.set("MMCONO", currentCompany)
      mitmasRequest.set("MMITNO", itno)
      if (mitmasQuery.read(mitmasRequest)) {
        unms = mitmasRequest.get("MMUNMS")
      }
      returnValue["ITNO"] = itno
      returnValue["UNMS"] = unms
      mitmasItno = itno
      mitmasUnms = unms
    }

    if (alun == "UVC"){
      returnValue["ITNO"] = mitmasItno
      returnValue["UNMS"] = mitmasUnms
      returnValue["ALUN"] = mitmasUnms
      returnValue["DMCF"] = "1"
      returnValue["COFA"] = "1"
      returnValue["DCCD"] = "1"
      return returnValue
    }


    if (itno != mitmasItno || oolineWhlo != mitbalWhlo) {
      int almt = 0
      DBAction mitbalQuery = database.table("MITBAL")
        .index("00")
        .selection(
          "MBCONO"
          , "MBWHLO"
          , "MBITNO"
          , "MBALMT"
        )
        .build()


      DBContainer mitbalRequest = mitbalQuery.getContainer()
      mitbalRequest.set("MBCONO", currentCompany)
      mitbalRequest.set("MBWHLO", oolineWhlo)
      mitbalRequest.set("MBITNO", itno)
      if (mitbalQuery.read(mitbalRequest)) {
        almt = mitbalRequest.get("MBALMT") as Integer
      }
      mitbalWhlo = oolineWhlo
      mitbalAlmt = almt
    }

    if (alun == mitmasUnms) {
      return null
    }

    DBAction mitaunQuery = database.table("MITAUN")
      .index("00")
      .selection(
        "MUCONO"
        , "MUAUTP"
        , "MUITNO"
        , "MUALUN"
        , "MUCOFA"
        , "MUDMCF"
        , "MUDCCD"
      )
      .build()

    DBContainer mitaunRequest = mitaunQuery.getContainer()
    mitaunRequest.set("MUCONO", currentCompany)
    mitaunRequest.set("MUITNO", itno)
    mitaunRequest.set("MUAUTP", 1)
    mitaunRequest.set("MUALUN", alun)
    if (mitaunQuery.read(mitaunRequest)) {
      int dmcf = mitaunRequest.get("MUDMCF") as Integer
      double cofa = mitaunRequest.get("MUCOFA") as Double
      int dccd = mitaunRequest.get("MUDCCD") as Integer

      logger.debug("loadItemUnitData mitaun found " + mitaunRequest.get("MUDMCF") + " " + mitaunRequest.get("MUCOFA"))
      returnValue["ALUN"] = alun as String
      returnValue["DMCF"] = dmcf as String
      returnValue["COFA"] = cofa as String
      returnValue["DCCD"] = dccd as String
      return returnValue
    }


  }
}
