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

  private long saved_DLIX
  private Integer currentCompany

  private HashMap<String, String> blockedIndexes
  private String newDelivery

  private String OOLINE_ORNO
  private String OOLINE_ITNO
  private double OOLINE_ORQT
  private String OOLINE_ALUN
  private String OOLINE_LTYP
  private String OOLINE_WHLO
  private String OOLINE_DWDZ
  private String OOLINE_DWHZ
  private String OOLINE_ADID
  private String OOLINE_PIDE
  private String OOLINE_DIP4
  private String OOLINE_DWDT
  private String OOLINE_PLDT
  private String OOLINE_SAPR
  private Integer OOLINE_PONR
  private Integer OOLINE_POSX
  private String OOLINE_RORC
  private String OOLINE_RORN
  private Integer OOLINE_RORL
  private Integer OOLINE_RORX

  private String MITMAS_ITNO
  private String MITMAS_UNMS
  private String MITBAL_WHLO
  private int MITBAL_ALMT

  private double EXT057_ALQT
  private int NEW_PONR
  private int NEW_POSX
  private LinkedHashMap<String, Object> itemUnits


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
   * TODO DOC
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

    /* todo reactivate
    deleteEXT057()
    deleteEXT059()
    */

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
      itemUnits = new LinkedHashMap<String, Object>()
    } else if (!itemUnits.containsKey(key)) {
      load = true
    }
    if (load)
      itemUnits.put(key, loadItemUnitData(itno, alun))

    def currentItemUnit = itemUnits.get(key)

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
    double currentALQT = 0
    String currentUCA4 = ""
    String currentUCA5 = ""
    String currentUCA6 = ""
    String currentCAMU = ""

    String orno = ""
    String ponr = ""
    String posx = ""



    // Read MITALO and add order lines into EXT057
    DBAction EXT059_query = database.table("EXT059").index("00").selection(
      "EXCONO"
      , "EXCAMU"
      , "EXTLIX"
      , "EXUCA4"
      , "EXUCA5"
      , "EXUCA6"
    ).build()

    DBContainer EXT059_request = EXT059_query.getContainer()
    EXT059_request.set("EXBJNO", bjno)
    EXT059_request.set("EXCONO", currentCompany)


    // Update EXT057
    Closure<?> EXT057_updater = { LockedResult EXT057_lockedresult ->
      LocalDateTime timeOfCreation = LocalDateTime.now()
      int changeNumber = EXT057_lockedresult.get("EXCHNO") as Integer
      String cams = EXT057_lockedresult.get("EXCAMS") as String
      double allocatedQuantity = EXT057_lockedresult.get("EXALQT") as Double
      //double qty_UPA = convertQty(OOLINE_ITNO, "UPA", currentALQT, 0)
      allocatedQuantity = allocatedQuantity + currentALQT

      logger.debug("palletProcess update EXT057 orno:${orno} ponr:${ponr} posx:${posx} qtyUPA:=${allocatedQuantity}")
      EXT057_lockedresult.set("EXALQT", allocatedQuantity)
      EXT057_lockedresult.setInt("EXLMDT", timeOfCreation.format(DateTimeFormatter.ofPattern("yyyyMMdd")) as Integer)
      EXT057_lockedresult.setInt("EXCHNO", changeNumber + 1)

      if (!cams.contains(currentCAMU))
        EXT057_lockedresult.set("EXCAMS", cams + ";" + currentCAMU)

      EXT057_lockedresult.set("EXCHID", program.getUser())
      EXT057_lockedresult.update()
    }


    // Retrieve MITALO
    Closure<?> MITALO_reader = { DBContainer MITALO ->
      currentALQT = MITALO.get("MQALQT") as double
      currentCAMU = MITALO.get("MQCAMU") as String
      orno = MITALO.get("MQRIDN")
      ponr = MITALO.get("MQRIDL")
      posx = MITALO.get("MQRIDX")

      logger.debug("found MITALO - orno:${orno} ponr:${ponr} posx:${posx} camu:${currentCAMU} alqt:${currentALQT}")
      LocalDateTime timeOfCreation = LocalDateTime.now()

      if (checkOOHEAD(MITALO.get("MQRIDN") as String, currentUCA4, currentUCA5, currentUCA6)) {

        getOOLINE_DATA(MITALO.get("MQRIDN") as String, MITALO.get("MQRIDL") as String, MITALO.get("MQRIDX") as String)
        DBAction EXT057_query = database.table("EXT057").index("00").selection("EXCAMS", "EXALQT", "EXALUN").build()
        DBContainer EXT057_request = EXT057_query.getContainer()

        EXT057_request.set("EXBJNO", bjno)
        EXT057_request.set("EXCONO", currentCompany)
        EXT057_request.set("EXORNO", orno)
        EXT057_request.set("EXPONR", ponr as Integer)
        EXT057_request.set("EXPOSX", posx as Integer)
        if (!EXT057_query.read(EXT057_request)) {
          logger.debug("palletProcess create EXT057 orno:${orno} ponr:${ponr} posx:${posx} currentALQT:=${currentALQT}")
          EXT057_request.set("EXALQT", currentALQT)
          EXT057_request.set("EXALUN", "")
          EXT057_request.setInt("EXRGDT", timeOfCreation.format(DateTimeFormatter.ofPattern("yyyyMMdd")) as Integer)
          EXT057_request.setInt("EXRGTM", timeOfCreation.format(DateTimeFormatter.ofPattern("HHmmss")) as Integer)
          EXT057_request.setInt("EXLMDT", timeOfCreation.format(DateTimeFormatter.ofPattern("yyyyMMdd")) as Integer)
          EXT057_request.setInt("EXCHNO", 1)
          EXT057_request.set("EXCAMS", currentCAMU)
          EXT057_request.set("EXCHID", program.getUser())
          EXT057_request.set("EXDLIX", getDLIX(3, MITALO.get("MQRIDN") as String, MITALO.get("MQRIDL") as int, MITALO.get("MQRIDX") as int))
          EXT057_query.insert(EXT057_request)
        } else {
          EXT057_request.set("EXBJNO", bjno)
          EXT057_request.set("EXCONO", currentCompany)
          EXT057_request.set("EXORNO", MITALO.get("MQRIDN"))
          EXT057_request.set("EXPONR", MITALO.get("MQRIDL"))
          EXT057_request.set("EXPOSX", MITALO.get("MQRIDX"))
          if (EXT057_query.readLock(EXT057_request, EXT057_updater)) {
          }
        }
      }
    }

    //Reader EXT059 record
    Closure<?> EXT059_reader = { DBContainer EXT059_result ->
      String camu = EXT059_result.get("EXCAMU")
      logger.debug("EXT059_result " + camu)
      currentUCA4 = EXT059_result.get("EXUCA4") as String
      currentUCA5 = EXT059_result.get("EXUCA5") as String
      currentUCA6 = EXT059_result.get("EXUCA6") as String

      //find corresponding allocation in MITALO by CONO, TTYP = 31 and CAMU
      ExpressionFactory MITALO_expression = database.getExpressionFactory("MITALO")
      MITALO_expression = MITALO_expression.eq("MQCAMU", camu)
      DBAction MITALO_query = database.table("MITALO")
        .index("10").matching(MITALO_expression)
        .selection(
          "MQCAMU",
          "MQRIDN",
          "MQRIDL",
          "MQRIDX", "" +
          "MQALQT").build()
      DBContainer MITALO = MITALO_query.getContainer()
      MITALO.set("MQCONO", currentCompany)
      MITALO.set("MQTTYP", 31)
      if (MITALO_query.readAll(MITALO, 2, MITALO_reader)) {
      }
    }

    if (!EXT059_query.readAll(EXT059_request, 2, EXT059_reader)) {
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
  public long getDLIX(int rorc, String ridn, int ridl, int ridx) {
    long dlix = 0

    DBAction MHDISL_query = database.table("MHDISL").index("10").build()
    DBContainer MHDISL_request = MHDISL_query.getContainer()
    MHDISL_request.set("URCONO", currentCompany)
    MHDISL_request.set("URRORC", rorc)
    MHDISL_request.set("URRIDN", ridn)
    MHDISL_request.set("URRIDL", ridl)
    MHDISL_request.set("URRIDX", ridx)

    Closure<?> closure_MHDISL = { DBContainer MHDISL_result ->
      if (dlix == 0)
        dlix = MHDISL_result.get("URDLIX") as long
    }
    if (!MHDISL_query.readAll(MHDISL_request, 5, closure_MHDISL)) {
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
    ExpressionFactory expression_EXT057 = database.getExpressionFactory("EXT057")
    /*TODO DEL
    if (completeLineProcess) {
      expression_EXT057 = expression_EXT057.eq("EXALQT", "0")
    } else {
      expression_EXT057 = expression_EXT057.ne("EXALQT", "0")
    }*/
    DBAction query_EXT057 = database.table("EXT057").index("10")
      .matching(expression_EXT057)
      .selection("EXCONO",
        "EXORNO",
        "EXPONR",
        "EXPOSX",
        "EXDLIX",
        "EXALQT",
        "EXALUN",
        "EXTLIX").build()
    DBContainer EXT057 = query_EXT057.getContainer()
    EXT057.set("EXBJNO", bjno)
    if (!query_EXT057.readAll(EXT057, 1, outData_EXT057_lineTransfer)) {
    }
  }

  /**
   * TODO Comment
   */
  public void prepareTransfer() {
    ExpressionFactory expression_EXT057 = database.getExpressionFactory("EXT057")
    DBAction query_EXT057 = database
      .table("EXT057")
      .index("10")
      .matching(expression_EXT057)
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
    DBContainer EXT057 = query_EXT057.getContainer()
    EXT057.set("EXBJNO", bjno)
    if (!query_EXT057.readAll(EXT057, 1, outData_EXT057_prepareTransfer)) {
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
    DBAction MHDISH_query = database.table("MHDISH")
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

    DBContainer MHDISH_request = MHDISH_query.getContainer()
    MHDISH_request.set("OQCONO", currentCompany)
    MHDISH_request.set("OQINOU", 1)
    MHDISH_request.set("OQDLIX", dlix as Long)

    if (MHDISH_query.read(MHDISH_request)) {
      cona = MHDISH_request.get("OQCONA") as String
      coaa = MHDISH_request.get("OQCOAA") as String
      agky = MHDISH_request.get("OQAGKY") as String
      dcc1 = MHDISH_request.get("OQDCC1") as String
    }

    //
    //  Load relative indexes
    //
    ExpressionFactory MHDISH2_expression = database.getExpressionFactory("MHDISH")
    MHDISH2_expression = MHDISH2_expression.eq("OQCOAA", coaa)
    MHDISH2_expression = MHDISH2_expression.and(MHDISH2_expression.eq("OQAGKY", agky))
    MHDISH2_expression = MHDISH2_expression.and(MHDISH2_expression.eq("OQDCC1", dcc1))
    MHDISH2_expression = MHDISH2_expression.and(MHDISH2_expression.lt("OQPGRS", '90'))

    DBAction MHDISH2_query = database.table("MHDISH")
      .matching(MHDISH2_expression)
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
    DBContainer MHDISH2_request = MHDISH_query.getContainer()
    MHDISH2_request.set("OQCONO", currentCompany)
    MHDISH_request.set("OQINOU", 1)
    MHDISH_request.set("OQCONA", cona)


    Closure<?> MHDISH2_reader = { DBContainer MHDISH2_result ->
      String curdlix = MHDISH2_result.get("OQDLIX") as String
      logger.debug("curdlix:${curdlix}")
      if (curdlix != tlix) {
        blockedIndexes.put(curdlix, curdlix)
        blopunblopIndex(curdlix, 1)
      }
    }

    //Launch query
    if (!MHDISH2_query.readAll(MHDISH2_request, 3, MHDISH2_reader)) {
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
  Closure<?> outData_EXT057_lineTransfer = { DBContainer EXT057 ->
    String dlix = EXT057.get("EXDLIX")
    String tlix = EXT057.get("EXTLIX")
    String rorc = "3"
    String ridn = EXT057.get("EXORNO")
    String ridl = EXT057.get("EXPONR")
    String ridx = EXT057.get("EXPOSX")
    double alqt = EXT057.get("EXALQT") as double
    String alun = EXT057.get("EXALUN")



    if (getOOLINE_DATA(ridn, ridl, ridx)) {
      logger.debug("closure outData_EXT057_lineTransfer : DLIX=${dlix}, TLIX=${tlix} RIDN=${ridn} RIDL=${ridl} RIDX=${ridx}")
      if (alqt != 0)
        alqt = convertQty(OOLINE_ITNO, alun, 0, alqt)

      logger.debug("TETS DDD alqt:${alqt} orqt:${OOLINE_ORQT}")

      if (alqt == OOLINE_ORQT || alqt == 0) {
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
  public checkOOHEAD(String orno, String uca4, String uca5, String uca6) {
    logger.debug("check OOHEAD orno=${orno} uca4=${uca4} uca5=${uca5} uca6=${uca6}")
    DBAction OOHEAD_query = database.table("OOHEAD").index("00").selection(
      "OAOBLC",
      "OAHOCD",
      "OAJNA",
      "OAJNU",
      "OAUCA4",
      "OAUCA5",
      "OAUCA6",
      "OACHID").build()
    DBContainer OOHEAD_request = OOHEAD_query.getContainer()
    OOHEAD_request.set("OACONO", currentCompany)
    OOHEAD_request.set("OAORNO", orno)
    if (OOHEAD_query.read(OOHEAD_request)) {
      String oaoblc = OOHEAD_request.get("OAOBLC") as String
      String oahocd = OOHEAD_request.get("OAHOCD") as String
      String oajna = OOHEAD_request.get("OAJNA") as String
      String oajnu = OOHEAD_request.get("OAJNU") as String
      String oachid = OOHEAD_request.get("OACHID") as String
      String oauca4 = OOHEAD_request.get("OAUCA4") as String
      String oauca5 = OOHEAD_request.get("OAUCA5") as String
      String oauca6 = OOHEAD_request.get("OAUCA6") as String
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
  public boolean getOOLINE_DATA(String orno, String ponr, String posx) {
    logger.debug(String.format("method getOOLINE_DATA : ORNO=%s, PONR=%s, POSX=%s ", orno, ponr, posx))
    if (OOLINE_ORNO == orno && OOLINE_PONR && ponr && OOLINE_POSX == posx) {
      return true
    }


    logger.debug(String.format("method getOOLINE_DATA : ORNO=%s, PONR=%s, POSX=%s ", orno, ponr, posx))
    DBAction OOHEAD_query = database.table("OOHEAD").index("00").selection(
      "OAOBLC",
      "OAHOCD",
      "OAJNA",
      "OAJNU",
      "OACHID").build()
    DBContainer OOHEAD_request = OOHEAD_query.getContainer()
    OOHEAD_request.set("OACONO", currentCompany)
    OOHEAD_request.set("OAORNO", orno)
    if (OOHEAD_query.read(OOHEAD_request)) {
      String oaoblc = OOHEAD_request.get("OAOBLC") as String
      String oahocd = OOHEAD_request.get("OAHOCD") as String
      String oajna = (OOHEAD_request.get("OAJNA") as String).trim()
      String oajnu = (OOHEAD_request.get("OAJNU") as String).trim()
      String oachid = (OOHEAD_request.get("OACHID") as String).trim()
      if (oaoblc.equals("1") || oahocd.equals("1")) {
        in60 = true
        msgd = "La commande ${orno} est bloquée par ${oachid} programme ${oajna} / ${oajnu}"
        logger.debug(msgd)
        return false
      }
    }

    logger.debug(String.format("method read line getOOLINE_DATA : ORNO=%s, PONR=%s, POSX=%s ", orno, ponr, posx))
    DBAction OOLINE_query = database.table("OOLINE").index("00").selection(
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

    DBContainer OOLINE = OOLINE_query.getContainer()
    OOLINE.set("OBCONO", currentCompany)
    OOLINE.set("OBORNO", orno)
    OOLINE.set("OBPONR", Integer.parseInt(ponr))
    OOLINE.set("OBPOSX", Integer.parseInt(posx))
    if (OOLINE_query.read(OOLINE)) {
      OOLINE_ORNO = OOLINE.get("OBORNO")
      OOLINE_PONR = OOLINE.get("OBPONR") as Integer
      OOLINE_POSX = OOLINE.get("OBPOSX") as Integer
      OOLINE_RORC = OOLINE.get("OBRORC")
      OOLINE_RORN = OOLINE.get("OBRORN")
      OOLINE_RORL = OOLINE.get("OBRORL") as Integer
      OOLINE_RORX = OOLINE.get("OBRORX") as Integer
      OOLINE_ITNO = OOLINE.get("OBITNO")
      OOLINE_ORQT = OOLINE.get("OBORQT") as Double
      OOLINE_ALUN = OOLINE.get("OBALUN")
      OOLINE_LTYP = OOLINE.get("OBLTYP")
      OOLINE_WHLO = OOLINE.get("OBWHLO")
      OOLINE_DWDZ = OOLINE.get("OBDWDZ")
      OOLINE_DWHZ = OOLINE.get("OBDWHZ")
      OOLINE_ADID = OOLINE.get("OBADID")
      OOLINE_PIDE = OOLINE.get("OBPIDE")
      OOLINE_DIP4 = OOLINE.get("OBDIP4")
      OOLINE_DWDT = OOLINE.get("OBDWDT")
      OOLINE_PLDT = OOLINE.get("OBPLDT")
      OOLINE_SAPR = OOLINE.get("OBSAPR")
      logger.debug("OOLINE_RORL:${OOLINE_RORL} OOLINE_RORC:${OOLINE_RORC}")
      return true
    }
    logger.debug(String.format("closure getOOLINE_DATA NOT FOUND : cono=%s ORNO=%s, PONR=%s, POSX=%s ", currentCompany + "", orno, ponr, posx))
    return false
  }
  /**
   * Execute MWS411MI.MoveDelLn
   */
  private executeMWS411MIMoveDelLn(String dlix, String rorc, String ridn, String ridl, String ridx, String tlix) {
    def parameters = ["DLIX": dlix, "RORC": rorc, "RIDN": ridn, "RIDL": ridl, "RIDX": ridx, "TLIX": tlix]
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
    logger.debug("blopunblopIndex dlix:${index} blop:${blop}")
    if (index == 0 || !(blop == 0 || blop == 1)) {
      return
    }


    DBAction query_MHDISH = database.table("MHDISH").index("00").selection("OQDLIX", "OQBLOP").build()
    DBContainer MHDISH = query_MHDISH.getContainer()
    MHDISH.set("OQCONO", currentCompany)
    MHDISH.set("OQINOU", 1)
    MHDISH.set("OQDLIX", index as Long)

    Closure<?> MHDISH_updater = { LockedResult MHDISH_lockedResult ->
      MHDISH_lockedResult.set("OQBLOP", blop)
      MHDISH_lockedResult.update()
    }

    if (query_MHDISH.readLock(MHDISH, MHDISH_updater)) {
    }
  }

  /**
   * Loop ON EXT057
   *
   */
  Closure<?> outData_EXT057_prepareTransfer = { DBContainer EXT057 ->
    logger.debug(String.format("closure outData_EXT057_prepareTransfer : ORNO=%s, PONR=%s, POSX=%s ", EXT057.get("EXORNO"), EXT057.get("EXPONR"), EXT057.get("EXPOSX")))
    String orno = EXT057.get("EXORNO") as String
    String ponr = EXT057.get("EXPONR") as String
    String posx = EXT057.get("EXPOSX") as String
    double alqt = EXT057.get("EXALQT") as Double
    String alun = EXT057.get("EXALUN") as String


    if (getOOLINE_DATA(EXT057.getString("EXORNO"), "" + EXT057.get("EXPONR"), "" + EXT057.get("EXPOSX"))) {
      // CONVERT CONVERSION PAL to UMNS
      EXT057_ALQT = convertQty(OOLINE_ITNO, alun, 0, alqt)
      logger.debug("RETEST DDD alqt:${EXT057_ALQT} orqt:${OOLINE_ORQT}")

      if (OOLINE_ORQT != EXT057_ALQT)
        transferLINE_Partial(EXT057.get("EXDLIX") as String, EXT057.get("EXTLIX") as String, EXT057.get("EXCAMS") as String)
    }
  }


  /**
   * break link between CO & PO
   */
  public void transferLINE_Partial(String dlix, String tlix, String cams) {
    logger.debug("method breakLinkedOrders")

    //Save MITPLO and MITALO RECORDS
    LinkedList<Object> mitplos = new LinkedList<Object>()
    DBAction MITPLO_query = database.table("MITPLO").index("10").selection(
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
    DBContainer MITPLO_request = MITPLO_query.getContainer()
    MITPLO_request.set("MOCONO", currentCompany)
    MITPLO_request.set("MOORCA", "311")
    MITPLO_request.set("MORIDN", OOLINE_ORNO)
    MITPLO_request.set("MORIDL", OOLINE_PONR)
    MITPLO_request.set("MORIDX", OOLINE_POSX)

    Closure<?> MITPLO_reader = { DBContainer MITPLO_result ->
      //Define return object structure
      def mitplo_data = [
        "MOWHLO"  : "" + MITPLO_result.get("MOWHLO")
        , "MOITNO": "" + MITPLO_result.get("MOITNO")
        , "MOPLDT": "" + MITPLO_result.get("MOPLDT")
        , "MOTIHM": "" + MITPLO_result.get("MOTIHM")
        , "MOORCA": "" + MITPLO_result.get("MOORCA")
        , "MORIDN": "" + MITPLO_result.get("MORIDN")
        , "MORIDL": "" + MITPLO_result.get("MORIDL")
        , "MORIDX": "" + MITPLO_result.get("MORIDX")
        , "MORIDI": "" + MITPLO_result.get("MORIDI")
        , "MOSTAT": "" + MITPLO_result.get("MOSTAT")
        , "MOALQT": "" + MITPLO_result.get("MOALQT")
      ]
      mitplos.add(mitplo_data)
    }
    MITPLO_query.readAll(MITPLO_request, 5, MITPLO_reader)


    //Save MITALO RECORDS
    LinkedList<Object> mitalos = new LinkedList<Object>()
    DBAction MITALO_query = database.table("MITALO").index("10").selection(
      "MQWHLO",
      "MQITNO",
      "MQWHSL",
      "MQBANO",
      "MQCAMU",
      "MQREPN",
      "MQALQT").build()
    DBContainer MITALO_request = MITALO_query.getContainer()
    MITALO_request.set("MQCONO", currentCompany)
    MITALO_request.set("MQTTYP", 31)
    MITALO_request.set("MQRIDN", OOLINE_ORNO)
    MITALO_request.set("MQRIDO", 0)
    MITALO_request.set("MQRIDL", OOLINE_PONR)
    MITALO_request.set("MQRIDX", OOLINE_POSX)

    Closure<?> MITALO_reader = { DBContainer MITALO_result ->
      //Define return object structure
      def mitalo_data = [
        "MQWHLO"  : "" + MITALO_result.get("MQWHLO")
        , "MQITNO": "" + MITALO_result.get("MQITNO")
        , "MQWHSL": "" + MITALO_result.get("MQWHSL")
        , "MQBANO": "" + MITALO_result.get("MQBANO")
        , "MQCAMU": "" + MITALO_result.get("MQCAMU")
        , "MQTTYP": "" + MITALO_result.get("MQTTYP")
        , "MQRIDN": "" + MITALO_result.get("MQRIDN")
        , "MQRIDO": "" + MITALO_result.get("MQRIDO")
        , "MQRIDL": "" + MITALO_result.get("MQRIDL")
        , "MQRIDX": "" + MITALO_result.get("MQRIDX")
        , "MQRIDI": "" + MITALO_result.get("MQRIDI")
        , "MQPLSX": "" + MITALO_result.get("MQPLSX")
        , "MQSOFT": "" + MITALO_result.get("MQSOFT")
        , "MQALQT": "" + MITALO_result.get("MQALQT")
      ]
      mitalos.add(mitalo_data)
    }

    MITALO_query.readAll(MITALO_request, 6, MITALO_reader)


    logger.debug("OOLINE_RORL:${OOLINE_RORL} OOLINE_RORC:${OOLINE_RORC}")
    // Break link with purchase order
    if (OOLINE_RORC.trim() == "2") {
      // Remove link from purchase order line
      executePPS200MIUpdLine(OOLINE_RORN, "" + OOLINE_RORL, "" + OOLINE_RORX, "0", "0", "0", "0")
    }

    //deallocation
    if (MITBAL_ALMT != 6 && MITBAL_ALMT != 7)
      executeMMS120MIDeAllocateOrLne("31", OOLINE_ORNO, "" + OOLINE_PONR, "" + OOLINE_POSX, "M")

    //Reduce Qty on original line
    logger.debug("calc new orqa ")
    double new_orqa = convertQty(OOLINE_ITNO, OOLINE_ALUN, OOLINE_ORQT - EXT057_ALQT, 0)
    logger.debug("calc new orqa ${new_orqa}")
    blockRelativeIndexes(dlix, dlix)
    executeOIS100MIChgLineBatchEnt(OOLINE_ORNO, "" + OOLINE_PONR, "" + OOLINE_POSX, OOLINE_RORN, new_orqa as String)
    unblockingIndexes()

    //recreate link with po line
    if (OOLINE_RORC.trim() == "2") {
      executePPS200MIUpdLine(OOLINE_RORN, "" + OOLINE_RORL, "" + OOLINE_RORX, "3", OOLINE_ORNO, "" + OOLINE_PONR, "" + OOLINE_POSX)
    }
    String theDLIX = tlix.length() > 0 && tlix != "0" ? tlix : newDelivery
    blockRelativeIndexes(dlix, theDLIX)
    //Create new co line
    if (EXT057_ALQT > 0) {
      NEW_PONR = 0
      NEW_POSX = 0
      logger.debug("calc new line orqa ")
      //new_orqa = convertQty(OOLINE_ITNO, OOLINE_ALUN, EXT057_ALQT, 0)
      //logger.debug("calc new line orqa:${new_orqa}")
      executeOIS100MIAddOrderLine(OOLINE_ORNO, OOLINE_ITNO, EXT057_ALQT as String, 'UVC', "0", OOLINE_WHLO, OOLINE_DWDZ, OOLINE_DWHZ, OOLINE_ADID, OOLINE_PIDE, OOLINE_DIP4, OOLINE_DWDT, OOLINE_PLDT, OOLINE_SAPR)
    }
    unblockingIndexes()

    //realoc
    double qte_original = OOLINE_ORQT
    double qte_new_line = EXT057_ALQT
    double qte_old_line = OOLINE_ORQT - EXT057_ALQT

    if (MITBAL_ALMT != 6 && MITBAL_ALMT != 7) {
      for (def mitalo in mitalos) {
        String mitalo_itno = mitalo["MQITNO"] as String
        String mitalo_bano = mitalo["MQBANO"] as String
        String mitalo_camu = mitalo["MQCAMU"] as String
        double mitalo_alqt = mitalo["MQALQT"] as Double

        double alqt = 0
        logger.debug("affectation itno:${mitalo_itno} bano:${mitalo_bano} camu:${mitalo_camu} qte_old_line:${qte_old_line} qte_new_line:${qte_new_line} cams:${cams}")
        if (qte_old_line > 0 && (!cams.contains(mitalo_camu) || cams.trim() == "")) {
          alqt = qte_old_line <= mitalo_alqt ? qte_old_line : mitalo_alqt
          logger.debug("affectation oldline itno:${mitalo_itno} bano:${mitalo_bano} camu:${mitalo_camu} qte_old_line:${qte_old_line} qte_new_line:${qte_new_line} cams:${cams}")
          executeMMS120MIUpdDetAlloc(
            mitalo["MQWHLO"] as String
            , mitalo["MQITNO"] as String
            , mitalo["MQWHSL"] as String
            , mitalo["MQBANO"] as String
            , mitalo["MQCAMU"] as String
            , alqt as String
            , "31"
            , OOLINE_ORNO
            , OOLINE_PONR as String
            , OOLINE_POSX as String)
          qte_old_line -= alqt
          mitalo_alqt -= alqt
        }
        if (qte_new_line > 0 &&  (cams.contains(mitalo_camu) || cams.trim() == "")) {
          alqt = qte_new_line <= mitalo_alqt ? qte_new_line : mitalo_alqt
          logger.debug("affectation newline itno:${mitalo_itno} bano:${mitalo_bano} camu:${mitalo_camu} qte_old_line:${qte_old_line} qte_new_line:${qte_new_line} cams:${cams}")
          if (alqt > 0) {
            executeMMS120MIUpdDetAlloc(
              mitalo["MQWHLO"] as String
              , mitalo["MQITNO"] as String
              , mitalo["MQWHSL"] as String
              , mitalo["MQBANO"] as String
              , mitalo["MQCAMU"] as String
              , alqt as String
              , "31"
              , OOLINE_ORNO
              , NEW_PONR as String
              , NEW_POSX as String)
            mitalo_alqt -= alqt
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
    def parameters = ["TTYP": TTYP, "RIDN": RIDN, "RIDL": RIDL, "RIDX": RIDX, "MAAL": MAAL]
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
    /* LINE DISABLED BECAUSE PPS200MI non compatible EXTENDSM3
     * def parameters = ["PUNO": PUNO, "PNLI": PNLI, "PNLS": PNLS, "RORC": RORC, "RORN": RORN, "RORL": RORL, "RORX": RORX]
     Closure<?> handler = { Map<String, String> response ->
     if (response.error != null) {
     return mi.error("Failed PPS200MI.UpdLine: "+ response.errorMessage)
     }
     }
     miCaller.call("PPS200MI", "UpdLine", parameters, handler)*/
    //CALL PPS200MI tru IONAPI
    logger.debug("executePPS200MIUpdLine : ligne OA ${PUNO} ${PNLI} ${PNLS} ligne CDV ${RORC} ${RORL} ${RORX}")
    def endpoint = "/M3/m3api-rest/v2/execute/PPS200MI/UpdLine"
    def headers = ["Accept": "application/json"]
    def queryParameters = [
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
    def parameters = ["ORNO": ORNO, "PONR": PONR, "POSX": POSX, "UCA2": UCA2, "ORQA": ORQA]
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
    def parameters = [
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
        NEW_PONR = response.PONR as int
        NEW_POSX = response.POSX as int
        logger.debug("OIS100.AddOrderLine " + NEW_PONR)
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
    def parameters = ["WHLO": whlo, "ITNO": itno, "WHSL": whsl, "BANO": bano, "CAMU": camu, "ALQT": alqt, "TTYP": ttyp, "RIDN": ridn, "RIDL": ridl, "RIDX": ridx]
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
  public void deleteEXT057() {
    LocalDateTime timeOfCreation = LocalDateTime.now()
    DBAction EXT057_query = database.table("EXT057").index("00").build()
    DBContainer EXT057_request = EXT057_query.getContainer()
    EXT057_request.set("EXBJNO", bjno)

    Closure<?> EXT057_deleter = { LockedResult EXT057_result ->
      EXT057_result.delete()
    }


    if (!EXT057_query.readAllLock(EXT057_request, 1, EXT057_deleter)) {
    }
  }

  /**
   *
   */
  public void deleteEXT059() {
    LocalDateTime timeOfCreation = LocalDateTime.now()
    DBAction EXT059_query = database.table("EXT059").index("00").build()
    DBContainer EXT059_request = EXT059_query.getContainer()
    EXT059_request.set("EXBJNO", bjno)

    Closure<?> EXT059_deleter = { LockedResult EXT059_result ->
      EXT059_result.delete()
    }


    if (!EXT059_query.readAllLock(EXT059_request, 1, EXT059_deleter)) {
    }
  }
  /**
   *
   */
  def loadItemUnitData(String itno, String alun) {
    def returnValue = [
      "ITNO"  : itno
      , "ALUN": alun
      , "DMCF": ""
      , "COFA": ""
      , "DCCD": ""
    ]

    if (itno != MITMAS_ITNO) {
      String unms = ""
      DBAction MITMAS_query = database.table("MITMAS")
        .index("00")
        .selection(
          "MMCONO"
          , "MMITNO"
          , "MMUNMS"
        )
        .build()

      DBContainer MITMAS_request = MITMAS_query.getContainer()
      MITMAS_request.set("MMCONO", currentCompany)
      MITMAS_request.set("MMITNO", itno)
      if (MITMAS_query.read(MITMAS_request)) {
        unms = MITMAS_request.get("MMUNMS")
      }
      returnValue["ITNO"] = itno
      returnValue["UNMS"] = unms
      MITMAS_ITNO = itno
      MITMAS_UNMS = unms
    }

    if (alun == "UVC"){
      returnValue["ITNO"] = MITMAS_ITNO
      returnValue["UNMS"] = MITMAS_UNMS
      returnValue["ALUN"] = MITMAS_UNMS
      returnValue["DMCF"] = "1"
      returnValue["COFA"] = "1"
      returnValue["DCCD"] = "1"
      return returnValue
    }


    if (itno != MITMAS_ITNO || OOLINE_WHLO != MITBAL_WHLO) {
      int almt = 0
      DBAction MITBAL_query = database.table("MITBAL")
        .index("00")
        .selection(
          "MBCONO"
          , "MBWHLO"
          , "MBITNO"
          , "MBALMT"
        )
        .build()


      DBContainer MITBAL_request = MITBAL_query.getContainer()
      MITBAL_request.set("MBCONO", currentCompany)
      MITBAL_request.set("MBWHLO", OOLINE_WHLO)
      MITBAL_request.set("MBITNO", itno)
      if (MITBAL_query.read(MITBAL_request)) {
        almt = MITBAL_request.get("MBALMT") as Integer
      }
      MITBAL_WHLO = OOLINE_WHLO
      MITBAL_ALMT = almt
    }

    if (alun == MITMAS_UNMS) {
      return null
    }

    DBAction MITAUN_query = database.table("MITAUN")
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

    DBContainer MITAUN_request = MITAUN_query.getContainer()
    MITAUN_request.set("MUCONO", currentCompany)
    MITAUN_request.set("MUITNO", itno)
    MITAUN_request.set("MUAUTP", 1)
    MITAUN_request.set("MUALUN", alun)
    if (MITAUN_query.read(MITAUN_request)) {
      int dmcf = MITAUN_request.get("MUDMCF") as Integer
      double cofa = MITAUN_request.get("MUCOFA") as Double
      int dccd = MITAUN_request.get("MUDCCD") as Integer

      logger.debug("loadItemUnitData mitaun found " + MITAUN_request.get("MUDMCF") + " " + MITAUN_request.get("MUCOFA"))
      returnValue["ALUN"] = alun as String
      returnValue["DMCF"] = dmcf as String
      returnValue["COFA"] = cofa as String
      returnValue["DCCD"] = dccd as String
      return returnValue
    }


  }
}
