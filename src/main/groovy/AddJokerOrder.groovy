/**
 * README
 * This extension is used by Mashup
 *
 * Name : EXT050MI.AddJokerOrder
 * Description : Adds new Joker order.
 * Date         Changed By   Description
 * 20230526     SEAR         LOG28 - Creation of files and containers
 */
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

public class AddJokerOrder extends ExtendM3Transaction {
  private final MIAPI mi
  private final LoggerAPI logger
  private final ProgramAPI program
  private final DatabaseAPI database
  private final SessionAPI session
  private final TransactionAPI transaction
  private final MICallerAPI miCaller
  private final UtilityAPI utility
  private String parm
  private int currentCompany
  private String ornoInput
  private String whloInput
  private int ooheadOrdt
  private int ooheadCudt
  private String ooheadOrtp
  private String newOrno
  private String oolineWhlo
  private double mitaunCofa
  private int dmcf
  private String mitmasPuun

  private String jobNumber
  private Integer nbMaxRecord = 10000

  public AddJokerOrder(LoggerAPI logger, MIAPI mi, DatabaseAPI database, ProgramAPI program, MICallerAPI miCaller, UtilityAPI utility) {
    this.logger = logger
    this.mi = mi
    this.database = database
    this.program = program
    this.miCaller = miCaller
    this.utility = utility
  }

  public void main() {

    LocalDateTime timeOfCreation = LocalDateTime.now()

    currentCompany = (Integer) program.getLDAZD().CONO

    //Get mi inputs
    jobNumber = (mi.in.get("BJNO") != null ? (String) mi.in.get("BJNO") : program.getJobNumber() + timeOfCreation.format(DateTimeFormatter.ofPattern("yyMMdd")) + timeOfCreation.format(DateTimeFormatter.ofPattern("HHmmss")))
    ornoInput = (mi.in.get("ORNO") != null ? (String) mi.in.get("ORNO") : "")
    whloInput = (mi.in.get("WHLO") != null ? (String) mi.in.get("WHLO") : "")

    //Check if record exists in Table (EXT055)
    DBAction getQueryEXT055 = database.table("EXT055").index("00").selection("EXBJNO").build()
    DBContainer getContainerEXT055 = getQueryEXT055.getContainer()
    getContainerEXT055.set("EXBJNO", jobNumber)
    //Record exists
    if (!getQueryEXT055.readAll(getContainerEXT055, 1, nbMaxRecord, getEXT055)) {
      mi.error("Numéro de job " + jobNumber + " n'existe pas dans la table EXT055")
    }

    DBAction oolineQuery = database.table("OOLINE").index("00").selection("OBORNO", "OBWHLO").build()
    DBContainer oolineContainer = oolineQuery.getContainer()
    oolineContainer.set("OBCONO", currentCompany)
    oolineContainer.set("OBORNO", ornoInput)

    oolineWhlo = ""

    if (oolineQuery.readAll(oolineContainer, 2, 1, { DBContainer result ->
      oolineWhlo = result.get("OBWHLO").toString()
      logger.debug("Line WHLO : ${oolineWhlo} for ORNO : ${ornoInput}")
    })) {
    }


    DBAction ooheadQuery = database.table("OOHEAD").index("00").selection("OAORNO", "OAORDT", "OACUDT", "OACUNO", "OAROUT", "OAORTP", "OARLDT", "OAFACI", "OAWHLO", "OARESP", "OASMCD", "OATEDL", "OAMODL", "OAADID", "OACUDT", "OAUCA1", "OAUCA2", "OAUCA3", "OAUCA4", "OAUCA5", "OAUCA6", "OAUCA7", "OAUCA8", "OAUCA9", "OAUDN1", "OAUID1", "OAUID2", "OAUID3").build()
    DBContainer OOHEAD = ooheadQuery.getContainer()
    OOHEAD.set("OACONO", currentCompany)
    OOHEAD.set("OAORNO", ornoInput)
    if (!ooheadQuery.read(OOHEAD)) {
      mi.error("Le numéro de commande " + ornoInput + " n'existe pas")
      return
    } else {
      ooheadOrdt = OOHEAD.getInt("OAORDT")
      ooheadCudt = OOHEAD.getInt("OACUDT")
      ooheadOrtp = OOHEAD.get("OAORTP")

      String cuor = ornoInput + "J"

      Map<String, String> orderParams = [
        CONO: currentCompany.toString(),
        ORTP: "R01",
        CUNO: OOHEAD.get("OACUNO").toString(),
        ORDT: timeOfCreation.format(DateTimeFormatter.ofPattern("yyyyMMdd")),
        CUDT: OOHEAD.get("OACUDT").toString(),
        CUOR: cuor,
        FACI: OOHEAD.get("OAFACI").toString(),
        WHLO: OOHEAD.get("OAWHLO").toString(),
        RESP: OOHEAD.get("OARESP").toString(),
        SMCD: OOHEAD.get("OASMCD").toString(),
        TEDL: OOHEAD.get("OATEDL").toString(),
        MODL: OOHEAD.get("OAMODL").toString(),
        ADID: OOHEAD.get("OAADID").toString(),
        UCA4: OOHEAD.get("OAUCA4").toString(),
        UCA5: OOHEAD.get("OAUCA5").toString(),
        UCA6: OOHEAD.get("OAUCA6").toString(),
        UDN1: "0",
        UID1: "0",
        UID2: "0",
        UID3: "0",
        RLHM: "2200"
      ]

      executeOIS100MIAddBatchHead(orderParams)

    }

    // list out data
    DBAction listqueryEXT055 = database.table("EXT055").index("00").selection("EXBJNO", "EXITNO", "EXZQUV", "EXZPQA", "EXCOFA").build()
    DBContainer listContainerEXT055 = listqueryEXT055.getContainer()
    listContainerEXT055.set("EXBJNO", jobNumber)
    //Record exists
    if (!listqueryEXT055.readAll(listContainerEXT055, 1, nbMaxRecord, ListEXT055)) {
    }

    executeOIS100MIConfirm()

    // delete workfile
    DBAction delQuery = database.table("EXT055").index("00").build()
    DBContainer delcontainerEXT055 = delQuery.getContainer()
    delcontainerEXT055.set("EXBJNO", jobNumber)
    if (!delQuery.readAllLock(delcontainerEXT055, 1, deleteCallBack)) {
    }
  }

  // Execute OIS100MI.AddBatchHead
  private executeOIS100MIAddBatchHead(params) {
    Map<String, String> parameters = params as LinkedHashMap
    parameters.keySet().each { key ->
      logger.debug("AddBatchHead params = ${key} : " + parameters[key])
    }

    Closure<?> handlerHead = { Map<String, String> response ->
      logger.debug("in AddBatchHead wirh response : ${response}")
      if (response.error != null) {
        return mi.error("Failed OIS100MI.AddBatchHead: " + response.errorMessage)
      }
      logger.debug("New cdv has ORNO : ${newOrno}")
      newOrno = response.ORNO.trim()

    }
    miCaller.call("OIS100MI", "AddBatchHead", parameters, handlerHead)
  }

  // Execute CRS980MI.CpyOrder
  private executeOIS100MICpyOrder(String CONO, String ORNR, String ORTP, String CORH, String RLDT, String CODT) {
    Map<String, String> parameters = ["CONO": CONO, "ORNR": ORNR, "ORTP": "R01", "CORH": CORH, "RLDT": RLDT, "CODT": CODT]

    parameters.keySet().each { key ->
      logger.debug("CopyOrder params = ${key} : " + parameters[key])
    }

    Closure<?> handler = { Map<String, String> response ->
      if (response.error != null) {
        return mi.error("Failed OIS100MI.CpyOrder: " + response.errorMessage)
      }
      newOrno = response.ORNO.trim()
      logger.debug("Copied cdv has ORNO : ${newOrno}")
    }
    miCaller.call("OIS100MI", "CpyOrder", parameters, handler)
  }

  // Execute OIS100MI.Confirm
  private executeOIS100MIConfirm() {
    LinkedHashMap params = ["CONO": currentCompany.toString(), "ORNO": newOrno]

    Closure<?> handlerConfirm = { Map<String, String> response ->
      if (response.error != null) {
        return mi.error("Failed OIS100MI.Confirm: " + response.errorMessage)
      }
      logger.debug("Confirmed cdv with ORNO : ${newOrno}")
    }

    miCaller.call("OIS100MI", "Confirm", params, handlerConfirm)
  }

  // Retrieve EXT055
  Closure<?> ListEXT055 = { DBContainer containerEXT055 ->

    logger.debug("in ListEXT055")

    String itemNumber = containerEXT055.get("EXITNO")
    double quantiteUvcEXT055 = containerEXT055.getDouble("EXZQUV")
    double quantitePalEXT055 = containerEXT055.getDouble("EXZPQA")
    double facteurConversionPalEXT055 = containerEXT055.getDouble("EXCOFA")
    logger.debug("quantiteUvcEXT055 " + quantiteUvcEXT055)

    double palQuantity = quantitePalEXT055 * facteurConversionPalEXT055
    double totQuantity = 0

    totQuantity = palQuantity + quantiteUvcEXT055

    logger.debug("totQuantity " + totQuantity.toString())
    logger.debug("newOrno " + newOrno.trim().toString())
    logger.debug("itemNumber " + itemNumber.trim().toString())

    // Execute CRS980MI.
    if (totQuantity > 0) {
      executeOIS100MIAddLineBatch(currentCompany.toString(), newOrno.trim(), itemNumber.trim(), totQuantity.toString(), oolineWhlo, "UVC")
    }
  }

  // Execute OIS100MI.AddLineBatch
  private executeOIS100MIAddLineBatch(String CONO, String ORNO, String ITNO, String ORQT, String WHLO, String alun) {
    Map<String, String> parameters = ["CONO": CONO, "ORNO": ORNO, "ITNO": ITNO, "ORQT": ORQT, "WHLO": WHLO, "ALUN": alun]
    logger.debug("call OIS100MI AddBatchLine with parameters : ORNO: ${ORNO}, ITNO : ${ITNO}, ORQT: ${ORQT}, WHLO:${WHLO}, ALUN: ${alun}")
    Closure<?> handler = { Map<String, String> response ->
      if (response.error != null) {
        return mi.error("Failed OIS100MI.AddBatchLine: " + response.errorMessage)
      }
    }
    miCaller.call("OIS100MI", "AddBatchLine", parameters, handler)
  }

  // Retrieve EXT055
  Closure<?> getEXT055 = { DBContainer containerEXT055 ->
    return
  }

  // Delete
  Closure<?> deleteCallBack = { LockedResult lockedResult ->
    lockedResult.delete()
  }
}
