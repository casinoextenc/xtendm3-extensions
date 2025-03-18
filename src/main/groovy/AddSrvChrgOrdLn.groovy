/**
 * README
 * This extension is used by EventHub
 *
 * Name : EXT062MI.AddSrvChrgOrdLn
 * Description : Add line to service order charge
 * Date         Changed By   Description
 * 20231124     RENARN       CMD03 - Calculation of service charges
 */
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

public class AddSrvChrgOrdLn extends ExtendM3Transaction {
  private final MIAPI mi
  private final DatabaseAPI database
  private final LoggerAPI logger
  private final MICallerAPI miCaller
  private final ProgramAPI program
  private final UtilityAPI utility
  private int currentCompany
  private String currentDate
  private String inORNO
  private Integer inPONR
  private Integer inPOSX
  private long inDLIX
  private String inWHLO
  private String inTEPY
  private String itno
  private String orqt
  private String sapr
  private String alun
  private String dwdt
  private String existingServiceChargeOrderOrno
  private String newORNO
  private Integer newPONR
  private Integer newPOSX
  private String orst
  private Integer chb6
  private boolean serviceChargeOrderLineExists
  private Integer nbMaxRecord = 10000

  public AddSrvChrgOrdLn(MIAPI mi, DatabaseAPI database, LoggerAPI logger, ProgramAPI program, UtilityAPI utility, MICallerAPI miCaller) {
    this.mi = mi
    this.database = database
    this.logger = logger
    this.program = program
    this.utility = utility
    this.miCaller = miCaller
  }

  public void main() {
    if (mi.in.get("CONO") == null) {
      currentCompany = (Integer) program.getLDAZD().CONO
    } else {
      currentCompany = mi.in.get("CONO")
    }

    // Get current date
    LocalDateTime timeOfCreation = LocalDateTime.now()
    currentDate = timeOfCreation.format(DateTimeFormatter.ofPattern("yyyyMMdd"))

    inORNO = ""
    if (mi.in.get("ORNO") != null && mi.in.get("ORNO") != "") {
      inORNO = mi.in.get("ORNO")
    } else {
      mi.error("Numéro commande de vente est obligatoire")
      return
    }
    inPONR = 0
    if (mi.in.get("PONR") != null && mi.in.get("PONR") != "") {
      if (utility.call("NumberUtil", "isValidNumber", mi.in.get("PONR") as String, ".")) {
        inPONR = mi.in.get("PONR") as Integer
      } else {
        mi.error("Format numérique ligne est incorrect")
        return
      }
    } else {
      mi.error("Numéro de ligne est obligatoire")
      return
    }
    inPOSX = 0
    if (mi.in.get("POSX") != null && mi.in.get("POSX") != "") {
      if (utility.call("NumberUtil", "isValidNumber", mi.in.get("POSX") as String, ".")) {
        inPOSX = mi.in.get("POSX") as Integer
      } else {
        mi.error("Format numérique suffixe est incorrect")
        return
      }
    }
    inDLIX = 0
    if (mi.in.get("DLIX") != null && mi.in.get("DLIX") != "") {
      if (utility.call("NumberUtil", "isValidNumber", mi.in.get("DLIX") as String, ".")) {
        inDLIX = mi.in.get("DLIX") as long
      } else {
        mi.error("Format numérique index de livraison est incorrect")
        return
      }
    } else {
      mi.error("Index de livraison est obligatoire")
      return
    }
    inWHLO = ""
    if (mi.in.get("WHLO") != null && mi.in.get("WHLO") != "") {
      inWHLO = mi.in.get("WHLO")
    } else {
      mi.error("Dépôt est obligatoire")
      return
    }

    inTEPY = mi.in.get("TEPY")
    orqt = ""

    // Check delivery order line
    DBAction odlineQuery = database.table("ODLINE")
      .selection("UBDLQT", "UBIVQT")
      .index("00").build()
    DBContainer odlineRequest = odlineQuery.getContainer()
    odlineRequest.set("UBCONO", currentCompany)
    odlineRequest.set("UBORNO", inORNO)
    odlineRequest.set("UBPONR", inPONR)
    odlineRequest.set("UBPOSX", inPOSX)
    odlineRequest.set("UBDLIX", inDLIX)
    odlineRequest.set("UBWHLO", inWHLO)
    odlineRequest.set("UBTEPY", inTEPY)
    if (!odlineQuery.read(odlineRequest)) {
      logger.debug("ODLINE not found")
      mi.error("Ligne de livraison non trouvée pour la commande " + inORNO + " ligne " + inPONR + " suffixe " + inPOSX + " index de livraison " + inDLIX)
      return
    } else {
      orqt = odlineRequest.get("UBDLQT")
      double xxorqt = odlineRequest.get("UBDLQT") as Double
      if (xxorqt <= 0) {
        orqt = odlineRequest.get("UBIVQT")
      }
    }

    // Check if a service charge order line already exists for the delivery order line
    serviceChargeOrderLineExists = false
    if (!serviceChargeOrderLineExists) {
      ExpressionFactory oolineExpression = database.getExpressionFactory("OOLINE")
      oolineExpression = oolineExpression.eq("OBUCA6", inORNO)
      oolineExpression = oolineExpression.and(oolineExpression.eq("OBUCA7", inPONR as String))
      oolineExpression = oolineExpression.and(oolineExpression.eq("OBUCA8", inPOSX as String))
      oolineExpression = oolineExpression.and(oolineExpression.eq("OBUCA9", inDLIX as String))
      oolineExpression = oolineExpression.and(oolineExpression.eq("OBWHLO", inWHLO))
      oolineExpression = oolineExpression.and(oolineExpression.eq("OBTEPY", inTEPY))

      DBAction oolineQuery2 = database.table("OOLINE").index("00").matching(oolineExpression).build()
      DBContainer oolineRequest = oolineQuery2.getContainer()
      oolineRequest.set("OBCONO", currentCompany)
      Closure<?> outDataOOLINE = { DBContainer OOLINE ->
        serviceChargeOrderLineExists = true
      }
      if (oolineQuery2.readAll(oolineRequest, 1, nbMaxRecord, outDataOOLINE)) {
      }
      if (serviceChargeOrderLineExists) {
        logger.debug("service charge order line already exists")
        return
      }
    }
    // Check order head
    DBAction ooheadQuery = database.table("OOHEAD").index("00").selection("OACUNO", "OAORST", "OAORTP", "OAORDT").build()
    DBContainer ooheadRequest = ooheadQuery.getContainer()
    ooheadRequest.set("OACONO", currentCompany)
    ooheadRequest.set("OAORNO", inORNO)
    if (ooheadQuery.read(ooheadRequest)) {
      if (ooheadRequest.get("OAORTP") == "P01") {
        mi.error("Commande " + inORNO + " est une commande de frais")
        return
      }
      orst = ooheadRequest.get("OAORST")

      chb6 = 0
      DBAction cugex1Query = database.table("CUGEX1").index("00").selection("F1CHB3", "F1CHB6").build()
      DBContainer cugex1Request = cugex1Query.getContainer()
      cugex1Request.set("F1CONO", currentCompany)
      cugex1Request.set("F1FILE", "OCUSMA")
      cugex1Request.set("F1PK01", ooheadRequest.get("OACUNO"))
      cugex1Request.set("F1PK02", "")
      cugex1Request.set("F1PK03", "")
      cugex1Request.set("F1PK04", "")
      cugex1Request.set("F1PK05", "")
      cugex1Request.set("F1PK06", "")
      cugex1Request.set("F1PK07", "")
      cugex1Request.set("F1PK08", "")
      if (cugex1Query.read(cugex1Request)) {
        chb6 = cugex1Request.get("F1CHB6")
      }
      if (chb6 == 0) {
        mi.error("Client " + ooheadRequest.get("OACUNO") + " est invalide")
        return
      }

      // Retrieve order line informations
      itno = ""
      sapr = ""
      alun = ""
      dwdt = ""
      DBAction oolineQuery = database.table("OOLINE").index("00").selection("OBITNO", "OBORQT", "OBSAPR", "OBWHLO", "OBALUN", "OBDWDT").build()
      DBContainer oolineRequest = oolineQuery.getContainer()
      oolineRequest.set("OBCONO", currentCompany)
      oolineRequest.set("OBORNO", inORNO)
      oolineRequest.set("OBPONR", inPONR)
      oolineRequest.set("OBPOSX", inPOSX)
      if (oolineQuery.read(oolineRequest)) {
        itno = oolineRequest.get("OBITNO")
        sapr = oolineRequest.get("OBSAPR")
        alun = oolineRequest.get("OBALUN")
        dwdt = oolineRequest.get("OBDWDT")
      } else {
        mi.error("Ligne de commande client non trouvée ${inORNO} ${inPONR} ${inPOSX}")
        return
      }

      // Search corresponding service charge order
      ExpressionFactory ooheadFrExpression = database.getExpressionFactory("OOHEAD")
      ooheadFrExpression = ooheadFrExpression.eq("OAOFNO", inORNO)
      ooheadFrExpression = ooheadFrExpression.and(ooheadFrExpression.lt("OAORST", "77"))

      DBAction ooheadFRQuery = database.table("OOHEAD").index("00").matching(ooheadFrExpression).selection("OACONO", "OAORNO").build()
      DBContainer ooheadFRRequest = ooheadQuery.getContainer()
      ooheadFRRequest.setInt("OACONO", currentCompany)
      Closure<?> ooheadFRReader = { DBContainer OOHEAD ->
        logger.debug("Commande de frais existante trouvée - Ajout ligne")
        // Existing service order charge is found, adding the line
        existingServiceChargeOrderOrno = OOHEAD.get("OAORNO")
        newPONR = 0
        newPOSX = 0
        logger.debug("Création ligne orqt:${orqt} alun:${alun}")
        executeOIS100MIAddLineBatchEnt(existingServiceChargeOrderOrno, itno, orqt, "", inWHLO, alun, dwdt)
        executeOIS100MIUpdUserDefCOL()
        mi.outData.put("ORNO", existingServiceChargeOrderOrno)
        mi.outData.put("PONR", newPONR as String)
        mi.outData.put("POSX", newPOSX as String)
        mi.write()
      }
      if (!ooheadFRQuery.readAll(ooheadFRRequest, 1, 1, ooheadFRReader)) {
        logger.debug("Commande de frais n'existe pas")
        // Service charge order does not exist, it must be created
        newORNO = ""
        logger.debug("Copie de la commande inORNO = " + inORNO)
        executeOIS100MICpyOrder(inORNO, "P01", "1", "0", "0", "1", "0", "1", "1", "0", "0", "0", "0", "0", "0")
        if (newORNO.trim() != "") {
          logger.debug("Commande de frais créée - No = " + newORNO)
          executeOIS100MIChgOrderRef(newORNO, inORNO)
          newPONR = 0
          newPOSX = 0
          logger.debug("Ajout ligne")
          executeOIS100MIAddLineBatchEnt(newORNO, itno, orqt, "", inWHLO, alun, dwdt)
          executeOIS100MIUpdUserDefCOL()
        }
        mi.outData.put("ORNO", newORNO)
        mi.outData.put("PONR", newPONR as String)
        mi.outData.put("POSX", newPOSX as String)
        mi.write()
      }
    } else {
      mi.error("Numéro de commande " + inORNO + " n'existe pas")
      return
    }
  }
  // Update new order line with delivery order line primary key
  private executeOIS100MIUpdUserDefCOL() {
    Map<String, String> parameters = [
      "ORNO": newORNO,
      "PONR": newPONR,
      "POSX": newPOSX,
      "UCA6": inORNO,
      "UCA7": inPONR as String,
      "UCA8": inPOSX as String,
      "UCA9": inDLIX as String,

    ]
    Closure<?> handler = { Map<String, String> response ->
      if (response.error != null) {
        return mi.error("Erreur OIS100MI CpyOrder: " + response.errorMessage)
      } else {
        logger.debug("NewORNO: " + response.ORNO)
        newORNO = response.ORNO.trim()
      }
    }
    miCaller.call("OIS100MI", "CpyOrder", parameters, handler)
  }
  // Execute OIS100MI.CpyOrder
  private executeOIS100MICpyOrder(String ORNR, String ORTP, String CORH, String CORL, String COCH, String COTX, String CLCH, String CLTX, String CADR, String SAPR, String UCOS, String JDCD, String RLDT, String CODT, String EPRI) {
    Map<String, String> parameters = ["ORNR": ORNR, "ORTP": ORTP, "CORH": CORH, "CORL": CORL, "COCH": COCH, "COTX": COTX, "CLCH": CLCH, "CLTX": CLTX, "CADR": CADR, "SAPR": SAPR, "UCOS": UCOS, "JDCD": JDCD, "RLDT": RLDT, "CODT": CODT, "EPRI": EPRI]
    Closure<?> handler = { Map<String, String> response ->
      if (response.error != null) {
        return mi.error("Erreur OIS100MI CpyOrder: " + response.errorMessage)
      } else {
        logger.debug("NewORNO: " + response.ORNO)
        newORNO = response.ORNO.trim()
      }
    }
    miCaller.call("OIS100MI", "CpyOrder", parameters, handler)
  }
  // Execute OIS100MI.ChgOrderRef
  private executeOIS100MIChgOrderRef(String orno, String ofno) {
    logger.debug("Changement de référence de commande orno:" + orno + " ofno:" + ofno)
    Map<String, String> parameters = ["ORNO": orno, "OFNO": ofno]
    Closure<?> handler = { Map<String, String> response ->
      if (response.error != null) {
        return mi.error("Erreur OIS100MI ChgOrderRef: " + response.errorMessage)
      } else {
      }
    }
    miCaller.call("OIS100MI", "ChgOrderRef", parameters, handler)
  }
  // Execute OIS100MI.AddLineBatchEnt
  private executeOIS100MIAddLineBatchEnt(String ORNO, String ITNO, String ORQT, String SAPR, String WHLO, String ALUN, String DWDT) {
    Map<String, String> parameters = ["ORNO": ORNO, "ITNO": ITNO, "ORQT": ORQT, "SAPR": SAPR, "WHLO": WHLO, "ALUN": ALUN, "DWDT": DWDT]
    Closure<?> handler = { Map<String, String> response ->
      if (response.error != null) {
        return mi.error("Erreur OIS100MI AddLineBatchEnt: " + response.errorMessage)
      } else {
        newORNO = response.ORNO.trim()
        newPONR = response.PONR.trim() as Integer
        newPOSX = response.POSX.trim() as Integer
      }
    }
    miCaller.call("OIS100MI", "AddLineBatchEnt", parameters, handler)
  }
}

