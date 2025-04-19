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
  private String inOrno
  private Integer inPonr
  private Integer inPosx
  private long inDlix
  private String inWhlo
  private String inTepy
  private String itno
  private String orqt
  private String sapr
  private String alun
  private String dwdt
  private String existingServiceChargeOrderOrno
  private String newOrno
  private Integer newPonr
  private Integer newPosx
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

    inOrno = ""
    if (mi.in.get("ORNO") != null && mi.in.get("ORNO") != "") {
      inOrno = mi.in.get("ORNO")
    } else {
      mi.error("Numéro commande de vente est obligatoire")
      return
    }
    inPonr = 0
    if (mi.in.get("PONR") != null && mi.in.get("PONR") != "") {
      if (utility.call("NumberUtil", "isValidNumber", mi.in.get("PONR") as String, ".")) {
        inPonr = mi.in.get("PONR") as Integer
      } else {
        mi.error("Format numérique ligne est incorrect")
        return
      }
    } else {
      mi.error("Numéro de ligne est obligatoire")
      return
    }
    inPosx = 0
    if (mi.in.get("POSX") != null && mi.in.get("POSX") != "") {
      if (utility.call("NumberUtil", "isValidNumber", mi.in.get("POSX") as String, ".")) {
        inPosx = mi.in.get("POSX") as Integer
      } else {
        mi.error("Format numérique suffixe est incorrect")
        return
      }
    }
    inDlix = 0
    if (mi.in.get("DLIX") != null && mi.in.get("DLIX") != "") {
      if (utility.call("NumberUtil", "isValidNumber", mi.in.get("DLIX") as String, ".")) {
        inDlix = mi.in.get("DLIX") as long
      } else {
        mi.error("Format numérique index de livraison est incorrect")
        return
      }
    } else {
      mi.error("Index de livraison est obligatoire")
      return
    }
    inWhlo = ""
    if (mi.in.get("WHLO") != null && mi.in.get("WHLO") != "") {
      inWhlo = mi.in.get("WHLO")
    } else {
      mi.error("Dépôt est obligatoire")
      return
    }

    inTepy = mi.in.get("TEPY")
    orqt = ""

    // Check delivery order line
    DBAction odlineQuery = database.table("ODLINE")
      .selection("UBDLQT", "UBIVQT")
      .index("00").build()
    DBContainer odlineRequest = odlineQuery.getContainer()
    odlineRequest.set("UBCONO", currentCompany)
    odlineRequest.set("UBORNO", inOrno)
    odlineRequest.set("UBPONR", inPonr)
    odlineRequest.set("UBPOSX", inPosx)
    odlineRequest.set("UBDLIX", inDlix)
    odlineRequest.set("UBWHLO", inWhlo)
    odlineRequest.set("UBTEPY", inTepy)
    if (!odlineQuery.read(odlineRequest)) {
      logger.debug("ODLINE not found")
      mi.error("Ligne de livraison non trouvée pour la commande " + inOrno + " ligne " + inPonr + " suffixe " + inPosx + " index de livraison " + inDlix)
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
      oolineExpression = oolineExpression.eq("OBUCA6", inOrno)
      oolineExpression = oolineExpression.and(oolineExpression.eq("OBUCA7", inPonr as String))
      oolineExpression = oolineExpression.and(oolineExpression.eq("OBUCA8", inPosx as String))
      oolineExpression = oolineExpression.and(oolineExpression.eq("OBUCA9", inDlix as String))
      oolineExpression = oolineExpression.and(oolineExpression.eq("OBWHLO", inWhlo))
      oolineExpression = oolineExpression.and(oolineExpression.eq("OBTEPY", inTepy))

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
    ooheadRequest.set("OAORNO", inOrno)
    if (ooheadQuery.read(ooheadRequest)) {
      if (ooheadRequest.get("OAORTP") == "P01") {
        mi.error("Commande " + inOrno + " est une commande de frais")
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
      oolineRequest.set("OBORNO", inOrno)
      oolineRequest.set("OBPONR", inPonr)
      oolineRequest.set("OBPOSX", inPosx)
      if (oolineQuery.read(oolineRequest)) {
        itno = oolineRequest.get("OBITNO")
        sapr = oolineRequest.get("OBSAPR")
        alun = oolineRequest.get("OBALUN")
        dwdt = oolineRequest.get("OBDWDT")
      } else {
        mi.error("Ligne de commande client non trouvée ${inOrno} ${inPonr} ${inPosx}")
        return
      }

      // Search corresponding service charge order
      ExpressionFactory ooheadFrExpression = database.getExpressionFactory("OOHEAD")
      ooheadFrExpression = ooheadFrExpression.eq("OAOFNO", inOrno)
      ooheadFrExpression = ooheadFrExpression.and(ooheadFrExpression.eq("OAUDN3", inDLIX))

      DBAction ooheadFRQuery = database.table("OOHEAD").index("00").matching(ooheadFrExpression).selection("OACONO", "OAORNO").build()
      DBContainer ooheadFRRequest = ooheadQuery.getContainer()
      ooheadFRRequest.setInt("OACONO", currentCompany)
      Closure<?> ooheadFRReader = { DBContainer OOHEAD ->
        logger.debug("Commande de frais existante trouvée - Ajout ligne")
        // Existing service order charge is found, adding the line
        existingServiceChargeOrderOrno = OOHEAD.get("OAORNO")
        newPonr = 0
        newPosx = 0
        logger.debug("Création ligne orqt:${orqt} alun:${alun}")
        executeOIS100MIAddLineBatchEnt(existingServiceChargeOrderOrno, itno, orqt, "", inWhlo, alun, dwdt)
        updateServiceOrderLine()
        mi.outData.put("ORNO", existingServiceChargeOrderOrno)
        mi.outData.put("PONR", newPonr as String)
        mi.outData.put("POSX", newPosx as String)
        mi.write()
      }
      if (!ooheadFRQuery.readAll(ooheadFRRequest, 1, 1, ooheadFRReader)) {
        logger.debug("Commande de frais n'existe pas")
        // Service charge order does not exist, it must be created
        newOrno = ""
        logger.debug("Copie de la commande inORNO = " + inOrno)
        executeOIS100MICpyOrder(inOrno, "P01", "1", "1", "0", "1", "0", "1", "1", "0", "0", "0", "0", "0", "0")
        if (newOrno.trim() != "") {
          logger.debug("Commande de frais créée - No = " + newOrno)
          executeOIS100MIChgOrderRef(newOrno, inOrno)
          newPonr = 0
          newPosx = 0
          logger.debug("Ajout ligne")
          //executeOIS100MIAddLineBatchEnt(newOrno, itno, orqt, "", inWhlo, alun, dwdt)
          updateServiceOrderLine()
        }
        mi.outData.put("ORNO", newOrno)
        mi.outData.put("PONR", newPonr as String)
        mi.outData.put("POSX", newPosx as String)
        mi.write()
      }
    } else {
      mi.error("Numéro de commande " + inOrno + " n'existe pas")
      return
    }
  }
  // Update new order line with delivery order line primary key
  private updateServiceOrderLine() {
    logger.debug("Màj ligne newORNO/newPONR/newPOSX = " + newOrno + "/" + newPonr + "/" + newPosx)
    DBAction queryOOLINE = database.table("OOLINE").index("00").build()
    DBContainer OOLINE = queryOOLINE.getContainer()
    OOLINE.set("OBCONO", currentCompany)
    OOLINE.set("OBORNO", newOrno)
    OOLINE.set("OBPONR", newPonr)
    OOLINE.set("OBPOSX", newPosx)
    if (!queryOOLINE.readLock(OOLINE, updateCallBack)) {
    }
  }
  // Execute OIS100MI.CpyOrder
  private executeOIS100MICpyOrder(String ornr, String ortp, String corh, String corl, String coch, String cotx, String clch, String cltx, String cadr, String sapr, String ucos, String jdcd, String rldt, String codt, String epri) {
    Map<String, String> parameters = ["ORNR": ornr, "ORTP": ortp, "CORH": corh, "CORL": corl, "COCH": coch, "COTX": cotx, "CLCH": clch, "CLTX": cltx, "CADR": cadr, "SAPR": sapr, "UCOS": ucos, "JDCD": jdcd, "RLDT": rldt, "CODT": codt, "EPRI": epri]
    logger.debug("Paramètres OIS100MI CpyOrder: " + parameters)
    Closure<?> handler = { Map<String, String> response ->
      if (response.error != null) {
        return mi.error("Erreur OIS100MI CpyOrder: " + response.errorMessage)
        logger.debug("Erreur OIS100MI CpyOrder: " + response.errorMessage)
      } else {
        logger.debug("NewORNO: " + response.ORNO)
        newOrno = response.ORNO.trim()
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
  private executeOIS100MIAddLineBatchEnt(String orno, String itno, String orqt, String sapr, String whlo, String alun, String dwdt) {
    Map<String, String> parameters = ["ORNO": orno, "ITNO": itno, "ORQT": orqt, "SAPR": sapr, "WHLO": whlo, "ALUN": alun, "DWDT": dwdt]
    Closure<?> handler = { Map<String, String> response ->
      if (response.error != null) {
        return mi.error("Erreur OIS100MI AddLineBatchEnt: " + response.errorMessage)
      } else {
        newOrno = response.ORNO.trim()
        newPonr = response.PONR.trim() as Integer
        newPosx = response.POSX.trim() as Integer
      }
    }
    miCaller.call("OIS100MI", "AddLineBatchEnt", parameters, handler)
  }

  // Update OOLINE
  Closure<?> updateCallBack = { LockedResult lockedResult ->
    LocalDateTime timeOfCreation = LocalDateTime.now()
    int changeNumber = lockedResult.get("OBCHNO")
    lockedResult.set("OBUCA6", inOrno)
    lockedResult.set("OBUCA7", inPonr as String)
    lockedResult.set("OBUCA8", inPosx as String)
    lockedResult.set("OBUCA9", inDlix as String)
    lockedResult.setInt("OBLMDT", timeOfCreation.format(DateTimeFormatter.ofPattern("yyyyMMdd")) as Integer)
    lockedResult.setInt("OBCHNO", changeNumber + 1)
    lockedResult.set("OBCHID", program.getUser())
    lockedResult.update()
  }
}
