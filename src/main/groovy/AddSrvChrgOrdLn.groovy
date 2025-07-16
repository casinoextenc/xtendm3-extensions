/****************************************************************************************
 Extension Name: EXT062MI.AddSrvChrgOrdLn
 Type: ExtendM3Transaction
 Script Author: RENARN
 Date: 2023-11-24
 Description:
 * Add line to service order charge

 Revision History:
 Name        Date         Version   Description of Changes
 RENARN      2023-11-24   1.0       CMD03 - Calculation of service charges
 ARENARD     2025-04-22   1.1       Code has been checked
 ******************************************************************************************/

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

      DBAction ooheadFrQuery = database.table("OOHEAD").index("00").matching(ooheadFrExpression).selection("OACONO", "OAORNO").build()
      DBContainer ooheadFrRequest = ooheadQuery.getContainer()
      ooheadFrRequest.setInt("OACONO", currentCompany)
      Closure<?> ooheadFrReader = { DBContainer OOHEAD ->
        logger.debug("Commande de frais existante trouvée - Ajout ligne")
        // Existing service order charge is found, adding the line
        existingServiceChargeOrderOrno = OOHEAD.get("OAORNO")
        newPonr = 0
        newPosx = 0
        executeOIS100MIUpdUserDefCOL(existingServiceChargeOrderOrno.toString(),inPonr.toString(),inPosx.toString(),inDlix.toString(),inOrno.toString(),inPonr.toString(),inPosx.toString())
        mi.outData.put("ORNO", existingServiceChargeOrderOrno)
        mi.outData.put("PONR", newPonr as String)
        mi.outData.put("POSX", newPosx as String)
        mi.write()
      }
      if (!ooheadFrQuery.readAll(ooheadFrRequest, 1, 1, ooheadFrReader)) {
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
          executeOIS100MIUpdUserDefCOL(newOrno.toString(),inPonr.toString(),inPosx.toString(),inDlix.toString(),inOrno.toString(),inPonr.toString(),inPosx.toString())
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
  /**
   * Execute OIS100MI.CpyOrder
   * @param ornr
   * @param ortp
   * @param corh
   * @param corl
   * @param coch
   * @param cotx
   * @param clch
   * @param cltx
   * @param cadr
   * @param sapr
   * @param ucos
   * @param jdcd
   * @param rldt
   * @param codt
   * @param epri
   */
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
  /**
   * Execute OIS100MI.ChgOrderRef
   * @param orno
   * @param ofno
   */
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
  /** Execute OIS100MI.AddLineBatchEnt
   * @param orno
   * @param itno
   * @param orqt
   * @param sapr
   * @param whlo
   * @param alun
   * @param dwdt
   */
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

  /** Execute OIS100MI.UpdUserDefCOL
   * @param newOrno
   * @param newPonr
   * @param newPosx
   * @param dlix
   * @param origORNO
   * @param origPONR
   * @param origPosx
   */
  private executeOIS100MIUpdUserDefCOL(String newOrno, String newPonr, String newPosx, String dlix, String origORNO, String origPONR, String origPosx) {
    Map<String, String> parameters = ["ORNO": newOrno, "PONR": newPonr, "UCA6": inOrno.toString(), "UCA7": inPonr.toString(), "UCA8": inPosx.toString(), "UCA9": dlix]
    Closure<?> handler = { Map<String, String> response ->
      if (response.error != null) {
        return mi.error("Erreur OIS100MI UpdUserDefCOL: " + response.errorMessage)
      } else {
        logger.debug("Order ${newOrno} updated on line ${inPonr.toString()}, newPonr: ${newPonr}")
      }
    }
    miCaller.call("OIS100MI", "UpdUserDefCOL", parameters, handler)
  }
}
