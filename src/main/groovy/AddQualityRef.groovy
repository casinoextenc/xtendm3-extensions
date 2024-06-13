/**
 * README
 * This extension is used by Mashup
 * QUAX01 Gestion du référentiel qualité
 * Name : EXT032MI.AddQualityRef
 * Description : Add records to the EXT032 table.
 * Date         Changed By   Description
 * 20230210     SEAR         QUAX01 - Constraints matrix
 * 20240605     FLEBARS      QUAX01 - Controle code pour validation Infor
 */

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

public class AddQualityRef extends ExtendM3Transaction {
  private final MIAPI mi
  private final LoggerAPI logger
  private final ProgramAPI program
  private final DatabaseAPI database
  private final SessionAPI session
  private final TransactionAPI transaction
  private final MICallerAPI miCaller
  private final UtilityAPI utility

  private int currentCompany
  private String nbnr

  public AddQualityRef(MIAPI mi, DatabaseAPI database, ProgramAPI program, MICallerAPI miCaller, UtilityAPI utility, LoggerAPI logger) {
    this.mi = mi
    this.database = database
    this.program = program
    this.miCaller = miCaller
    this.utility = utility
    this.logger = logger
  }

  public void main() {
    if (mi.in.get("CONO") == null) {
      currentCompany = (Integer) program.getLDAZD().CONO
    } else {
      currentCompany = mi.in.get("CONO")
    }

    //Get mi inputs
    String suno = (mi.in.get("SUNO") != null ? (String) mi.in.get("SUNO") : "")
    String popn = (mi.in.get("POPN") != null ? (String) mi.in.get("POPN") : "")
    String orco = (mi.in.get("ORCO") != null ? (String) mi.in.get("ORCO") : "")
    String zori = ""
    int zalc = (mi.in.get("ZALC") != null ? (Integer) mi.in.get("ZALC") : 0)
    String zca1 = (mi.in.get("ZCA1") != null ? (String) mi.in.get("ZCA1") : "")
    String zca2 = (mi.in.get("ZCA2") != null ? (String) mi.in.get("ZCA2") : "")
    String zca3 = (mi.in.get("ZCA3") != null ? (String) mi.in.get("ZCA3") : "")
    String zca4 = (mi.in.get("ZCA4") != null ? (String) mi.in.get("ZCA4") : "")
    String zca5 = (mi.in.get("ZCA5") != null ? (String) mi.in.get("ZCA5") : "")
    String zca6 = (mi.in.get("ZCA6") != null ? (String) mi.in.get("ZCA6") : "")
    String zca7 = (mi.in.get("ZCA7") != null ? (String) mi.in.get("ZCA7") : "")
    String zca8 = (mi.in.get("ZCA8") != null ? (String) mi.in.get("ZCA8") : "")
    long txid = (mi.in.get("TXID") != null ? (Long) mi.in.get("TXID") : 0)
    String zcon = (mi.in.get("ZCON") != null ? (String) mi.in.get("ZCON") : "")
    double zpeg = (mi.in.get("ZPEG") != null ? (Double) mi.in.get("ZPEG") : 0)
    int zsan = (mi.in.get("ZSAN") != null ? (Integer) mi.in.get("ZSAN") : 0)
    int zagr = (mi.in.get("ZAGR") != null ? (Integer) mi.in.get("ZAGR") : 0)
    String zcoi = (mi.in.get("ZCOI") != null ? (String) mi.in.get("ZCOI") : "")
    int zphy = (mi.in.get("ZPHY") != null ? (Integer) mi.in.get("ZPHY") : 0)
    String zlat = (mi.in.get("ZLAT") != null ? (String) mi.in.get("ZLAT") : "")
    String znut = (mi.in.get("ZNUT") != null ? (String) mi.in.get("ZNUT") : "")
    double zcal = (mi.in.get("ZCAL") != null ? (Double) mi.in.get("ZCAL") : 0)
    double zjou = (mi.in.get("ZJOU") != null ? (Double) mi.in.get("ZJOU") : 0)
    double zmat = (mi.in.get("ZMAT") != null ? (Double) mi.in.get("ZMAT") : 0)
    double zags = (mi.in.get("ZAGS") != null ? (Double) mi.in.get("ZAGS") : 0)
    double zglu = (mi.in.get("ZGLU") != null ? (Double) mi.in.get("ZGLU") : 0)
    double zsuc = (mi.in.get("ZSUC") != null ? (Double) mi.in.get("ZSUC") : 0)
    double zfib = (mi.in.get("ZFIB") != null ? (Double) mi.in.get("ZFIB") : 0)
    double zpro = (mi.in.get("ZPRO") != null ? (Double) mi.in.get("ZPRO") : 0)
    double zsel = (mi.in.get("ZSEL") != null ? (Double) mi.in.get("ZSEL") : 0)
    double zall = (mi.in.get("ZALL") != null ? (Double) mi.in.get("ZALL") : 0)
    double zagt = (mi.in.get("ZAGT") != null ? (Double) mi.in.get("ZAGT") : 0)
    int zqua = (mi.in.get("ZQUA") != null ? (Integer) mi.in.get("ZQUA") : 0)
    int zali = (mi.in.get("ZALI") != null ? (Integer) mi.in.get("ZALI") : 0)
    String parm = ""

    //Check if SIGMA6 exists in MITPOP
    if (popn.length() > 0) {
      ExpressionFactory ext031Expression = database.getExpressionFactory("MITPOP")
      ext031Expression = ext031Expression.ge("MPREMK", "SIGMA6")
      DBAction mitpopQuery = database.table("MITPOP").index("10").matching(ext031Expression).build()
      DBContainer mitpopRequest = mitpopQuery.getContainer()
      mitpopRequest.set("MPCONO", currentCompany)
      mitpopRequest.setInt("MPALWT", 1)
      mitpopRequest.set("MPALWQ", "")
      mitpopRequest.set("MPPOPN", popn)
      if (!mitpopQuery.readAll(mitpopRequest, 4, 1, mitpopReader)) {
        mi.error("SIGMA6 " + popn + " n'existe pas")
        return
      }
    }

    //Check if record existe in supplier Table (CIDMAS)
    if (suno.length() > 0) {
      DBAction cidmasQuery = database.table("CIDMAS").index("00").build()
      DBContainer cidmasRequest = cidmasQuery.getContainer()
      cidmasRequest.set("IDCONO", currentCompany)
      cidmasRequest.set("IDSUNO", suno)
      if (!cidmasQuery.read(cidmasRequest)) {
        mi.error("Code fournisseur " + suno + " n'existe pas")
        return
      }
    }

    //Check if record exists in country Code Table (EXT034)
    zori = "0"
    if (orco.length() > 0) {
      DBAction csytabQuery = database.table("CSYTAB").index("00").selection("CTPARM").build()
      DBContainer csytabRequest = csytabQuery.getContainer()
      csytabRequest.set("CTCONO", currentCompany)
      csytabRequest.set("CTSTCO", "CSCD")
      csytabRequest.set("CTSTKY", orco)
      if (!csytabQuery.read(csytabRequest)) {
        mi.error("Code pays origine " + orco + " n'existe pas")
        return
      } else {
        parm = csytabRequest.get("CTPARM")
        zori = parm.substring(0, 1)
        if (!"1".equals(zori))
          zori = "0"
      }
    }

    // check alcohol
    if (zalc != 0 && zalc != 1) {
      mi.error("L'indicateur d'alcool ZALC doit être égal à 0 ou 1")
      return
    }

    //Check if record exists in Feature Contraint Table (EXT033)
    if (zca1.length() > 0) {
      DBAction ext033Query = database.table("EXT033").index("00").build()
      DBContainer ext033Request = ext033Query.getContainer()
      ext033Request.set("EXCONO", currentCompany)
      ext033Request.set("EXZCAR", zca1)
      if (!ext033Query.read(ext033Request)) {
        mi.error("Caractéristique de contrainte 1 (EXT033) " + zca1 + " n'existe pas")
        return
      }
    }
    if (zca2.length() > 0) {
      DBAction ext033Query = database.table("EXT033").index("00").build()
      DBContainer ext033Request = ext033Query.getContainer()
      ext033Request.set("EXCONO", currentCompany)
      ext033Request.set("EXZCAR", zca2)
      if (!ext033Query.read(ext033Request)) {
        mi.error("Caractéristique de contrainte 2 (EXT033) " + zca2 + " n'existe pas")
        return
      }
    }
    if (zca3.length() > 0) {
      DBAction ext033Query = database.table("EXT033").index("00").build()
      DBContainer ext033Request = ext033Query.getContainer()
      ext033Request.set("EXCONO", currentCompany)
      ext033Request.set("EXZCAR", zca3)
      if (!ext033Query.read(ext033Request)) {
        mi.error("Caractéristique de contrainte 3 (EXT033) " + zca3 + " n'existe pas")
        return
      }
    }
    if (zca4.length() > 0) {
      DBAction ext033Query = database.table("EXT033").index("00").build()
      DBContainer ext033Request = ext033Query.getContainer()
      ext033Request.set("EXCONO", currentCompany)
      ext033Request.set("EXZCAR", zca4)
      if (!ext033Query.read(ext033Request)) {
        mi.error("Caractéristique de contrainte 4 (EXT033) " + zca4 + " n'existe pas")
        return
      }
    }
    if (zca5.length() > 0) {
      DBAction ext033Query = database.table("EXT033").index("00").build()
      DBContainer ext033Request = ext033Query.getContainer()
      ext033Request.set("EXCONO", currentCompany)
      ext033Request.set("EXZCAR", zca5)
      if (!ext033Query.read(ext033Request)) {
        mi.error("Caractéristique de contrainte 5 (EXT033) " + zca5 + " n'existe pas")
        return
      }
    }
    if (zca6.length() > 0) {
      DBAction ext033Query = database.table("EXT033").index("00").build()
      DBContainer ext033Request = ext033Query.getContainer()
      ext033Request.set("EXCONO", currentCompany)
      ext033Request.set("EXZCAR", zca6)
      if (!ext033Query.read(ext033Request)) {
        mi.error("Caractéristique de contrainte 6 (EXT033) " + zca6 + " n'existe pas")
        return
      }
    }
    if (zca7.length() > 0) {
      DBAction ext033Query = database.table("EXT033").index("00").build()
      DBContainer ext033Request = ext033Query.getContainer()
      ext033Request.set("EXCONO", currentCompany)
      ext033Request.set("EXZCAR", zca7)
      if (!ext033Query.read(ext033Request)) {
        mi.error("Caractéristique de contrainte 7 (EXT033) " + zca7 + " n'existe pas")
        return
      }
    }
    if (zca8.length() > 0) {
      DBAction ext033Query = database.table("EXT033").index("00").build()
      DBContainer ext033Request = ext033Query.getContainer()
      ext033Request.set("EXCONO", currentCompany)
      ext033Request.set("EXZCAR", zca8)
      if (!ext033Query.read(ext033Request)) {
        mi.error("Caractéristique de contrainte 8 (EXT033) " + zca8 + " n'existe pas")
        return
      }
    }

    //Check if record has an associated text
    if (txid > 0) {
      String textID = (Long) mi.in.get("TXID")
      ExpressionFactory msytxhExpression = database.getExpressionFactory("MSYTXH")
      msytxhExpression = msytxhExpression.ge("THTXID", textID)
      DBAction msytxhQuery = database.table("MSYTXH").index("10").matching(msytxhExpression).build()
      DBContainer msytxhRequest = msytxhQuery.getContainer()
      msytxhRequest.set("THCONO", currentCompany)
      msytxhRequest.set("THDIVI", "")
      msytxhRequest.set("THKFLD", "COMPO")
      msytxhRequest.set("THTXVR", "EXT032")
      if (!msytxhQuery.readAll(msytxhRequest, 4, 10000, msytxhReader)) {
        mi.error("code text TXID " + txid + " n'existe pas")
        return
      }
    }

    // check sanitary
    if (zsan != 0 && zsan != 1) {
      mi.error("L'indicateur sanitaire ZSAN doit être égal à 0 ou 1")
      return
    }

    // check agreement
    if (zagr != 0 && zagr != 1) {
      mi.error("L'indicateur contrat ZAGR doit être égal à 0 ou 1")
      return
    }

    // check PhytoSanitary
    if (zphy != 0 && zphy != 1) {
      mi.error("L'indicateur Phytosanitaire ZPHY doit être égal à 0 ou 1")
      return
    }

    // check alcohol
    if (zalc != 0 && zalc != 1) {
      mi.error("L'indicateur d'alcool ZALC doit être égal à 0 ou 1")
      return
    }

    // check Dangerous
    if (zqua != 0 && zqua != 1) {
      mi.error("L'indicateur qualité ZQUA doit être égal à 0 ou 1")
      return
    }

    // check Alimental
    if (zali != 0 && zali != 1) {
      mi.error("L'indicateur alimentaire ZALI doit être égal à 0 ou 1")
      return
    }

    LocalDateTime timeOfCreation = LocalDateTime.now()
    DBAction ext032Query = database.table("EXT032").index("00").build()
    DBContainer ext032Request = ext032Query.getContainer()
    ext032Request.set("EXCONO", currentCompany)
    ext032Request.set("EXPOPN", popn)
    ext032Request.set("EXSUNO", suno)
    ext032Request.set("EXORCO", orco)
    if (!ext032Query.read(ext032Request)) {
      ext032Request.set("EXZORI", Integer.parseInt(zori))
      ext032Request.set("EXZALC", zalc)
      ext032Request.set("EXZCA1", zca1)
      ext032Request.set("EXZCA2", zca2)
      ext032Request.set("EXZCA3", zca3)
      ext032Request.set("EXZCA4", zca4)
      ext032Request.set("EXZCA5", zca5)
      ext032Request.set("EXZCA6", zca6)
      ext032Request.set("EXZCA7", zca7)
      ext032Request.set("EXZCA8", zca8)
      ext032Request.set("EXTXID", txid)
      ext032Request.set("EXZCON", zcon)
      ext032Request.set("EXZPEG", zpeg)
      ext032Request.set("EXZSAN", zsan)
      ext032Request.set("EXZAGR", zagr)
      ext032Request.set("EXZCOI", zcoi)
      ext032Request.set("EXZPHY", zphy)
      ext032Request.set("EXZLAT", zlat)
      ext032Request.set("EXZNUT", znut)
      ext032Request.set("EXZCAL", zcal)
      ext032Request.set("EXZJOU", zjou)
      ext032Request.set("EXZMAT", zmat)
      ext032Request.set("EXZAGS", zags)
      ext032Request.set("EXZGLU", zglu)
      ext032Request.set("EXZSUC", zsuc)
      ext032Request.set("EXZFIB", zfib)
      ext032Request.set("EXZPRO", zpro)
      ext032Request.set("EXZSEL", zsel)
      ext032Request.set("EXZALL", zall)
      ext032Request.set("EXZAGT", zagt)
      ext032Request.set("EXZQUA", zqua)
      ext032Request.set("EXZALI", zali)
      ext032Request.setInt("EXRGDT", timeOfCreation.format(DateTimeFormatter.ofPattern("yyyyMMdd")) as Integer)
      ext032Request.setInt("EXRGTM", timeOfCreation.format(DateTimeFormatter.ofPattern("HHmmss")) as Integer)
      ext032Request.setInt("EXLMDT", timeOfCreation.format(DateTimeFormatter.ofPattern("yyyyMMdd")) as Integer)
      ext032Request.setInt("EXCHNO", 1)
      ext032Request.set("EXCHID", program.getUser())
      ext032Query.insert(ext032Request)
    } else {
      mi.error("L'enregistrement existe déjà")
      return
    }
  }

  Closure<?> mitpopReader = { DBContainer mitpopResult ->
  }

  Closure<?> msytxhReader = { DBContainer msytxhResult ->
  }
}
