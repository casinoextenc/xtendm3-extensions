/**
 * README
 * This extension is used by Mashup
 *
 * Name : EXT032MI.UpdQualityRef
 * Description : Update records to the EXT032 table.
 * Date         Changed By   Description
 * 20230210     SEAR         QUAX01 - Constraints matrix
 */

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

public class UpdQualityRef extends ExtendM3Transaction {
  private final MIAPI mi
  private final LoggerAPI logger
  private final ProgramAPI program
  private final DatabaseAPI database
  private final SessionAPI session
  private final TransactionAPI transaction
  private final MICallerAPI miCaller
  private final UtilityAPI utility
  private String NBNR
  private String ZGKY
  private Integer zblc
  private Integer ztps
  private String parm

  public UpdQualityRef(MIAPI mi, DatabaseAPI database, ProgramAPI program, MICallerAPI miCaller, UtilityAPI utility) {
    this.mi = mi
    this.database = database
    this.program = program
    this.miCaller = miCaller
    this.utility = utility
  }

  public void main() {

    Integer currentCompany
    if (mi.in.get("CONO") == null) {
      currentCompany = (Integer)program.getLDAZD().CONO
    } else {
      currentCompany = mi.in.get("CONO")
    }

    //Get mi inputs
    String suno = (mi.in.get("SUNO") != null ? (String)mi.in.get("SUNO") : "")
    String popn = (mi.in.get("POPN") != null ? (String)mi.in.get("POPN") : "")
    String orco = (mi.in.get("ORCO") != null ? (String)mi.in.get("ORCO") : "")
    String zori = ""
    int zalc = (mi.in.get("ZALC") != null ? (Integer)mi.in.get("ZALC") : 0)
    String zca1 = (mi.in.get("ZCA1") != null ? (String)mi.in.get("ZCA1") : "")
    String zca2 = (mi.in.get("ZCA2") != null ? (String)mi.in.get("ZCA2") : "")
    String zca3 = (mi.in.get("ZCA3") != null ? (String)mi.in.get("ZCA3") : "")
    String zca4 = (mi.in.get("ZCA4") != null ? (String)mi.in.get("ZCA4") : "")
    String zca5 = (mi.in.get("ZCA5") != null ? (String)mi.in.get("ZCA5") : "")
    String zca6 = (mi.in.get("ZCA6") != null ? (String)mi.in.get("ZCA6") : "")
    String zca7 = (mi.in.get("ZCA7") != null ? (String)mi.in.get("ZCA7") : "")
    String zca8 = (mi.in.get("ZCA8") != null ? (String)mi.in.get("ZCA8") : "")
    long txid = (mi.in.get("TXID") != null ? (Long)mi.in.get("TXID") :0)
    String zcon = (mi.in.get("ZCON") != null ? (String)mi.in.get("ZCON") : "")
    double zpeg = (mi.in.get("ZPEG") != null ? (Double)mi.in.get("ZPEG") : 0)
    int zsan = (mi.in.get("ZSAN") != null ? (Integer)mi.in.get("ZSAN") : 0)
    int zagr = (mi.in.get("ZAGR") != null ? (Integer)mi.in.get("ZAGR") : 0)
    String zcoi = (mi.in.get("ZCOI") != null ? (String)mi.in.get("ZCOI") : "")
    int zphy = (mi.in.get("ZPHY") != null ? (Integer)mi.in.get("ZPHY") : 0)
    String zlat = (mi.in.get("ZLAT") != null ? (String)mi.in.get("ZLAT") : "")
    String znut = (mi.in.get("ZNUT") != null ? (String)mi.in.get("ZNUT") : "")
    double zcal = (mi.in.get("ZCAL") != null ? (Double)mi.in.get("ZCAL") : 0)
    double zjou = (mi.in.get("ZJOU") != null ? (Double)mi.in.get("ZJOU") : 0)
    double zmat = (mi.in.get("ZMAT") != null ? (Double)mi.in.get("ZMAT") : 0)
    double zags = (mi.in.get("ZAGS") != null ? (Double)mi.in.get("ZAGS") : 0)
    double zglu = (mi.in.get("ZGLU") != null ? (Double)mi.in.get("ZGLU") : 0)
    double zsuc = (mi.in.get("ZSUC") != null ? (Double)mi.in.get("ZSUC") : 0)
    double zfib = (mi.in.get("ZFIB") != null ? (Double)mi.in.get("ZFIB") : 0)
    double zpro = (mi.in.get("ZPRO") != null ? (Double)mi.in.get("ZPRO") : 0)
    double zsel = (mi.in.get("ZSEL") != null ? (Double)mi.in.get("ZSEL") : 0)
    double zall = (mi.in.get("ZALL") != null ? (Double)mi.in.get("ZALL") : 0)
    double zagt = (mi.in.get("ZAGT") != null ? (Double)mi.in.get("ZAGT") : 0)
    int zqua = (mi.in.get("ZQUA") != null ? (Integer)mi.in.get("ZQUA") : 0)
    int zali = (mi.in.get("ZALI") != null ? (Integer)mi.in.get("ZALI") : 0)

    //Check if SIGMA6 exists in MITPOP
    if (popn.length() > 0) {
      ExpressionFactory expression = database.getExpressionFactory("MITPOP")
      expression = expression.ge("MPREMK", "SIGMA6")
      DBAction queryMITPOP = database.table("MITPOP").index("10").matching(expression).build()
      DBContainer ContainerMITPOP = queryMITPOP.getContainer()
      ContainerMITPOP.set("MPCONO", currentCompany)
      ContainerMITPOP.setInt("MPALWT", 1)
      ContainerMITPOP.set("MPALWQ", "")
      ContainerMITPOP.set("MPPOPN", popn)
      if (!queryMITPOP.readAll(ContainerMITPOP, 4, MITPOPData)) {
        mi.error("SIGMA6 " + popn + " n'existe pas")
        return
      }
    }

    //Check if record existe in supplier Table (CIDMAS)
    if (suno.length() > 0) {
      DBAction queryCIDMAS = database.table("CIDMAS").index("00").build()
      DBContainer ContainerCIDMAS = queryCIDMAS.getContainer()
      ContainerCIDMAS.set("IDCONO", currentCompany)
      ContainerCIDMAS.set("IDSUNO", suno)
      if (!queryCIDMAS.read(ContainerCIDMAS)) {
        mi.error("Code fournisseur " + suno + " n'existe pas")
        return
      }
    }

    //Check if record exists in country Code Table (EXT034)
    if (orco.length() > 0) {
      zori = "0"
      DBAction queryCSYTAB = database.table("CSYTAB").index("00").selection("CTPARM").build()
      DBContainer ContainerCSYTAB = queryCSYTAB.getContainer()
      ContainerCSYTAB.set("CTCONO", currentCompany)
      ContainerCSYTAB.set("CTSTCO", "CSCD")
      ContainerCSYTAB.set("CTSTKY", orco)
      if (!queryCSYTAB.read(ContainerCSYTAB)) {
        mi.error("Code pays origine " + orco + " n'existe pas")
        return
      } else {
        parm = ContainerCSYTAB.get("CTPARM")
        zori = parm.substring(0,1)
        if (!"1".equals(zori))
          zori = "0"
      }
    }

    // check alcohol
    if (zalc != 0 && zalc != 1){
      mi.error("L'indicateur d'alcool ZALC doit être égal à 0 ou 1")
      return
    }

    //Check if record exists in Feature Contraint Table (EXT033)
    if (zca1.length() > 0) {
      DBAction queryEXT033 = database.table("EXT033").index("00").build()
      DBContainer ContainerEXT033 = queryEXT033.getContainer()
      ContainerEXT033.set("EXCONO", currentCompany)
      ContainerEXT033.set("EXZCAR", zca1)
      if(!queryEXT033.read(ContainerEXT033)){
        mi.error("Caractéristique de contrainte 1 (EXT033) " + zca1 + " n'existe pas")
        return
      }
    }
    if (zca2.length() > 0) {
      DBAction queryEXT033 = database.table("EXT033").index("00").build()
      DBContainer ContainerEXT033 = queryEXT033.getContainer()
      ContainerEXT033.set("EXCONO", currentCompany)
      ContainerEXT033.set("EXZCAR", zca2)
      if(!queryEXT033.read(ContainerEXT033)){
        mi.error("Caractéristique de contrainte 2 (EXT033) " + zca2 + " n'existe pas")
        return
      }
    }
    if (zca3.length() > 0) {
      DBAction queryEXT033 = database.table("EXT033").index("00").build()
      DBContainer ContainerEXT033 = queryEXT033.getContainer()
      ContainerEXT033.set("EXCONO", currentCompany)
      ContainerEXT033.set("EXZCAR", zca3)
      if(!queryEXT033.read(ContainerEXT033)){
        mi.error("Caractéristique de contrainte 3 (EXT033) " + zca3 + " n'existe pas")
        return
      }
    }
    if (zca4.length() > 0) {
      DBAction queryEXT033 = database.table("EXT033").index("00").build()
      DBContainer ContainerEXT033 = queryEXT033.getContainer()
      ContainerEXT033.set("EXCONO", currentCompany)
      ContainerEXT033.set("EXZCAR", zca4)
      if(!queryEXT033.read(ContainerEXT033)){
        mi.error("Caractéristique de contrainte 4 (EXT033) " + zca4 + " n'existe pas")
        return
      }
    }
    if (zca5.length() > 0) {
      DBAction queryEXT033 = database.table("EXT033").index("00").build()
      DBContainer ContainerEXT033 = queryEXT033.getContainer()
      ContainerEXT033.set("EXCONO", currentCompany)
      ContainerEXT033.set("EXZCAR", zca5)
      if(!queryEXT033.read(ContainerEXT033)){
        mi.error("Caractéristique de contrainte 5 (EXT033) " + zca5 + " n'existe pas")
        return
      }
    }
    if (zca6.length() > 0) {
      DBAction queryEXT033 = database.table("EXT033").index("00").build()
      DBContainer ContainerEXT033 = queryEXT033.getContainer()
      ContainerEXT033.set("EXCONO", currentCompany)
      ContainerEXT033.set("EXZCAR", zca6)
      if(!queryEXT033.read(ContainerEXT033)){
        mi.error("Caractéristique de contrainte 6 (EXT033) " + zca6 + " n'existe pas")
        return
      }
    }
    if (zca7.length() > 0) {
      DBAction queryEXT033 = database.table("EXT033").index("00").build()
      DBContainer ContainerEXT033 = queryEXT033.getContainer()
      ContainerEXT033.set("EXCONO", currentCompany)
      ContainerEXT033.set("EXZCAR", zca7)
      if(!queryEXT033.read(ContainerEXT033)){
        mi.error("Caractéristique de contrainte 7 (EXT033) " + zca7 + " n'existe pas")
        return
      }
    }
    if (zca8.length() > 0) {
      DBAction queryEXT033 = database.table("EXT033").index("00").build()
      DBContainer ContainerEXT033 = queryEXT033.getContainer()
      ContainerEXT033.set("EXCONO", currentCompany)
      ContainerEXT033.set("EXZCAR", zca8)
      if(!queryEXT033.read(ContainerEXT033)){
        mi.error("Caractéristique de contrainte 8 (EXT033) " + zca8 + " n'existe pas")
        return
      }
    }

    //Check if record has an associated text
    if (txid > 0) {
      String textID = (Long)mi.in.get("TXID")
      ExpressionFactory expression = database.getExpressionFactory("MSYTXH")
      expression = expression.ge("THTXID", textID)
      DBAction queryMSYTXH = database.table("MSYTXH").index("10").matching(expression).build()
      DBContainer ContainerMSYTXH = queryMSYTXH.getContainer()
      ContainerMSYTXH.set("THCONO", currentCompany)
      ContainerMSYTXH.set("THDIVI", "")
      ContainerMSYTXH.set("THKFLD", "COMPO")
      ContainerMSYTXH.set("THTXVR", "EXT032")
      if (!queryMSYTXH.readAll(ContainerMSYTXH, 4, MSYTXHData)) {
        mi.error("code text TXID " + txid + " n'existe pas")
        return
      }
    }

    // check sanitary
    if (zsan != 0 && zsan != 1){
      mi.error("L'indicateur sanitaire ZSAN doit être égal à 0 ou 1")
      return
    }

    // check agreement
    if (zagr != 0 && zagr != 1){
      mi.error("L'indicateur contrat ZAGR doit être égal à 0 ou 1")
      return
    }

    // check PhytoSanitary
    if (zphy != 0 && zphy != 1){
      mi.error("L'indicateur Phytosanitaire ZPHY doit être égal à 0 ou 1")
      return
    }

    // check alcohol
    if (zalc != 0 && zalc != 1){
      mi.error("L'indicateur d'alcool ZALC doit être égal à 0 ou 1")
      return
    }

    // check Dangerous
    if (zqua != 0 && zqua != 1){
      mi.error("L'indicateur qualité ZQUA doit être égal à 0 ou 1")
      return
    }

    // check Alimental
    if (zali != 0 && zali != 1){
      mi.error("L'indicateur alimentaire ZALI doit être égal à 0 ou 1")
      return
    }

    LocalDateTime timeOfCreation = LocalDateTime.now()
    DBAction queryEXT032 = database.table("EXT032").index("00").build()
    DBContainer containerEXT032 = queryEXT032.getContainer()
    containerEXT032.set("EXCONO", currentCompany)
    containerEXT032.set("EXPOPN", popn)
    containerEXT032.set("EXSUNO", suno)
    containerEXT032.set("EXORCO", orco)

    //Record exists
    if (!queryEXT032.read(containerEXT032)) {
      mi.error("L'enregistrement n'existe pas")
      return
    }

    Closure<?> updateEXT032 = { LockedResult lockedResultEXT032 ->
      lockedResultEXT032.set("EXZORI", zori as Integer)
      if (mi.in.get("ZALC") != null)
        lockedResultEXT032.set("EXZALC", zalc)
      if (mi.in.get("ZCA1") != null)
        lockedResultEXT032.set("EXZCA1", zca1)
      if (mi.in.get("ZCA2") != null)
        lockedResultEXT032.set("EXZCA2", zca2)
      if (mi.in.get("ZCA3") != null)
        lockedResultEXT032.set("EXZCA3", zca3)
      if (mi.in.get("ZCA4") != null)
        lockedResultEXT032.set("EXZCA4", zca4)
      if (mi.in.get("ZCA5") != null)
        lockedResultEXT032.set("EXZCA5", zca5)
      if (mi.in.get("ZCA6") != null)
        lockedResultEXT032.set("EXZCA6", zca6)
      if (mi.in.get("ZCA7") != null)
        lockedResultEXT032.set("EXZCA7", zca7)
      if (mi.in.get("ZCA8") != null)
        lockedResultEXT032.set("EXZCA8", zca8)
      if (mi.in.get("TXID") != null)
        lockedResultEXT032.set("EXTXID", txid)
      if (mi.in.get("ZCON") != null)
        lockedResultEXT032.set("EXZCON", zcon)
      if (mi.in.get("ZPEG") != null)
        lockedResultEXT032.set("EXZPEG", zpeg)
      if (mi.in.get("ZSAN") != null)
        lockedResultEXT032.set("EXZSAN", zsan)
      if (mi.in.get("ZAGR") != null)
        lockedResultEXT032.set("EXZAGR", zagr)
      if (mi.in.get("ZCOI") != null)
        lockedResultEXT032.set("EXZCOI", zcoi)
      if (mi.in.get("ZPHY") != null)
        lockedResultEXT032.set("EXZPHY", zphy)
      if (mi.in.get("ZLAT") != null)
        lockedResultEXT032.set("EXZLAT", zlat)
      if (mi.in.get("ZNUT") != null)
        lockedResultEXT032.set("EXZNUT", znut)
      if (mi.in.get("ZCAL") != null)
        lockedResultEXT032.set("EXZCAL", zcal)
      if (mi.in.get("ZJOU") != null)
        lockedResultEXT032.set("EXZJOU", zjou)
      if (mi.in.get("ZMAT") != null)
        lockedResultEXT032.set("EXZMAT", zmat)
      if (mi.in.get("ZAGS") != null)
        lockedResultEXT032.set("EXZAGS", zags)
      if (mi.in.get("ZGLU") != null)
        lockedResultEXT032.set("EXZGLU", zglu)
      if (mi.in.get("ZSUC") != null)
        lockedResultEXT032.set("EXZSUC", zsuc)
      if (mi.in.get("ZFIB") != null)
        lockedResultEXT032.set("EXZFIB", zfib)
      if (mi.in.get("ZPRO") != null)
        lockedResultEXT032.set("EXZPRO", zpro)
      if (mi.in.get("ZSEL") != null)
        lockedResultEXT032.set("EXZSEL", zsel)
      if (mi.in.get("ZALL") != null)
        lockedResultEXT032.set("EXZALL", zall)
      if (mi.in.get("ZAGT") != null)
        lockedResultEXT032.set("EXZAGT", zagt)
      if (mi.in.get("ZQUA") != null)
        lockedResultEXT032.set("EXZQUA", zqua)
      if (mi.in.get("ZALI") != null)
        lockedResultEXT032.set("EXZALI", zali)
      lockedResultEXT032.setInt("EXLMDT", timeOfCreation.format(DateTimeFormatter.ofPattern("yyyyMMdd")) as Integer)
      lockedResultEXT032.set("EXCHNO", ((Integer)lockedResultEXT032.get("EXCHNO") + 1))
      lockedResultEXT032.set("EXCHID", program.getUser())
      lockedResultEXT032.update()
    }

    queryEXT032.readLock(containerEXT032, updateEXT032)
  }

  Closure<?> MITPOPData = { DBContainer ContainerMITPOP ->
    String itno = ContainerMITPOP.get("MPITNO")
  }
  Closure<?> MSYTXHData = { DBContainer ContainerMSYTXH ->
  }
}