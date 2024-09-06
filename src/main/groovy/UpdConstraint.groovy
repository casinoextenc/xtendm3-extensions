/**
 * README
 * This extension is used by Mashup
 * QUAX01 Gestion du référentiel qualité
 * Name : EXT030MI.UpdConstraint
 * Description : Upd records to the EXT030 table.
 * Date         Changed By   Description
 * 20230210     SEAR         QUAX01 - Constraints matrix
 * 20230620     FLEBARS      QUAX01 - evol contrainte 
 * 20240605     FLEBARS      QUAX01 - Controle code pour validation Infor
 * 20240716     FLEBARS      QUAX01 - Controle code pour validation Infor Retours
 */

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

public class UpdConstraint extends ExtendM3Transaction {
  private final MIAPI mi
  private final LoggerAPI logger
  private final ProgramAPI program
  private final DatabaseAPI database
  private final MICallerAPI miCaller
  private final UtilityAPI utility

  private int currentCompany

  public UpdConstraint(MIAPI mi, DatabaseAPI database, ProgramAPI program, MICallerAPI miCaller, UtilityAPI utility, LoggerAPI logger) {
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
    int zcid = (mi.in.get("ZCID") != null ? (Integer) mi.in.get("ZCID") : 0)
    String zcod = (mi.in.get("ZCOD") != null ? (String) mi.in.get("ZCOD") : "")
    String stat = (mi.in.get("STAT") != null ? (String) mi.in.get("STAT") : "")
    int zblo = (mi.in.get("ZBLO") != null ? (Integer) mi.in.get("ZBLO") : 0)
    String cscd = (mi.in.get("CSCD") != null ? (String) mi.in.get("CSCD") : "")
    String cuno = (mi.in.get("CUNO") != null ? (String) mi.in.get("CUNO") : "")
    String zcap = (mi.in.get("ZCAP") != null ? (String) mi.in.get("ZCAP") : "")
    String zcas = (mi.in.get("ZCAS") != null ? (String) mi.in.get("ZCAS") : "")
    String orco = (mi.in.get("ORCO") != null ? (String) mi.in.get("ORCO") : "")
    String popn = (mi.in.get("POPN") != null ? (String) mi.in.get("POPN") : "")
    String hie0 = (mi.in.get("HIE0") != null ? (String) mi.in.get("HIE0") : "")
    int hazi = (mi.in.get("HAZI") != null ? (Integer) mi.in.get("HAZI") : 2)
    String csno = (mi.in.get("CSNO") != null ? (String) mi.in.get("CSNO") : "")
    int zalc = (mi.in.get("ZALC") != null ? (Integer) mi.in.get("ZALC") : 2)
    String cfi4 = (mi.in.get("CFI4") != null ? (String) mi.in.get("CFI4") : "")
    int zsan = (mi.in.get("ZSAN") != null ? (Integer) mi.in.get("ZSAN") : 2)
    String znag = (mi.in.get("ZNAG") != null ? (String) mi.in.get("ZNAG") : "")
    int zali = (mi.in.get("ZALI") != null ? (Integer) mi.in.get("ZALI") : 2)
    int zphy = (mi.in.get("ZPHY") != null ? (Integer) mi.in.get("ZPHY") : 2)
    int zori = (mi.in.get("ZORI") != null ? (Integer) mi.in.get("ZORI") : 2)
    int zohf = (mi.in.get("ZOHF") != null ? (Integer) mi.in.get("ZOHF") : 2)

    //Check if record exists in Constraint Code Table (EXT034)
    if (zcod.length() > 0 && zcod.trim() != "?") {
      DBAction ext034query = database.table("EXT034").index("00").build()
      DBContainer ext034request = ext034query.getContainer()
      ext034request.set("EXCONO", currentCompany)
      ext034request.set("EXZCOD", zcod)
      if (!ext034query.read(ext034request)) {
        mi.error("Code contrainte " + zcod + " n'existe pas")
        return
      }
    }

    // check Status
    if (stat == "") {
      stat = "10"
    }
    if (stat != "10" && stat != "20" && stat != "90") {
      mi.error("Statut autorisé : 10, 20 ou 90")
      return
    }

    // check assortment
    if (zblo != 0 && zblo != 1) {
      mi.error("L'indicateur dangerosité ZBLO doit être égal à 0 ou 1")
      return
    }

    //Check if record exists in country Code Table (EXT034)
    if (cscd.length() > 0) {
      DBAction csytabQuery = database.table("CSYTAB").index("00").build()
      DBContainer csytabRequest = csytabQuery.getContainer()
      csytabRequest.set("CTCONO", currentCompany)
      csytabRequest.set("CTSTCO", "CSCD")
      csytabRequest.set("CTSTKY", cscd)
      if (!csytabQuery.read(csytabRequest)) {
        mi.error("Code pays " + cscd + " n'existe pas")
        return
      }
    }

    //Check if record Cutomer in Customer Table (OCUSMA)
    if (cuno.length() > 0) {
      DBAction ocusmaQuery = database.table("OCUSMA").index("00").build()
      DBContainer ocusmaRequest = ocusmaQuery.getContainer()
      ocusmaRequest.set("OKCONO", currentCompany)
      ocusmaRequest.set("OKCUNO", cuno)
      if (!ocusmaQuery.read(ocusmaRequest)) {
        mi.error("Code client " + cuno + " n'existe pas")
        return
      }
    }
    //Check if record in Feature Contraint Table (EXT033)
    if (zcap.length() > 0) {
      DBAction ext033Query = database.table("EXT033").index("00").build()
      DBContainer ext033Request = ext033Query.getContainer()
      ext033Request.set("EXCONO", currentCompany)
      ext033Request.set("EXZCAR", zcap)
      if (!ext033Query.read(ext033Request)) {
        mi.error("Caractéristique de contrainte principale (EXT033) " + zcap + " n'existe pas")
        return
      }
    }

    //Check if record in Feature Contraint Table (EXT033)
    if (zcas.length() > 0) {
      DBAction ext033Query = database.table("EXT033").index("00").build()
      DBContainer ext033Request = ext033Query.getContainer()
      ext033Request.set("EXCONO", currentCompany)
      ext033Request.set("EXZCAR", zcas)
      if (!ext033Query.read(ext033Request)) {
        mi.error("Caractéristique de contrainte secondaire (EXT033) " + zcas + " n'existe pas")
        return
      }
    }

    //Check if Origine exists in country Code Table (CSYTAB)
    if (orco.length() > 0) {
      DBAction csytabQuery = database.table("CSYTAB").index("00").build()
      DBContainer csytabRequest = csytabQuery.getContainer()
      csytabRequest.set("CTCONO", currentCompany)
      csytabRequest.set("CTSTCO", "CSCD")
      csytabRequest.set("CTSTKY", orco)
      if (!csytabQuery.read(csytabRequest)) {
        mi.error("Code origine " + orco + " n'existe pas")
        return
      }
    }

    //Check if SIGMA6 exists in MITPOP
    if (popn.length() > 0) {
      ExpressionFactory mitpopExpression = database.getExpressionFactory("MITPOP")
      mitpopExpression = mitpopExpression.ge("MPREMK", "SIGMA6")
      DBAction mitpopQuery = database.table("MITPOP").index("10").matching(mitpopExpression).build()
      DBContainer mitpopRequest = mitpopQuery.getContainer()
      mitpopRequest.set("MPCONO", currentCompany)
      mitpopRequest.setInt("MPALWT", 1)
      mitpopRequest.set("MPALWQ", "")
      mitpopRequest.set("MPPOPN", popn)
      Closure<?> mitpopReader = { DBContainer mitpopResult ->
      }
      if (!mitpopQuery.readAll(mitpopRequest, 4, 1, mitpopReader)) {
        mi.error("SIGMA6 " + popn + " n'existe pas")
        return
      }
    }
    // check Dangerous
    if (hazi != 0 && hazi != 1 && hazi != 2) {
      mi.error("L'indicateur dangerosité HAZI doit être égal à 0, 1 ou 2")
      return
    }

    //Check if Origine exists in country Code Table (CSYCSN)
    if (csno.length() > 0 && !csno.contains("*")) {
      DBAction csycsnQuery = database.table("CSYCSN").index("00").build()
      DBContainer csycsnRequest = csycsnQuery.getContainer()
      csycsnRequest.set("CKCONO", currentCompany)
      csycsnRequest.set("CKCSNO", csno)
      if (!csycsnQuery.read(csycsnRequest)) {
        mi.error("Code douane " + csno + " n'existe pas")
        return
      }
    }
    // check alcohol
    if (zalc != 0 && zalc != 1 && zalc != 2) {
      mi.error("L'indicateur d'alcool ZALC doit être égal à 0, 1 ou 2")
      return
    }

    // Check control code
    if (cfi4.length() > 0) {
      DBAction csytabQuery = database.table("CSYTAB").index("00").build()
      DBContainer csytabRequest = csytabQuery.getContainer()
      csytabRequest.set("CTCONO", currentCompany)
      csytabRequest.set("CTSTCO", "CFI4")
      csytabRequest.set("CTSTKY", cfi4)
      if (!csytabQuery.read(csytabRequest)) {
        mi.error("Code régie " + cfi4 + " n'existe pas")
        return
      }
    }

    // check sanitary
    if (zsan != 0 && zsan != 1 && zsan != 2) {
      mi.error("L'indicateur sanitaire ZSAN doit être égal à 0, 1 ou 2")
      return
    }

    //Check if Origine exists in country Code Table (MITHRY)
    if (hie0.length() > 0 && !hie0.contains("*")) {
      DBAction mithryQuery = database.table("MITHRY").index("00").build()
      DBContainer mithryRequest = mithryQuery.getContainer()
      mithryRequest.set("HICONO", currentCompany)
      mithryRequest.set("HIHLVL", 5)
      mithryRequest.set("HIHIE0", hie0)
      if (!mithryQuery.read(mithryRequest)) {
        mi.error("Hierarchie " + hie0 + " n'existe pas")
        return
      }
    }

    // check food
    if (zali != 0 && zali != 1 && zali != 2) {
      mi.error("L'indicateur alimentaire ZALI doit être égal à 0, 1 ou 2")
      return
    }

    // check PhytoSanitary
    if (zphy != 0 && zphy != 1 && zphy != 2) {
      mi.error("L'indicateur Phytosanitaire ZPHY doit être égal à 0, 1 ou 2")
      return
    }

    // check Origin UE
    if (zori != 0 && zori != 1 && zori != 2) {
      mi.error("L'indicateur origine UE ZORI doit être égal à 0, 1 ou 2")
      return
    }
    if (zohf != 0 && zohf != 1 && zohf != 2) {
      mi.error("L'indicateur hors France ZOHF doit être égal à 0, 1 ou 2")
      return
    }

    LocalDateTime timeOfCreation = LocalDateTime.now()

    //Check if record exists
    DBAction ext030Query = database.table("EXT030")
      .index("00")
      .selection(
        "EXZCID",
        "EXZCOD",
        "EXSTAT",
        "EXZBLO",
        "EXCSCD",
        "EXCUNO",
        "EXZCAP",
        "EXZCAS",
        "EXORCO",
        "EXPOPN",
        "EXHIE0",
        "EXHAZI",
        "EXCSNO",
        "EXZALC",
        "EXCFI4",
        "EXZSAN",
        "EXZNAG",
        "EXZALI",
        "EXZORI",
        "EXZPHY",
        "EXRGDT",
        "EXLMDT",
        "EXCHNO",
        "EXCHID"
      )
      .build()

    DBContainer ext030Request = ext030Query.getContainer()
    ext030Request.set("EXCONO", currentCompany)
    ext030Request.set("EXZCID", zcid)

    Closure<?> ext030Updater = { LockedResult ext030lockedResult ->
      ext030lockedResult.set("EXSTAT", stat)
      ext030lockedResult.set("EXZBLO", zblo)
      if (mi.in.get("ZCOD") != null)
        ext030lockedResult.set("EXZCOD", zcod)
      if (mi.in.get("CSCD") != null)
        ext030lockedResult.set("EXCSCD", cscd)
      if (mi.in.get("CUNO") != null)
        ext030lockedResult.set("EXCUNO", cuno)
      if (mi.in.get("ZCAP") != null)
        ext030lockedResult.set("EXZCAP", zcap)
      if (mi.in.get("ZCAS") != null)
        ext030lockedResult.set("EXZCAS", zcas)
      if (mi.in.get("ORCO") != null)
        ext030lockedResult.set("EXORCO", orco)
      if (mi.in.get("POPN") != null)
        ext030lockedResult.set("EXPOPN", popn)
      if (mi.in.get("HIE0") != null)
        ext030lockedResult.set("EXHIE0", hie0)
      if (mi.in.get("HAZI") != null)
        ext030lockedResult.set("EXHAZI", hazi)
      if (mi.in.get("CSNO") != null)
        ext030lockedResult.set("EXCSNO", csno)
      if (mi.in.get("ZALC") != null)
        ext030lockedResult.set("EXZALC", zalc)
      if (mi.in.get("CFI4") != null)
        ext030lockedResult.set("EXCFI4", cfi4)
      if (mi.in.get("ZSAN") != null)
        ext030lockedResult.set("EXZSAN", zsan)
      if (mi.in.get("ZNAG") != null)
        ext030lockedResult.set("EXZNAG", znag)
      if (mi.in.get("ZALI") != null)
        ext030lockedResult.set("EXZALI", zali)
      if (mi.in.get("ZPHY") != null)
        ext030lockedResult.set("EXZPHY", zphy)
      if (mi.in.get("ZORI") != null)
        ext030lockedResult.set("EXZORI", zori)

      ext030lockedResult.set("EXZOHF", zohf)
      ext030lockedResult.setInt("EXLMDT", timeOfCreation.format(DateTimeFormatter.ofPattern("yyyyMMdd")) as Integer)
      ext030lockedResult.set("EXCHNO", ((Integer) ext030lockedResult.get("EXCHNO") + 1))
      ext030lockedResult.set("EXCHID", program.getUser())
      ext030lockedResult.update()
    }

    if (!ext030Query.readLock(ext030Request, ext030Updater)){
      mi.error("L'enregistrement n'existe pas")
      return
    }
  }
}
