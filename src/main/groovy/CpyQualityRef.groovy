/**
 * README
 * This extension is used by Mashup
 * QUAX01 Gestion du référentiel qualité
 * Name : EXT032MI.CpyQualityRef
 * Description : Copy records to the EXT032 table.
 * Date         Changed By   Description
 * 20230125     SEAR         QUAX01 - Constraints matrix 
 * 20240605     FLEBARS      QUAX01 - Controle code pour validation Infor
 */
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

public class CpyQualityRef extends ExtendM3Transaction {
  private final MIAPI mi
  private final LoggerAPI logger
  private final ProgramAPI program
  private final DatabaseAPI database

  private int currentCompany

  public CpyQualityRef(MIAPI mi, DatabaseAPI database, ProgramAPI program, LoggerAPI logger) {
    this.mi = mi
    this.database = database
    this.program = program
    this.logger = logger
  }

  public void main() {
    String suno = (mi.in.get("SUNO") != null ? (String) mi.in.get("SUNO") : "")
    String popn = (mi.in.get("POPN") != null ? (String) mi.in.get("POPN") : "")
    String orco = (mi.in.get("ORCO") != null ? (String) mi.in.get("ORCO") : "")
    String zsun = (mi.in.get("ZSUN") != null ? (String) mi.in.get("ZSUN") : "")
    String zpop = (mi.in.get("ZPOP") != null ? (String) mi.in.get("ZPOP") : "")
    String zorc = (mi.in.get("ZORC") != null ? (String) mi.in.get("ZORC") : "")
    if (mi.in.get("CONO") == null) {
      currentCompany = (Integer) program.getLDAZD().CONO
    } else {
      currentCompany = mi.in.get("CONO")
    }

    //Check if SIGMA6 exists in MITPOP
    if (zpop.length() > 0) {
      ExpressionFactory mitpopExpression = database.getExpressionFactory("MITPOP")
      mitpopExpression = mitpopExpression.ge("MPREMK", "SIGMA6")

      DBAction mitpopQuery = database.table("MITPOP").index("10").matching(mitpopExpression).build()
      DBContainer mitpopRequest = mitpopQuery.getContainer()
      mitpopRequest.set("MPCONO", currentCompany)
      mitpopRequest.setInt("MPALWT", 1)
      mitpopRequest.set("MPALWQ", "")
      mitpopRequest.set("MPPOPN", zpop)
      Closure<?> mitpopReader = { DBContainer mitpopResult ->
      }

      if (!mitpopQuery.readAll(mitpopRequest, 4, 1, mitpopReader)) {
        mi.error("SIGMA6 (to) " + zpop + " n'existe pas")
        return
      }
    }

    //Check if record existe in supplier Table (CIDMAS)
    if (zsun.length() > 0) {
      DBAction cidmasQuery = database.table("CIDMAS").index("00").build()
      DBContainer cidmasRequest = cidmasQuery.getContainer()
      cidmasRequest.set("IDCONO", currentCompany)
      cidmasRequest.set("IDSUNO", zsun)
      if (!cidmasQuery.read(cidmasRequest)) {
        mi.error("Code fournisseur (to) " + zsun + " n'existe pas")
        return
      }
    }

    //Check if record exists in country Code Table (EXT034)
    if (zorc.length() > 0) {
      DBAction csytabQuery = database.table("CSYTAB").index("00").build()
      DBContainer csytabRequest = csytabQuery.getContainer()
      csytabRequest.set("CTCONO", currentCompany)
      csytabRequest.set("CTSTCO", "CSCD")
      csytabRequest.set("CTSTKY", zorc)
      if (!csytabQuery.read(csytabRequest)) {
        mi.error("Code pays origine (to) " + zorc + " n'existe pas")
        return
      }
    }

    LocalDateTime timeOfCreation = LocalDateTime.now()
    DBAction ext032Query = database.table("EXT032").index("00").selection(
      "EXSUNO",
      "EXPOPN",
      "EXORCO",
      "EXZORI",
      "EXZALC",
      "EXZCA1",
      "EXZCA2",
      "EXZCA3",
      "EXZCA4",
      "EXZCA5",
      "EXZCA6",
      "EXZCA7",
      "EXZCA8",
      "EXTXID",
      "EXZCON",
      "EXZPEG",
      "EXZSAN",
      "EXZAGR",
      "EXZCOI",
      "EXZPHY",
      "EXZLAT",
      "EXZNUT",
      "EXZCAL",
      "EXZJOU",
      "EXZMAT",
      "EXZAGS",
      "EXZGLU",
      "EXZSUC",
      "EXZFIB",
      "EXZPRO",
      "EXZSEL",
      "EXZALL",
      "EXZAGT",
      "EXZQUA",
      "EXZALI",
      "EXRGDT",
      "EXRGTM",
      "EXLMDT",
      "EXCHNO",
      "EXCHID")
      .build()
    DBContainer ext032Request = ext032Query.getContainer()
    ext032Request.set("EXCONO", currentCompany)
    ext032Request.set("EXPOPN", mi.in.get("POPN"))
    ext032Request.set("EXSUNO", mi.in.get("SUNO"))
    ext032Request.set("EXORCO", mi.in.get("ORCO"))
    if (ext032Query.read(ext032Request)) {
      ext032Request.set("EXPOPN", mi.in.get("ZPOP"))
      ext032Request.set("EXSUNO", mi.in.get("ZSUN"))
      ext032Request.set("EXORCO", mi.in.get("ZORC"))
      if (!ext032Query.read(ext032Request)) {
        ext032Request.setInt("EXRGDT", timeOfCreation.format(DateTimeFormatter.ofPattern("yyyyMMdd")) as Integer)
        ext032Request.setInt("EXRGTM", timeOfCreation.format(DateTimeFormatter.ofPattern("HHmmss")) as Integer)
        ext032Request.setInt("EXLMDT", timeOfCreation.format(DateTimeFormatter.ofPattern("yyyyMMdd")) as Integer)
        ext032Request.setInt("EXCHNO", 1)
        ext032Request.set("EXCHID", program.getUser())
        ext032Query.insert(ext032Request)
      } else {
        mi.error("L'enregistrement existe déjà")
      }
    } else {
      mi.error("L'enregistrement n'existe pas")
    }
  }
}
