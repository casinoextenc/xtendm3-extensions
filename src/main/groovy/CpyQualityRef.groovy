/**
 * README
 * This extension is used by Mashup
 * 
 * Name : EXT032MI.CpyQualityRef
 * Description : Copy records to the EXT032 table.
 * Date         Changed By   Description
 * 20230125     SEAR         QUAX01 - Constraints matrix 
 */
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

public class CpyQualityRef extends ExtendM3Transaction {
  private final MIAPI mi
  private final LoggerAPI logger
  private final ProgramAPI program
  private final DatabaseAPI database
  private final SessionAPI session
  private final TransactionAPI transaction

  public CpyQualityRef(MIAPI mi, DatabaseAPI database, ProgramAPI program) {
    this.mi = mi
    this.database = database
    this.program = program
  }

  public void main() {
    Integer currentCompany
    String suno = (mi.in.get("SUNO") != null ? (String)mi.in.get("SUNO") : "")
    String popn = (mi.in.get("POPN") != null ? (String)mi.in.get("POPN") : "")
    String orco = (mi.in.get("ORCO") != null ? (String)mi.in.get("ORCO") : "")
    String zsun = (mi.in.get("ZSUN") != null ? (String)mi.in.get("ZSUN") : "")
    String zpop = (mi.in.get("ZPOP") != null ? (String)mi.in.get("ZPOP") : "")
    String zorc = (mi.in.get("ZORC") != null ? (String)mi.in.get("ZORC") : "")
    if (mi.in.get("CONO") == null) {
      currentCompany = (Integer)program.getLDAZD().CONO
    } else {
      currentCompany = mi.in.get("CONO")
    }

    //Check if SIGMA6 exists in MITPOP
    if (zpop.length() > 0) {
      ExpressionFactory expression = database.getExpressionFactory("MITPOP")
      expression = expression.ge("MPREMK", "SIGMA6")
      DBAction queryMITPOP = database.table("MITPOP").index("10").matching(expression).build()
      DBContainer ContainerMITPOP = queryMITPOP.getContainer()
      ContainerMITPOP.set("MPCONO", currentCompany)
      ContainerMITPOP.setInt("MPALWT", 1)
      ContainerMITPOP.set("MPALWQ", "")
      ContainerMITPOP.set("MPPOPN", zpop)
      if (!queryMITPOP.readAll(ContainerMITPOP, 4, MITPOPData)) {
        mi.error("SIGMA6 (to) " + zpop + " n'existe pas")
        return
      }
    }

    //Check if record existe in supplier Table (CIDMAS)
    if (zsun.length() > 0) {
      DBAction queryCIDMAS = database.table("CIDMAS").index("00").build()
      DBContainer ContainerCIDMAS = queryCIDMAS.getContainer()
      ContainerCIDMAS.set("IDCONO", currentCompany)
      ContainerCIDMAS.set("IDSUNO", zsun)
      if (!queryCIDMAS.read(ContainerCIDMAS)) {
        mi.error("Code fournisseur (to) " + zsun + " n'existe pas")
        return
      }
    }

    //Check if record exists in country Code Table (EXT034)
    if (zorc.length() > 0) {
      DBAction queryCSYTAB = database.table("CSYTAB").index("00").build()
      DBContainer ContainerCSYTAB = queryCSYTAB.getContainer()
      ContainerCSYTAB.set("CTCONO", currentCompany)
      ContainerCSYTAB.set("CTSTCO", "CSCD")
      ContainerCSYTAB.set("CTSTKY", zorc)
      if (!queryCSYTAB.read(ContainerCSYTAB)) {
        mi.error("Code pays origine (to) " + zorc + " n'existe pas")
        return
      }
    }

    LocalDateTime timeOfCreation = LocalDateTime.now()
    DBAction query = database.table("EXT032").index("00").selection(
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
    DBContainer EXT032 = query.getContainer()
    EXT032.set("EXCONO", currentCompany)
    EXT032.set("EXPOPN", mi.in.get("POPN"))
    EXT032.set("EXSUNO", mi.in.get("SUNO"))
    EXT032.set("EXORCO", mi.in.get("ORCO"))
    if(query.read(EXT032)){
      EXT032.set("EXPOPN", mi.in.get("ZPOP"))
      EXT032.set("EXSUNO", mi.in.get("ZSUN"))
      EXT032.set("EXORCO", mi.in.get("ZORC"))
      if (!query.read(EXT032)) {
        EXT032.setInt("EXRGDT", timeOfCreation.format(DateTimeFormatter.ofPattern("yyyyMMdd")) as Integer)
        EXT032.setInt("EXRGTM", timeOfCreation.format(DateTimeFormatter.ofPattern("HHmmss")) as Integer)
        EXT032.setInt("EXLMDT", timeOfCreation.format(DateTimeFormatter.ofPattern("yyyyMMdd")) as Integer)
        EXT032.setInt("EXCHNO", 1)
        EXT032.set("EXCHID", program.getUser())
        query.insert(EXT032)
      } else {
        mi.error("L'enregistrement existe déjà")
      }
    } else {
      mi.error("L'enregistrement n'existe pas")
      return
    }
  }
  Closure<?> MITPOPData = { DBContainer ContainerMITPOP ->
    String itno = ContainerMITPOP.get("MPITNO")
  }
}
