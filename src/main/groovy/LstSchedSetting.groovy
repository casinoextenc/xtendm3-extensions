/**
 * README
 * This extension is used by Mashup
 *
 * Name : EXT820MI.LstSchedSetting
 * Description : List records from the EXT820 table.
 * Date         Changed By   Description
 * 20211130     RENARN       INTX99 Batch submission
 */
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.concurrent.TimeUnit

public class LstSchedSetting extends ExtendM3Transaction {
  private final MIAPI mi;
  private final DatabaseAPI database
  private final LoggerAPI logger
  private final ProgramAPI program
  private String joid = ""
  private String usid = ""
  private String xvsn = ""
  private String tx30 = ""
  private String tx60 = ""
  private String xcat = ""
  private String scty = ""
  private String xnow = ""
  private String xtod = ""
  private String xnmo = ""
  private String xntu = ""
  private String xnwe = ""
  private String xnth = ""
  private String xnfr = ""
  private String xnsa = ""
  private String xnsu = ""
  private String xemo = ""
  private String xetu = ""
  private String xewe = ""
  private String xeth = ""
  private String xefr = ""
  private String xesa = ""
  private String xesu = ""
  private String xemt = ""
  private String xrdy = ""
  private String xjdt = ""
  private String xjtm = ""
  private String xsti = ""
  private String fdat = ""
  private String tdat = ""
  private String jsca = ""
  private String petp = ""
  private String tizo = ""

  public LstSchedSetting(MIAPI mi, DatabaseAPI database, LoggerAPI logger, ProgramAPI program) {
    this.mi = mi;
    this.database = database
    this.logger = logger
    this.program = program
  }

  public void main() {
    Integer currentCompany
    if (mi.in.get("CONO") == null) {
      currentCompany = (Integer)program.getLDAZD().CONO
    } else {
      currentCompany = mi.in.get("CONO")
    }
    if (mi.in.get("JOID") == null) {
      DBAction query = database.table("EXT820").index("00").selection("EXJOID","EXUSID","EXXVSN","EXTX30","EXTX60","EXXCAT","EXSCTY","EXXNOW","EXXTOD","EXXNMO","EXXNTU","EXXNWE","EXXNTH","EXXNFR","EXXNSA","EXXNSU","EXXEMO","EXXETU","EXXEWE","EXXETH","EXXEFR","EXXESA","EXXESU","EXXEMT","EXXRDY","EXXJDT","EXXJTM","EXXSTI","EXFDAT","EXTDAT","EXJSCA","EXPETP","EXTIZO","EXRGDT","EXRGTM","EXLMDT","EXCHNO","EXCHID").build()
      DBContainer EXT820 = query.getContainer()
      EXT820.set("EXCONO", currentCompany)
      if(!query.readAll(EXT820, 1, outData)){
        mi.error("L'enregistrement n'existe pas")
        return
      }
    } else {
      joid = mi.in.get("JOID")
      ExpressionFactory expression = database.getExpressionFactory("EXT820")
      expression = expression.ge("EXJOID", joid)
      DBAction query = database.table("EXT820").index("00").matching(expression).selection("EXJOID","EXUSID","EXXVSN","EXTX30","EXTX60","EXXCAT","EXSCTY","EXXNOW","EXXTOD","EXXNMO","EXXNTU","EXXNWE","EXXNTH","EXXNFR","EXXNSA","EXXNSU","EXXEMO","EXXETU","EXXEWE","EXXETH","EXXEFR","EXXESA","EXXESU","EXXEMT","EXXRDY","EXXJDT","EXXJTM","EXXSTI","EXFDAT","EXTDAT","EXJSCA","EXPETP","EXTIZO","EXRGDT","EXRGTM","EXLMDT","EXCHNO","EXCHID").build()
      DBContainer EXT820 = query.getContainer()
      EXT820.set("EXCONO", currentCompany)
      if(!query.readAll(EXT820, 1, outData)){
        mi.error("L'enregistrement n'existe pas")
        return
      }
    }
  }

  Closure<?> outData = { DBContainer EXT820 ->
    joid = EXT820.get("EXJOID")
    usid = EXT820.get("EXUSID")
    xvsn = EXT820.get("EXXVSN")
    tx30 = EXT820.get("EXTX30")
    tx60 = EXT820.get("EXTX60")
    xcat = EXT820.get("EXXCAT")
    scty = EXT820.get("EXSCTY")
    xnow = EXT820.get("EXXNOW")
    xtod = EXT820.get("EXXTOD")
    xnmo = EXT820.get("EXXNMO")
    xntu = EXT820.get("EXXNTU")
    xnwe = EXT820.get("EXXNWE")
    xnth = EXT820.get("EXXNTH")
    xnfr = EXT820.get("EXXNFR")
    xnsa = EXT820.get("EXXNSA")
    xnsu = EXT820.get("EXXNSU")
    xemo = EXT820.get("EXXEMO")
    xetu = EXT820.get("EXXETU")
    xewe = EXT820.get("EXXEWE")
    xeth = EXT820.get("EXXETH")
    xefr = EXT820.get("EXXEFR")
    xesa = EXT820.get("EXXESA")
    xesu = EXT820.get("EXXESU")
    xemt = EXT820.get("EXXEMT")
    xrdy = EXT820.get("EXXRDY")
    xjdt = EXT820.get("EXXJDT")
    xjtm = EXT820.get("EXXJTM")
    xsti = EXT820.get("EXXSTI")
    fdat = EXT820.get("EXFDAT")
    tdat = EXT820.get("EXTDAT")
    jsca = EXT820.get("EXJSCA")
    petp = EXT820.get("EXPETP")
    tizo = EXT820.get("EXTIZO")
    String entryDate = EXT820.get("EXRGDT")
    String entryTime = EXT820.get("EXRGTM")
    String changeDate = EXT820.get("EXLMDT")
    String changeNumber = EXT820.get("EXCHNO")
    String changedBy = EXT820.get("EXCHID")
    mi.outData.put("JOID", joid)
    mi.outData.put("USID", usid)
    mi.outData.put("XVSN", xvsn)
    mi.outData.put("TX30", tx30)
    mi.outData.put("TX60", tx60)
    mi.outData.put("XCAT", xcat)
    mi.outData.put("SCTY", scty)
    mi.outData.put("XNOW", xnow)
    mi.outData.put("XTOD", xtod)
    mi.outData.put("XNMO", xnmo)
    mi.outData.put("XNTU", xntu)
    mi.outData.put("XNWE", xnwe)
    mi.outData.put("XNTH", xnth)
    mi.outData.put("XNFR", xnfr)
    mi.outData.put("XNSA", xnsa)
    mi.outData.put("XNSU", xnsu)
    mi.outData.put("XEMO", xemo)
    mi.outData.put("XETU", xetu)
    mi.outData.put("XEWE", xewe)
    mi.outData.put("XETH", xeth)
    mi.outData.put("XEFR", xefr)
    mi.outData.put("XESA", xesa)
    mi.outData.put("XESU", xesu)
    mi.outData.put("XEMT", xemt)
    mi.outData.put("XRDY", xrdy)
    mi.outData.put("XJDT", xjdt)
    mi.outData.put("XJTM", xjtm)
    mi.outData.put("XSTI", xsti)
    mi.outData.put("FDAT", fdat)
    mi.outData.put("TDAT", tdat)
    mi.outData.put("JSCA", jsca)
    mi.outData.put("PETP", petp)
    mi.outData.put("TIZO", tizo)
    mi.outData.put("RGDT", entryDate)
    mi.outData.put("RGTM", entryTime)
    mi.outData.put("LMDT", changeDate)
    mi.outData.put("CHNO", changeNumber)
    mi.outData.put("CHID", changedBy)
    mi.write()
  }
}
