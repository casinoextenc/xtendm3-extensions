/**
 * README
 * This extension is used by Mashup
 *
 * Name : EXT870MI.SearchTablEntry
 * Description : The SearchTablEntry transaction allow search on TX15 or TX40 from CSYTAB .
 * Date         Changed By   Description
 * 20231130     MLECLERCQ     COMX01 - Add assortment
 */
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
public class SearchTablEntry extends ExtendM3Transaction {
  private final MIAPI mi
  private final DatabaseAPI database
  private final LoggerAPI logger
  private final MICallerAPI miCaller
  private final ProgramAPI program
  private final UtilityAPI utility

  private Integer currentCompany

  private String input_divi
  private String input_stco
  private String input_lncd
  private String input_tx15
  private String input_tx40

  public SearchTablEntry(MIAPI mi, DatabaseAPI database, LoggerAPI logger, ProgramAPI program, UtilityAPI utility) {
    this.mi = mi
    this.database = database
    this.logger = logger
    this.program = program
    this.utility = utility
  }

  public void main() {

    this.input_divi = (mi.in.get("DIVI") != null ? (String)mi.in.get("DIVI") :  "")
    this.input_stco = (mi.in.get("STCO") != null ? (String)mi.in.get("STCO") :  "")
    this.input_lncd = (mi.in.get("LNCD") != null ? (String)mi.in.get("LNCD") :  "")
    this.input_tx15 = (mi.in.get("TX15") != null ? (String)mi.in.get("TX15") :  "")
    this.input_tx40 = (mi.in.get("TX40") != null ? (String)mi.in.get("TX40") :  "")

    logger.debug("Input parameters : Divi = " + this.input_divi + ",STCO = " + this.input_stco + ",LNCD = " + this.input_lncd + ",TX15 = " + this.input_tx15 + ",TX40 = " + this.input_tx40)



    if (mi.in.get("CONO") == null) {
      this.currentCompany = (Integer) program.getLDAZD().CONO
    } else {
      this.currentCompany = (Integer)mi.in.get("CONO")
    }

    if (this.input_divi == "") {
      DBAction diviQuery = database.table("CMNDIV").index("00").build()
      DBContainer CMNDIV = diviQuery.getContainer()
      CMNDIV.set("CCCONO", this.currentCompany)
      CMNDIV.set("CCDIVI", this.input_divi)
      if (!diviQuery.read(CMNDIV)) {
        mi.error("Division ${this.input_divi} n'existe pas")
        return
      }
    }

    if (this.input_stco == "") {
      logger.debug("STCO is null")
      mi.error("STCO Obligatoire")
      return
    }

    if (this.input_tx15 == "" && this.input_tx40 == "") {
      mi.error("TX15 ou TX40 doit être renseigné")
      return
    }
    if (this.input_tx15 != "" && this.input_tx40 != "") {
      mi.error("TX15 ou TX40 doit être renseigné")
      return
    }

    //Create Expression
    ExpressionFactory expression = database.getExpressionFactory("CSYTAB")
    expression = expression.eq("CTCONO", this.currentCompany.toString())
    if (this.input_tx40 != "") {
      expression = expression.and(expression.like("CTTX40", "*${this.input_tx40}*"))
    }
    if (this.input_tx15 != "") {
      expression = expression.and(expression.like("CTTX15", "*${this.input_tx15}*"))
    }
    //Run Select
    DBAction query = database.table("CSYTAB").index("60").matching(expression).selection("CTCONO", "CTDIVI", "CTLNCD", "CTSTKY", "CTTX40", "CTTX15", "CTRGDT", "CTPARM", "CTTXID", "CTRGTM", "CTLMDT", "CTCHNO", "CTCHID").build()
    DBContainer CSYTAB_container = query.getContainer()
    CSYTAB_container.set("CTCONO", this.currentCompany)
    CSYTAB_container.set("CTDIVI", this.input_divi)
    CSYTAB_container.set("CTSTCO", this.input_stco)
    CSYTAB_container.set("CTLNCD", this.input_lncd)
    if (!query.readAll(CSYTAB_container, 4, outData)) {
      mi.error("L'enregistrement n'existe pas")
      return
    }
  }
  Closure<?> outData = { DBContainer CSYTAB ->
    String cono = CSYTAB.get("CTCONO")
    String divi = CSYTAB.get("CTDIVI")
    String lncd = CSYTAB.get("CTLNCD")
    String stco = CSYTAB.get("CTSTCO")
    String stky = CSYTAB.get("CTSTKY")
    String tx15 = CSYTAB.get("CTTX15")
    String tx40 = CSYTAB.get("CTTX40")
    String txid = CSYTAB.get("CTTXID")
    String parm = CSYTAB.get("CTPARM")

    String entryDate = CSYTAB.get("CTRGDT")
    String entryTime = CSYTAB.get("CTRGTM")
    String changeDate = CSYTAB.get("CTLMDT")
    String changeNumber = CSYTAB.get("CTCHNO")
    String changedBy = CSYTAB.get("CTCHID")

    mi.outData.put("CONO", cono)
    mi.outData.put("DIVI", divi)
    mi.outData.put("STCO", stco)
    mi.outData.put("LNCD", lncd)
    mi.outData.put("STKY", stky)
    mi.outData.put("TX15", tx15)
    mi.outData.put("TX40", tx40)
    mi.outData.put("TXID", txid)
    mi.outData.put("PARM", parm)
    mi.outData.put("RGDT", entryDate)
    mi.outData.put("RGTM", entryTime)
    mi.outData.put("LMDT", changeDate)
    mi.outData.put("CHNO", changeNumber)
    mi.outData.put("CHID", changedBy)
    mi.write()
  }
}
