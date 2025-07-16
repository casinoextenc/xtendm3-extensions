/**
 * README
 * This extension is used by Mashup
 *
 * Name : EXT870MI.SearchTablEntry
 * Description : The SearchTablEntry transaction allow search on TX15 or TX40 from CSYTAB .
 * Date         Changed By   Description
 * 20231130     MLECLERCQ     COMX01 - Add assortment
 * 20240701     PBEAUDOUIN    For validation Xtend
 */
public class SearchTablEntry extends ExtendM3Transaction {
  private final MIAPI mi
  private final DatabaseAPI database
  private final LoggerAPI logger
  private final ProgramAPI program
  private final UtilityAPI utility

  private Integer currentCompany

  private String iDivi
  private String iStco
  private String iLncd
  private String iTx15
  private String iTx40
  private Integer nbMaxRecord = 10000

  public SearchTablEntry(MIAPI mi, DatabaseAPI database, LoggerAPI logger, ProgramAPI program, UtilityAPI utility) {
    this.mi = mi
    this.database = database
    this.logger = logger
    this.program = program
    this.utility = utility
  }

  public void main() {

    this.iDivi = (mi.in.get("DIVI") != null ? (String) mi.in.get("DIVI") : "")
    this.iStco = (mi.in.get("STCO") != null ? (String) mi.in.get("STCO") : "")
    this.iLncd = (mi.in.get("LNCD") != null ? (String) mi.in.get("LNCD") : "")
    this.iTx15 = (mi.in.get("TX15") != null ? (String) mi.in.get("TX15") : "")
    this.iTx40 = (mi.in.get("TX40") != null ? (String) mi.in.get("TX40") : "")


    if (mi.in.get("CONO") == null) {
      this.currentCompany = (Integer) program.getLDAZD().CONO
    } else {
      this.currentCompany = (Integer) mi.in.get("CONO")
    }

    if (this.iDivi == "") {
      DBAction diviQuery = database.table("CMNDIV").index("00").build()
      DBContainer CMNDIV = diviQuery.getContainer()
      CMNDIV.set("CCCONO", this.currentCompany)
      CMNDIV.set("CCDIVI", this.iDivi)
      if (!diviQuery.read(CMNDIV)) {
        mi.error("Division ${this.iDivi} n'existe pas")
        return
      }
    }

    if (this.iStco == "") {
      mi.error("STCO Obligatoire")
      return
    }

    if (this.iTx15 == "" && this.iTx40 == "") {
      mi.error("TX15 ou TX40 doit être renseigné")
      return
    }
    if (this.iTx15 != "" && this.iTx40 != "") {
      mi.error("TX15 ou TX40 doit être renseigné")
      return
    }

    //Create Expression
    ExpressionFactory expression = database.getExpressionFactory("CSYTAB")
    expression = expression.eq("CTCONO", this.currentCompany.toString())
    if (this.iTx40 != "") {
      expression = expression.and(expression.like("CTTX40", "*${this.iTx40}*"))
    }
    if (this.iTx15 != "") {
      expression = expression.and(expression.like("CTTX15", "*${this.iTx15}*"))
    }
    //Run Select
    DBAction csytabQuery = database.table("CSYTAB").index("60").matching(expression).selection("CTCONO", "CTDIVI", "CTLNCD", "CTSTKY", "CTTX40", "CTTX15", "CTRGDT", "CTPARM", "CTTXID", "CTRGTM", "CTLMDT", "CTCHNO", "CTCHID").build()
    DBContainer csytabRequest = csytabQuery.getContainer()
    csytabRequest.set("CTCONO", this.currentCompany)
    csytabRequest.set("CTDIVI", this.iDivi)
    csytabRequest.set("CTSTCO", this.iStco)
    csytabRequest.set("CTLNCD", this.iLncd)
    if (!csytabQuery.readAll(csytabRequest, 4, nbMaxRecord, csytabReader)) {
      mi.error("L'enregistrement n'existe pas")
      return
    }
  }
  // Write outData
  Closure<?> csytabReader = { DBContainer csytabResult ->
    String cono = csytabResult.get("CTCONO")
    String divi = csytabResult.get("CTDIVI")
    String lncd = csytabResult.get("CTLNCD")
    String stco = csytabResult.get("CTSTCO")
    String stky = csytabResult.get("CTSTKY")
    String tx15 = csytabResult.get("CTTX15")
    String tx40 = csytabResult.get("CTTX40")
    String txid = csytabResult.get("CTTXID")
    String parm = csytabResult.get("CTPARM")

    String entryDate = csytabResult.get("CTRGDT")
    String entryTime = csytabResult.get("CTRGTM")
    String changeDate = csytabResult.get("CTLMDT")
    String changeNumber = csytabResult.get("CTCHNO")
    String changedBy = csytabResult.get("CTCHID")

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
