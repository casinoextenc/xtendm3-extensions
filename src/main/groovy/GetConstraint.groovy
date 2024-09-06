/**
 * README
 * This extension is used by Mashup
 * QUAX01 Gestion du référentiel qualité
 * Name : EXT030MI.GetConstraint
 * Description : Get records to the EXT030 table.
 * Date         Changed By   Description
 * 20230210     SEAR         QUAX01 - Constraints matrix
 * 20230620     FLEBARS      QUAX01 - evol contrainte
 * 20240605     FLEBARS      QUAX01 - Controle code pour validation Infor
 * 20240716     FLEBARS      QUAX01 - Controle code pour validation Infor Retours
 */

public class GetConstraint extends ExtendM3Transaction {
  private final MIAPI mi
  private final LoggerAPI logger
  private final ProgramAPI program
  private final DatabaseAPI database
  private final SessionAPI session
  private final TransactionAPI transaction
  private final MICallerAPI miCaller
  private final UtilityAPI utility

  private int currentCompany

  public GetConstraint(MIAPI mi, DatabaseAPI database, ProgramAPI program, MICallerAPI miCaller, UtilityAPI utility) {
    this.mi = mi
    this.database = database
    this.program = program
    this.miCaller = miCaller
    this.utility = utility
  }

  public void main() {
    int zcid = (mi.in.get("ZCID") != null ? (Integer) mi.in.get("ZCID") : 0)
    if (mi.in.get("CONO") == null) {
      currentCompany = (Integer) program.getLDAZD().CONO
    } else {
      currentCompany = mi.in.get("CONO")
    }

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
        "EXZOHF",
        "EXZPHY",
        "EXRGDT",
        "EXRGTM",
        "EXLMDT",
        "EXCHNO",
        "EXCHID"
      )
      .build()

    DBContainer ext030Request = ext030Query.getContainer()
    ext030Request.set("EXCONO", currentCompany)
    ext030Request.set("EXZCID", zcid)

    //Record exists
    if (!ext030Query.read(ext030Request)) {
      String constraintCode = ext030Request.get("EXZCOD")
      String constraintID = ext030Request.get("EXZCID")
      String status = ext030Request.get("EXSTAT")
      String countryCode = ext030Request.get("EXCSCD")
      String blocAssortment = ext030Request.get("EXZBLO")
      String customerCode = ext030Request.get("EXCUNO")
      String constraintTypeP = ext030Request.get("EXZCAP")
      String constraintTypeS = ext030Request.get("EXZCAS")
      String originCountry = ext030Request.get("EXORCO")
      String Sigma6 = ext030Request.get("EXPOPN")
      String Hierarchy = ext030Request.get("EXHIE0")
      String dangerous = ext030Request.get("EXHAZI")
      String statisticNumber = ext030Request.get("EXCSNO")
      String alcohol = ext030Request.get("EXZALC")
      String ruleCode = ext030Request.get("EXCFI4")
      String sanitary = ext030Request.get("EXZSAN")
      String agreementNumber = ext030Request.get("EXZNAG")
      String foodProduct = ext030Request.get("EXZALI")
      String originUE = ext030Request.get("EXZORI")
      String phytosanitary = ext030Request.get("EXZPHY")
      String entryDate = ext030Request.get("EXRGDT")
      String entryTime = ext030Request.get("EXRGTM")
      String changeDate = ext030Request.get("EXLMDT")
      String changeNumber = ext030Request.get("EXCHNO")
      String changedBy = ext030Request.get("EXCHID")
      mi.outData.put("ZCOD", constraintCode)
      mi.outData.put("ZCID", constraintID)
      mi.outData.put("STAT", status)
      mi.outData.put("CSCD", countryCode)
      mi.outData.put("ZBLO", blocAssortment)
      mi.outData.put("CUNO", customerCode)
      mi.outData.put("ZCAP", constraintTypeP)
      mi.outData.put("ZCAS", constraintTypeS)
      mi.outData.put("ORCO", originCountry)
      mi.outData.put("POPN", Sigma6)
      mi.outData.put("HIE0", Hierarchy)
      mi.outData.put("HAZI", dangerous)
      mi.outData.put("CSNO", statisticNumber)
      mi.outData.put("ZALC", alcohol)
      mi.outData.put("CFI4", ruleCode)
      mi.outData.put("ZSAN", sanitary)
      mi.outData.put("ZNAG", agreementNumber)
      mi.outData.put("ZALI", foodProduct)
      mi.outData.put("ZORI", originUE)
      mi.outData.put("ZPHY", phytosanitary)
      mi.outData.put("RGDT", entryDate)
      mi.outData.put("RGTM", entryTime)
      mi.outData.put("LMDT", changeDate)
      mi.outData.put("CHNO", changeNumber)
      mi.outData.put("CHID", changedBy)
      mi.outData.put("ZOHF", ext030Request.get("EXZOHF") as String)
      mi.write()
    } else {
      mi.error("L'enregistrement n'existe pas")
      return
    }
  }
}
