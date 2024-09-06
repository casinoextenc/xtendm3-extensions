/**
 * README
 * This extension is used by Mashup
 * QUAX01 Gestion du référentiel qualité
 * Name : EXT030MI.LstConstraint
 * Description : List records to the EXT030 table.
 * Date         Changed By   Description
 * 20230210     SEAR         QUAX01 - Constraints matrix
 * 20230620     FLEBARS      QUAX01 - evol contrainte
 * 20240605     FLEBARS      QUAX01 - Controle code pour validation Infor
 * 20240716     FLEBARS      QUAX01 - Controle code pour validation Infor Retours
 */

public class LstConstraint extends ExtendM3Transaction {
  private final MIAPI mi
  private final LoggerAPI logger
  private final ProgramAPI program
  private final DatabaseAPI database
  private final SessionAPI session
  private final TransactionAPI transaction
  private final MICallerAPI miCaller
  private final UtilityAPI utility

  private int currentCompany

  public LstConstraint(MIAPI mi, DatabaseAPI database, ProgramAPI program, MICallerAPI miCaller, UtilityAPI utility) {
    this.mi = mi
    this.database = database
    this.program = program
    this.miCaller = miCaller
    this.utility = utility
  }

  public void main() {
    int zcid = (mi.in.get("ZCID") != null ? (Integer) mi.in.get("ZCID") : 0)
    String zcod = (mi.in.get("ZCOD") != null ? (String) mi.in.get("ZCOD") : "")
    String stat = (mi.in.get("STAT") != null ? (String) mi.in.get("STAT") : "")
    int zblo = (mi.in.get("ZBLO") != null ? (Integer) mi.in.get("ZBLO") : 0)
    String cscd = (mi.in.get("CSCD") != null ? (String) mi.in.get("CSCD") : "")
    String cuno = (mi.in.get("CUNO") != null ? (String) mi.in.get("CUNO") : "")
    if (mi.in.get("CONO") == null) {
      currentCompany = (Integer) program.getLDAZD().CONO
    } else {
      currentCompany = mi.in.get("CONO")
    }

    //Check if record exists
    String constraintIDEx = (mi.in.get("ZCID") != null ? (Integer) mi.in.get("ZCID") : 0)
    String constraintCode = (String) (mi.in.get("ZCOD") != null ? mi.in.get("ZCOD") : "")
    String countryCode = (String) (mi.in.get("CSCD") != null ? mi.in.get("CSCD") : "")
    String status = (String) (mi.in.get("STAT") != null ? mi.in.get("STAT") : "")
    String customer = (String) (mi.in.get("CUNO") != null ? mi.in.get("CUNO") : "")
    String assortmentBloc = (mi.in.get("ZBLO") != null ? (Integer) mi.in.get("ZBLO") : 0)

    ExpressionFactory ext030Expression = database.getExpressionFactory("EXT030")
    ext030Expression = ext030Expression.ge("EXZCID", constraintIDEx)
      .and(ext030Expression.ge("EXZCOD", constraintCode))
      .and(ext030Expression.ge("EXSTAT", status))
      .and(ext030Expression.ge("EXZBLO", assortmentBloc))
      .and(ext030Expression.ge("EXCSCD", countryCode))
      .and(ext030Expression.ge("EXCUNO", customer))

    DBAction ext030Query = database.table("EXT030")
      .index("00")
      .matching(ext030Expression)
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
        "EXRGTM",
        "EXLMDT",
        "EXCHNO",
        "EXZOHF",
        "EXCHID"
      )
      .build()

    DBContainer ext030Request = ext030Query.getContainer()
    ext030Request.set("EXCONO", currentCompany)
    if (!ext030Query.readAll(ext030Request, 1, 10000, ext030Reader)) {
      mi.error("L'enregistrement n'existe pas")
      return
    }
  }

  Closure<?> ext030Reader = { DBContainer ext030Result ->
    String constraintCode = ext030Result.get("EXZCOD")
    String constraintID = ext030Result.get("EXZCID")
    String status = ext030Result.get("EXSTAT")
    String countryCode = ext030Result.get("EXCSCD")
    String blocAssortment = ext030Result.get("EXZBLO")
    String customerCode = ext030Result.get("EXCUNO")
    String constraintTypeP = ext030Result.get("EXZCAP")
    String constraintTypeS = ext030Result.get("EXZCAS")
    String originCountry = ext030Result.get("EXORCO")
    String Sigma6 = ext030Result.get("EXPOPN")
    String Hierarchy = ext030Result.get("EXHIE0")
    String dangerous = ext030Result.get("EXHAZI")
    String statisticNumber = ext030Result.get("EXCSNO")
    String alcohol = ext030Result.get("EXZALC")
    String ruleCode = ext030Result.get("EXCFI4")
    String sanitary = ext030Result.get("EXZSAN")
    String agreementNumber = ext030Result.get("EXZNAG")
    String foodProduct = ext030Result.get("EXZALI")
    String originUE = ext030Result.get("EXZORI")
    String phytosanitary = ext030Result.get("EXZPHY")
    String entryDate = ext030Result.get("EXRGDT")
    String entryTime = ext030Result.get("EXRGTM")
    String changeDate = ext030Result.get("EXLMDT")
    String changeNumber = ext030Result.get("EXCHNO")
    String changedBy = ext030Result.get("EXCHID")
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
    mi.outData.put("ZOHF", ext030Result.get("EXZOHF") as String)
    mi.write()
  }
}
