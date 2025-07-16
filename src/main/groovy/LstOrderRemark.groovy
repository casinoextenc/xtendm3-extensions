/**
 * Name : EXT013MI.LstOrderRemark
 *
 * Description :
 * This API method to list records from specific table EXT013
 *
 *
 * Date         Changed By    Description
 * 20230308     SEAR          CMD08 - Rapport d'intégration de demande
 * 20250410     ARENARD       Extension has been fixed
 */
public class LstOrderRemark extends ExtendM3Transaction {
  private final MIAPI mi
  private final DatabaseAPI database
  private final LoggerAPI logger
  private final ProgramAPI program
  private final UtilityAPI utility

  private int currentCompany
  private String errorMessage
  private Integer nbMaxRecord = 10000

  public LstOrderRemark(MIAPI mi, DatabaseAPI database, LoggerAPI logger, ProgramAPI program, UtilityAPI utility) {
    this.mi = mi
    this.database = database
    this.logger = logger
    this.program = program
    this.utility=utility
  }

  /**
   * Get mi inputs
   * Check input values
   * Check if record not exists in EXT010
   * Serialize in EXT010
   */
  public void main() {
    currentCompany = (int)program.getLDAZD().CONO

    //Get mi inputs
    String orno = (String)(mi.in.get("ORNO") != null ? mi.in.get("ORNO") : "")
    int ponr = (Integer)(mi.in.get("PONR") != null ? mi.in.get("PONR") : 0)
    int posx = (Integer)(mi.in.get("POSX") != null ? mi.in.get("POSX") : 0)
    int lino = (Integer)(mi.in.get("LINO") != null ? mi.in.get("LINO") : 0)

    String orderNumber = (mi.in.get("ORNO") != null ? (String)mi.in.get("ORNO") : "")
    String orderLineNumber = (mi.in.get("PONR") != null ? (Integer)mi.in.get("PONR") : 0)
    String suffixeLineNumber = (mi.in.get("POSX") != null ? (Integer)mi.in.get("POSX") : 0)
    String remarkLineNumber =  (mi.in.get("LINO") != null ? (String)mi.in.get("LINO") : 0)

    ExpressionFactory expression = database.getExpressionFactory("EXT013")
    expression = expression.ge("EXORNO", orderNumber)
      .and(expression.ge("EXPONR", orderLineNumber))
      .and(expression.ge("EXPOSX", suffixeLineNumber))
      .and(expression.ge("EXLINO", remarkLineNumber))

    DBAction queryEXT013 = database.table("EXT013")
      .index("00")
      .matching(expression)
      .selection(
        "EXCONO",
        "EXORNO",
        "EXPONR",
        "EXPOSX",
        "EXLINO",
        "EXFITN",
        "EXMSCD",
        "EXREMK",
        "EXRGDT",
        "EXRGTM",
        "EXLMDT",
        "EXCHNO",
        "EXCHID"
      )
      .build()

    DBContainer containerEXT013 = queryEXT013.getContainer()
    containerEXT013.set("EXCONO", currentCompany)

    //Record exists
    if (!queryEXT013.readAll(containerEXT013, 1, nbMaxRecord, outData)){
      mi.error("L'enregistrement n'existe pas")
      return
    }
  }
  // Retrieve data from EXT013
  Closure<?> outData = { DBContainer containerEXT013 ->
    String orderNumber = containerEXT013.get("EXORNO")
    String orderLineNumber = containerEXT013.get("EXPONR")
    String suffixeLineNumber = containerEXT013.get("EXPOSX")
    String remarkLineNumber = containerEXT013.get("EXLINO")
    String item = containerEXT013.get("EXFITN")
    String messageCode = containerEXT013.get("EXMSCD")
    String remark = containerEXT013.get("EXREMK")
    String entryDate = containerEXT013.get("EXRGDT")
    String entryTime = containerEXT013.get("EXRGTM")
    String changeDate = containerEXT013.get("EXLMDT")
    String changeNumber = containerEXT013.get("EXCHNO")
    String changedBy = containerEXT013.get("EXCHID")
    mi.outData.put("ORNO", orderNumber)
    mi.outData.put("PONR", orderLineNumber)
    mi.outData.put("POSX", suffixeLineNumber)
    mi.outData.put("LINO", remarkLineNumber)
    mi.outData.put("FITN", item)
    mi.outData.put("MSCD", messageCode)
    mi.outData.put("REMK", remark)
    mi.outData.put("RGDT", entryDate)
    mi.outData.put("RGTM", entryTime)
    mi.outData.put("LMDT", changeDate)
    mi.outData.put("CHNO", changeNumber)
    mi.outData.put("CHID", changedBy)
    mi.write()
  }
}
