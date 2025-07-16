/**
 * Name : EXT012MI.GetEarliestDate
 *
 * Description :
 * APP02 Planning GDA Get the earliest date in EXT012
 *
 *
 * Date         Changed By    Description
 * 20230308     SEAR          APP02 - Planning GDA
 * 20240911     FLEBARS       Revue code pour validation
 */
public class GetEarliestDate extends ExtendM3Transaction {
  private final MIAPI mi
  private final DatabaseAPI database
  private final LoggerAPI logger
  private final ProgramAPI program
  private final UtilityAPI utility

  private int currentCompany

  public GetEarliestDate(MIAPI mi, DatabaseAPI database, LoggerAPI logger, ProgramAPI program, UtilityAPI utility) {
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
    String asgd = (String)(mi.in.get("ASGD") != null ? mi.in.get("ASGD") : "")
    String cuno = (String)(mi.in.get("CUNO") != null ? mi.in.get("CUNO") : "")
    String suno = (String)(mi.in.get("SUNO") != null ? mi.in.get("SUNO") : "")
    int drgd = (Integer)(mi.in.get("DRGD") != null ? mi.in.get("DRGD") : 0)
    int hrgd = (Integer)(mi.in.get("HRGD") != null ? mi.in.get("HRGD") : 0)

    String pickupDate = (mi.in.get("DRGD") != null ? (Integer)mi.in.get("DRGD") : 0)

    ExpressionFactory expression = database.getExpressionFactory("EXT012")
    expression = expression.gt("EXDRGD", pickupDate)

    DBAction ext012Query = database.table("EXT012")
      .index("00")
      .matching(expression)
      .selection(
        "EXCONO",
        "EXCUNO",
        "EXSUNO",
        "EXASGD",
        "EXDRGD",
        "EXHRGD",
        "EXDLGD",
        "EXRGDT",
        "EXRGTM",
        "EXLMDT",
        "EXCHNO",
        "EXCHID"
      )
      .build()

    DBContainer ext012Request = ext012Query.getContainer()
    ext012Request.set("EXCONO", currentCompany)
    ext012Request.set("EXCUNO", cuno)
    ext012Request.set("EXSUNO", suno)
    ext012Request.set("EXASGD", asgd)
    ext012Request.set("EXDRGD", drgd)


    //Record exists
    if (!ext012Query.readAll(ext012Request, 5, 1,ext012OutData)){
      mi.error("L'enregistrement n'existe pas")
      return
    }
  }
  /**
   * Write outData
   */
  Closure<?> ext012OutData = { DBContainer ext012Result ->
    String customerCode = ext012Result.get("EXCUNO")
    String supplierCode = ext012Result.get("EXSUNO")
    String Assortment = ext012Result.get("EXASGD")
    String pickupDate = ext012Result.get("EXDRGD")
    String pickuptHour = ext012Result.get("EXHRGD")
    String deliveryDate = ext012Result.get("EXDLGD")
    String entryDate = ext012Result.get("EXRGDT")
    String entryTime = ext012Result.get("EXRGTM")
    String changeDate = ext012Result.get("EXLMDT")
    String changeNumber = ext012Result.get("EXCHNO")
    String changedBy = ext012Result.get("EXCHID")
    mi.outData.put("CUNO", customerCode)
    mi.outData.put("SUNO", supplierCode)
    mi.outData.put("ASGD", Assortment)
    mi.outData.put("DRGD", pickupDate)
    mi.outData.put("HRGD", pickuptHour)
    mi.outData.put("DLGD", deliveryDate)
    mi.outData.put("RGDT", entryDate)
    mi.outData.put("RGTM", entryTime)
    mi.outData.put("LMDT", changeDate)
    mi.outData.put("CHNO", changeNumber)
    mi.outData.put("CHID", changedBy)
    mi.write()
  }


}
