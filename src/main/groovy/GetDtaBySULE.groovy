/**
 * Name : EXT010MI.GetDtaBySULE
 *
 * Description :
 * This API method to add records in specific table EXT010 Customer Assortment
 *
 *
 * Date         Changed By    Description
 * 20231016     FLEBARS       COMX01 - Creation
 */
public class GetDtaBySULE extends ExtendM3Transaction {
  private final MIAPI mi
  private final DatabaseAPI database
  private final LoggerAPI logger
  private final ProgramAPI program
  private final UtilityAPI utility
  private int currentCompany

  public GetDtaBySULE(MIAPI mi, DatabaseAPI database, LoggerAPI logger, ProgramAPI program, UtilityAPI utility) {
    this.mi = mi
    this.database = database
    this.logger = logger
    this.program = program
    this.utility = utility
  }

  public void main() {
    currentCompany = (int) program.getLDAZD().CONO
    String itno = (String) (mi.in.get("ITNO") != null ? mi.in.get("ITNO") : "")
    String sule = (String) (mi.in.get("SULE") != null ? mi.in.get("SULE") : "")

    //Chain EXT010
    DBAction EXT010_query = database.table("EXT010")
      .index("03")
      .selection("EXRSCL")
      .build();

    DBContainer EXT010_request = EXT010_query.getContainer()
    EXT010_request.set("EXCONO", currentCompany)
    EXT010_request.set("EXITNO", itno)
    EXT010_request.set("EXSULE", sule)

    Closure<?> EXT010_reader = { DBContainer EXT010_result ->
      mi.outData.put("RSCL", EXT010_result.get("EXRSCL") as String)
        mi.write()
    }

    //if record exists
    if (!EXT010_query.readAll(EXT010_request, 3, 1, EXT010_reader)) {
      mi.error("L'enregistrement n'existe pas")
    }
  }
}
