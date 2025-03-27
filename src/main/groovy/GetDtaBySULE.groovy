/**
 * Name : EXT010MI.GetDtaBySULE
 * COMX01 Gestion des assortiments clients
 * Description :
 * This API method to add records in specific table EXT010 Customer Assortment
 * Date         Changed By    Description
 * 20231016     FLEBARS       COMX01 - Creation
 * 20240620     FLEBARS       COMX01 - Controle code pour validation Infor
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
    DBAction ext010Query = database.table("EXT010")
      .index("03")
      .selection("EXRSCL")
      .build()

    DBContainer ext010Request = ext010Query.getContainer()
    ext010Request.set("EXCONO", currentCompany)
    ext010Request.set("EXITNO", itno)
    ext010Request.set("EXSULE", sule)

    Closure<?> ext010Reader = { DBContainer ext010Result ->
      mi.outData.put("RSCL", ext010Result.get("EXRSCL") as String)
      mi.write()
    }

    //if record exists
    if (!ext010Query.readAll(ext010Request, 3, 1, ext010Reader)) {
      mi.error("L'enregistrement n'existe pas")
    }
  }
}
