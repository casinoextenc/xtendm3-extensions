/**
 * README
 * This extension is used by Mashup
 * QUAX01 Gestion du référentiel qualité
 * Name : EXT031MI.GetConstrType
 * Description : Retrieve records from the EXT031 table.
 * Date         Changed By   Description
 * 20210125     SEAR         QUAX01 - Constraints matrix
 * 20240605     FLEBARS      QUAX01 - Controle code pour validation Infor
 */


public class GetConstrType extends ExtendM3Transaction {
  private final MIAPI mi
  private final DatabaseAPI database
  private final LoggerAPI logger
  private final ProgramAPI program

  private int currentCompany

  public GetConstrType(MIAPI mi, DatabaseAPI database, LoggerAPI logger, ProgramAPI program) {
    this.mi = mi
    this.database = database
    this.logger = logger
    this.program = program
  }

  public void main() {
    if (mi.in.get("CONO") == null) {
      currentCompany = (Integer) program.getLDAZD().CONO
    } else {
      currentCompany = mi.in.get("CONO")
    }

    DBAction ext031Query = database.table("EXT031").index("00").selection("EXZCTY", "EXZTYP", "EXRGDT", "EXRGTM", "EXLMDT", "EXCHNO", "EXCHID").build()
    DBContainer ext031Request = ext031Query.getContainer()
    ext031Request.set("EXCONO", currentCompany)
    ext031Request.set("EXZCTY", mi.in.get("ZCTY"))
    if (ext031Query.read(ext031Request)) {
      mi.outData.put("ZCTY", ext031Request.get("EXZCTY") as String)
      mi.outData.put("ZTYP", ext031Request.get("EXZTYP") as String)
      mi.outData.put("RGDT", ext031Request.get("EXRGDT") as String)
      mi.outData.put("RGTM", ext031Request.get("EXRGTM") as String)
      mi.outData.put("LMDT", ext031Request.get("EXLMDT") as String)
      mi.outData.put("CHNO", ext031Request.get("EXCHNO") as String)
      mi.outData.put("CHID", ext031Request.get("EXCHID") as String)
      mi.write()
    } else {
      mi.error("L'enregistrement n'existe pas")
      return
    }
  }

}
