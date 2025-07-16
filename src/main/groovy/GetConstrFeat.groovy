/**
 * README
 * This extension is used by Mashup
 * QUAX01 Gestion du référentiel qualité
 * Name : EXT033MI.GetConstrFeat
 * Description : Retrieve records from the EXT033 table.
 * Date         Changed By   Description
 * 20210125     SEAR         QUAX01 - Constraints matrix
 * 20240605     FLEBARS      QUAX01 - Controle code pour validation Infor
 */
public class GetConstrFeat extends ExtendM3Transaction {
  private final MIAPI mi
  private final DatabaseAPI database
  private final LoggerAPI logger
  private final ProgramAPI program

  private int currentCompany

  public GetConstrFeat(MIAPI mi, DatabaseAPI database, LoggerAPI logger, ProgramAPI program) {
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
    DBAction ext033Query = database.table("EXT033").index("00").selection("EXZCAR", "EXZDES", "EXRGDT", "EXRGTM", "EXLMDT", "EXCHNO", "EXCHID").build()
    DBContainer ext033Request = ext033Query.getContainer()
    ext033Request.set("EXCONO", currentCompany)
    ext033Request.set("EXZCAR", mi.in.get("ZCAR"))
    if (ext033Query.read(ext033Request)) {
      String constraintFeat = ext033Request.get("EXZCAR")
      String description = ext033Request.get("EXZDES")
      String entryDate = ext033Request.get("EXRGDT")
      String entryTime = ext033Request.get("EXRGTM")
      String changeDate = ext033Request.get("EXLMDT")
      String changeNumber = ext033Request.get("EXCHNO")
      String changedBy = ext033Request.get("EXCHID")
      mi.outData.put("ZCAR", constraintFeat)
      mi.outData.put("ZDES", description)
      mi.outData.put("RGDT", entryDate)
      mi.outData.put("RGTM", entryTime)
      mi.outData.put("LMDT", changeDate)
      mi.outData.put("CHNO", changeNumber)
      mi.outData.put("CHID", changedBy)
      mi.write()

    } else {
      mi.error("L'enregistrement n'existe pas")
    }
  }
}

