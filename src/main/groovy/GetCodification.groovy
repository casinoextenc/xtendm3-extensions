/**
 * README
 * This extension is used by Mashup
 * QUAX01 Gestion du référentiel qualité
 * Name : EXT034MI.GetCodification
 * Description : Retrieve records from the EXT034 table.
 * Date         Changed By   Description
 * 20210125     SEAR         QUAX01 - Constraints matrix
 * 20230620     FLEBARS      QUAX01 - evol contrainte
 * 20240605     FLEBARS      QUAX01 - Controle code pour validation Infor
 */
public class GetCodification extends ExtendM3Transaction {
  private final MIAPI mi
  private final DatabaseAPI database
  private final LoggerAPI logger
  private final ProgramAPI program

  private int currentCompany

  public GetCodification(MIAPI mi, DatabaseAPI database, LoggerAPI logger, ProgramAPI program) {
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
    DBAction ext034Query = database.table("EXT034").index("00").selection(
      "EXZCOD"
      , "EXZDES"
      , "EXZCTY"
      , "EXRGDT"
      , "EXRGTM"
      , "EXLMDT"
      , "EXCHNO"
      , "EXZSTY"
      , "EXCHID").build()
    DBContainer ext034Request = ext034Query.getContainer()
    ext034Request.set("EXCONO", currentCompany)
    ext034Request.set("EXZCOD", mi.in.get("ZCOD"))
    if (ext034Query.read(ext034Request)) {
      String codification = ext034Request.get("EXZCOD")
      String description = ext034Request.get("EXZDES")
      String constraintType = ext034Request.get("EXZCTY")
      String entryDate = ext034Request.get("EXRGDT")
      String entryTime = ext034Request.get("EXRGTM")
      String changeDate = ext034Request.get("EXLMDT")
      String changeNumber = ext034Request.get("EXCHNO")
      String changedBy = ext034Request.get("EXCHID")
      String zsty = ext034Request.get("EXZSTY")
      mi.outData.put("ZCOD", codification)
      mi.outData.put("ZCTY", constraintType)
      mi.outData.put("ZDES", description)
      mi.outData.put("RGDT", entryDate)
      mi.outData.put("RGTM", entryTime)
      mi.outData.put("LMDT", changeDate)
      mi.outData.put("CHNO", changeNumber)
      mi.outData.put("CHID", changedBy)
      mi.outData.put("ZSTY", zsty)
      mi.write()
    } else {
      mi.error("L'enregistrement n'existe pas")
    }
  }
}

