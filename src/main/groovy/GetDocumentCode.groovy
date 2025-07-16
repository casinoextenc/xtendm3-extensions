/**
 * README
 * This extension is used by Mashup
 * QUAX01 Gestion du référentiel qualité
 * Name : EXT035MI.GetDocumentCode
 * Description : Retrieve records from the EXT035 table.
 * Date         Changed By   Description
 * 20210125     SEAR         QUAX01 - Constraints matrix
 * 20240605     FLEBARS      QUAX01 - Controle code pour validation Infor
 */
public class GetDocumentCode extends ExtendM3Transaction {
  private final MIAPI mi
  private final DatabaseAPI database
  private final LoggerAPI logger
  private final ProgramAPI program

  private int currentCompany

  public GetDocumentCode(MIAPI mi, DatabaseAPI database, LoggerAPI logger, ProgramAPI program) {
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
    DBAction ext035Query = database.table("EXT035").index("00").selection("EXZCOD", "EXCSCD", "EXCUNO", "EXDOID", "EXADS1", "EXRGDT", "EXRGTM", "EXLMDT", "EXCHNO", "EXCHID").build()
    DBContainer ext035Request = ext035Query.getContainer()
    ext035Request.set("EXCONO", currentCompany)
    ext035Request.set("EXZCOD", mi.in.get("ZCOD"))
    ext035Request.set("EXCUNO", mi.in.get("CUNO"))
    ext035Request.set("EXCSCD", mi.in.get("CSCD"))
    ext035Request.set("EXDOID", mi.in.get("DOID"))
    if (ext035Query.read(ext035Request)) {
      String constraintCode = ext035Request.get("EXZCOD")
      String countryCode = ext035Request.get("EXCSCD")
      String customerCode = ext035Request.get("EXCUNO")
      String documentCode = ext035Request.get("EXDOID")
      String documentCodeType = ext035Request.get("EXADS1")
      String entryDate = ext035Request.get("EXRGDT")
      String entryTime = ext035Request.get("EXRGTM")
      String changeDate = ext035Request.get("EXLMDT")
      String changeNumber = ext035Request.get("EXCHNO")
      String changedBy = ext035Request.get("EXCHID")
      mi.outData.put("ZCOD", constraintCode)
      mi.outData.put("CSCD", countryCode)
      mi.outData.put("CUNO", customerCode)
      mi.outData.put("DOID", documentCode)
      mi.outData.put("ADS1", documentCodeType)
      mi.outData.put("RGDT", entryDate)
      mi.outData.put("RGTM", entryTime)
      mi.outData.put("LMDT", changeDate)
      mi.outData.put("CHNO", changeNumber)
      mi.outData.put("CHID", changedBy)
      mi.write()
    } else {
      mi.error("L'enregistrement n'existe pas")
      return
    }
  }
}

