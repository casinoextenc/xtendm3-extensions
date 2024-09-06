/**
 * README
 * This extension is used by Mashup
 * QUAX01 Gestion du référentiel qualité
 * Name : EXT031MI.CpyConstrType
 * Description : Copy records to the EXT031 table.
 * Date         Changed By   Description
 * 20230125     SEAR         QUAX01 - Constraints matrix
 * 20240605     FLEBARS      QUAX01 - Controle code pour validation Infor
 * 20240716     FLEBARS      QUAX01 - Controle code pour validation Infor Retours
 */
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

public class CpyConstrType extends ExtendM3Transaction {
  private final MIAPI mi
  private final LoggerAPI logger
  private final ProgramAPI program
  private final DatabaseAPI database

  private int currentCompany

  public CpyConstrType(MIAPI mi, DatabaseAPI database, ProgramAPI program, LoggerAPI logger) {
    this.mi = mi
    this.database = database
    this.program = program
    this.logger = logger
  }

  public void main() {
    if (mi.in.get("CONO") == null) {
      currentCompany = (Integer) program.getLDAZD().CONO
    } else {
      currentCompany = mi.in.get("CONO") as int
      String currentUser = program.getUser()
      if (!checkCompany(currentCompany, currentUser)) {
        mi.error("Company ${currentCompany} does not exist for the user ${currentUser}")
        return
      }
    }

    LocalDateTime timeOfCreation = LocalDateTime.now()
    DBAction ext031Query = database.table("EXT031").index("00").selection("EXZTYP").build()
    DBContainer ext031Request = ext031Query.getContainer()
    ext031Request.set("EXCONO", currentCompany)
    ext031Request.set("EXZCTY", mi.in.get("ZCTY"))

    if (ext031Query.read(ext031Request)) {
      ext031Request.set("EXZCTY", mi.in.get("CZCT"))
      if (!ext031Query.read(ext031Request)) {
        ext031Request.setInt("EXRGDT", timeOfCreation.format(DateTimeFormatter.ofPattern("yyyyMMdd")) as Integer)
        ext031Request.setInt("EXRGTM", timeOfCreation.format(DateTimeFormatter.ofPattern("HHmmss")) as Integer)
        ext031Request.setInt("EXLMDT", timeOfCreation.format(DateTimeFormatter.ofPattern("yyyyMMdd")) as Integer)
        ext031Request.setInt("EXCHNO", 1)
        ext031Request.set("EXCHID", program.getUser())
        ext031Query.insert(ext031Request)
      } else {
        mi.error("L'enregistrement existe déjà")
      }
    } else {
      mi.error("L'enregistrement n'existe pas")
    }
  }

  /**
   *  Check if CONO is alowed for user
   * @param cono
   * @param user
   * @return true if alowed false otherwise
   */
  private boolean checkCompany(int cono, String user) {
    DBAction csyusrQuery = database.table("CSYUSR").index("00").build()
    DBContainer csyusrRequest = csyusrQuery.getContainer()
    csyusrRequest.set("CRCONO", cono)
    csyusrRequest.set("CRDIVI", '')
    csyusrRequest.set("CRRESP", user)
    if (!csyusrQuery.read(csyusrRequest)) {
      return false
    }
    return true
  }

}
