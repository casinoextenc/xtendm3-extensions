/**
 * README
 * This extension is used by Mashup
 * QUAX01 Gestion du référentiel qualité
 * Name : EXT034MI.CpyCodification
 * Description : Copy records to the EXT034 table.
 * Date         Changed By   Description
 * 20230125     SEAR         QUAX01 - Constraints matrix 
 * 20240605     FLEBARS      QUAX01 - Controle code pour validation Infor
 * 20240708     FLEBARS      QUAX01 - Controle code pour validation Infor retour controle CONO
 */
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

public class CpyCodification extends ExtendM3Transaction {
  private final MIAPI mi
  private final LoggerAPI logger
  private final ProgramAPI program
  private final DatabaseAPI database

  private int currentCompany

  public CpyCodification(MIAPI mi, DatabaseAPI database, ProgramAPI program, LoggerAPI logger) {
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
    DBAction ext034Query = database.table("EXT034").index("00").selection("EXZDES", "EXZCTY").build()
    DBContainer ext034Request = ext034Query.getContainer()
    ext034Request.set("EXCONO", currentCompany)
    ext034Request.set("EXZCOD", mi.in.get("ZCOD"))
    if (ext034Query.read(ext034Request)) {
      ext034Request.set("EXZCOD", mi.in.get("CZCO"))
      if (!ext034Query.read(ext034Request)) {
        ext034Request.setInt("EXRGDT", timeOfCreation.format(DateTimeFormatter.ofPattern("yyyyMMdd")) as Integer)
        ext034Request.setInt("EXRGTM", timeOfCreation.format(DateTimeFormatter.ofPattern("HHmmss")) as Integer)
        ext034Request.setInt("EXLMDT", timeOfCreation.format(DateTimeFormatter.ofPattern("yyyyMMdd")) as Integer)
        ext034Request.setInt("EXCHNO", 1)
        ext034Request.set("EXCHID", program.getUser())
        ext034Query.insert(ext034Request)
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
