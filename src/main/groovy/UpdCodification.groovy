/**
 * README
 * This extension is used by Mashup
 * QUAX01 Gestion du référentiel qualité
 * Name : EXT034MI.UpdCodification
 * Description : Update records from the EXT034 table.
 * Date         Changed By   Description
 * 20210125     SEAR         QUAX01 - Constraints matrix
 * 20230620     FLEBARS      QUAX01 - evol contrainte
 * 20240605     FLEBARS      QUAX01 - Controle code pour validation Infor
 * 20240708     FLEBARS      QUAX01 - Controle code pour validation Infor retour controle CONO
 */
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

public class UpdCodification extends ExtendM3Transaction {
  private final MIAPI mi
  private final LoggerAPI logger
  private final ProgramAPI program
  private final DatabaseAPI database

  private int currentCompany

  public UpdCodification(MIAPI mi, DatabaseAPI database, ProgramAPI program, LoggerAPI logger) {
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

    //Check if record exists in Constraint Type Table (EXT031)
    if (mi.in.get("ZCTY") != null) {
      DBAction ext031Query = database.table("EXT031").index("00").build()
      DBContainer ext031Request = ext031Query.getContainer()
      ext031Request.set("EXCONO", currentCompany)
      ext031Request.set("EXZCTY", mi.in.get("ZCTY"))
      if (!ext031Query.read(ext031Request)) {
        mi.error("Type de contrainte " + mi.in.get("ZCTY") + " n'existe pas")
        return
      }
    }

    DBAction ext034Query = database.table("EXT034").index("00").selection("EXCONO", "EXZCOD").build()
    DBContainer ext034Request = ext034Query.getContainer()
    ext034Request.set("EXCONO", currentCompany)
    ext034Request.set("EXZCOD", mi.in.get("ZCOD"))
    if (!ext034Query.readLock(ext034Request, ext034Updater)) {
      mi.error("L'enregistrement n'existe pas")
      return
    }
  }
  Closure<?> ext034Updater = { LockedResult ext034LockedResult ->
    LocalDateTime timeOfCreation = LocalDateTime.now()
    int changeNumber = ext034LockedResult.get("EXCHNO")
    if (mi.in.get("ZDES") != null)
      ext034LockedResult.set("EXZDES", mi.in.get("ZDES"))
    if (mi.in.get("ZCTY") != null)
      ext034LockedResult.set("EXZCTY", mi.in.get("ZCTY"))
    if (mi.in.get("ZSTY") != null)
      ext034LockedResult.set("EXZSTY", mi.in.get("ZSTY"))
    ext034LockedResult.setInt("EXLMDT", timeOfCreation.format(DateTimeFormatter.ofPattern("yyyyMMdd")) as Integer)
    ext034LockedResult.setInt("EXCHNO", changeNumber + 1)
    ext034LockedResult.set("EXCHID", program.getUser())
    ext034LockedResult.update()
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
