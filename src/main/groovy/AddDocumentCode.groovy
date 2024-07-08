/**
 * README
 * This extension is used by Mashup
 * QUAX01 Gestion du référentiel qualité
 * Name : EXT035MI.AddDocumentCode
 * Description : Add records to the EXT035 table.
 * Date         Changed By   Description
 * 20230201     SEAR         QUAX01 - Constraints matrix
 * 20240605     FLEBARS      QUAX01 - Controle code pour validation Infor
 * 20240708     FLEBARS      QUAX01 - Controle code pour validation Infor retours
 */
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

public class AddDocumentCode extends ExtendM3Transaction {
  private final MIAPI mi
  private final LoggerAPI logger
  private final ProgramAPI program
  private final DatabaseAPI database

  private int currentCompany
  private String ads1 = ""

  public AddDocumentCode(MIAPI mi, DatabaseAPI database, ProgramAPI program, LoggerAPI logger) {
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

    //Check if record exists in Constraint Code Table (EXT034)
    if (mi.in.get("ZCOD") != null) {
      DBAction ext034Query = database.table("EXT034").index("00").build()
      DBContainer ext034Request = ext034Query.getContainer()
      ext034Request.set("EXCONO", currentCompany)
      ext034Request.set("EXZCOD", mi.in.get("ZCOD"))
      if (!ext034Query.read(ext034Request)) {
        mi.error("Code contrainte " + mi.in.get("ZCOD") + " n'existe pas")
        return
      }
    }

    //Check if record exists in country Code Table (CSYTAB)
    if (mi.in.get("CSCD") != null) {
      DBAction csytabQuery = database.table("CSYTAB").index("00").build()
      DBContainer csytabRequest = csytabQuery.getContainer()
      csytabRequest.set("CTCONO", currentCompany)
      csytabRequest.set("CTSTCO", "CSCD")
      csytabRequest.set("CTSTKY", mi.in.get("CSCD"))
      if (!csytabQuery.read(csytabRequest)) {
        mi.error("Code pays " + mi.in.get("CSCD") + " n'existe pas")
        return
      }
    }

    //Check if record Cutomer in Customer Table (OCUSMA)
    if (mi.in.get("CUNO") != null) {
      DBAction ocusmaQuery = database.table("OCUSMA").index("00").build()
      DBContainer ocusmaRequest = ocusmaQuery.getContainer()
      ocusmaRequest.set("OKCONO", currentCompany)
      ocusmaRequest.set("OKCUNO", mi.in.get("CUNO"))
      if (!ocusmaQuery.read(ocusmaRequest)) {
        mi.error("Code client " + mi.in.get("CUNO") + " n'existe pas")
        return
      }
    }

    //Check if record exists in Document Code Table (MPDDOC)
    if (mi.in.get("DOID") != null) {
      DBAction mpddocQuery = database.table("MPDDOC").index("00").selection("DOADS1").build()
      DBContainer mpddocRequest = mpddocQuery.getContainer()
      mpddocRequest.set("DOCONO", currentCompany)
      mpddocRequest.set("DODOID", mi.in.get("DOID"))
      if (!mpddocQuery.read(mpddocRequest)) {
        mi.error("Code Document " + mi.in.get("DOID") + " n'existe pas")
        return
      }
      ads1 = (String) mpddocRequest.get("DOADS1")
    }
    //document type from input
    if (mi.in.get("ADS1") != null) {
      ads1 = mi.in.get("ADS1")
    }

    LocalDateTime timeOfCreation = LocalDateTime.now()
    DBAction ext035Query = database.table("EXT035").index("00").build()
    DBContainer ext035Request = ext035Query.getContainer()
    ext035Request.set("EXCONO", currentCompany)
    ext035Request.set("EXZCOD", mi.in.get("ZCOD"))
    ext035Request.set("EXCUNO", mi.in.get("CUNO"))
    ext035Request.set("EXCSCD", mi.in.get("CSCD"))
    ext035Request.set("EXDOID", mi.in.get("DOID"))
    if (!ext035Query.read(ext035Request)) {
      ext035Request.set("EXADS1", ads1)
      ext035Request.setInt("EXRGDT", timeOfCreation.format(DateTimeFormatter.ofPattern("yyyyMMdd")) as Integer)
      ext035Request.setInt("EXRGTM", timeOfCreation.format(DateTimeFormatter.ofPattern("HHmmss")) as Integer)
      ext035Request.setInt("EXLMDT", timeOfCreation.format(DateTimeFormatter.ofPattern("yyyyMMdd")) as Integer)
      ext035Request.setInt("EXCHNO", 1)
      ext035Request.set("EXCHID", program.getUser())
      ext035Query.insert(ext035Request)
    } else {
      mi.error("L'enregistrement existe déjà")
      return
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
