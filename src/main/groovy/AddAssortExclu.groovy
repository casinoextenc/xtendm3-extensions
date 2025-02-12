/**
 * This extension is used by Mashup
 * Name : EXT025MI.AddAssortExclu
 * COMX01 Gestion des assortiments clients
 * Description : The AddAssortExclu transaction adds records to the EXT025 table.
 * Date         Changed By   Description
 * 20240206     YVOYOU        COMX01 - Add assortment item exclusion
 * 20240620     FLEBARS       COMX01 - Controle code pour validation Infor
 * 20240709     FLEBARS       COMX01 - Controle code pour validation Infor retours
 */
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

public class AddAssortExclu extends ExtendM3Transaction {
  private final MIAPI mi
  private final DatabaseAPI database
  private final LoggerAPI logger
  private final ProgramAPI program
  private final UtilityAPI utility

  private int currentCompany

  public AddAssortExclu(MIAPI mi, DatabaseAPI database, LoggerAPI logger, ProgramAPI program, UtilityAPI utility) {
    this.mi = mi
    this.database = database
    this.logger = logger
    this.program = program
    this.utility = utility
  }

  public void main() {
    String cuno = ""
    String itno = ""
    String fdat = ""

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

    if (mi.in.get("CUNO") != null) {
      DBAction ocusmaQuery = database.table("OCUSMA").index("00").build()
      DBContainer ocusmaRequest = ocusmaQuery.getContainer()
      ocusmaRequest.set("OKCONO", currentCompany)
      ocusmaRequest.set("OKCUNO", mi.in.get("CUNO"))
      if (!ocusmaQuery.read(ocusmaRequest)) {
        mi.error("Code Client " + mi.in.get("CUNO") + " n'existe pas")
        return
      }
      cuno = mi.in.get("CUNO")
    } else {
      mi.error("Code Client est obligatoire")
      return
    }
    if (mi.in.get("ITNO") != null) {
      itno = mi.in.get("ITNO")
      DBAction mitmasQuery = database.table("MITMAS").index("00").selection("MMCONO", "MMITNO", "MMSTAT", "MMFUDS").build()
      DBContainer mitmasRequest = mitmasQuery.getContainer()
      mitmasRequest.set("MMCONO", currentCompany)
      mitmasRequest.set("MMITNO", itno)
      if (mitmasQuery.read(mitmasRequest)) {
        String stat = mitmasRequest.get("MMSTAT") as String
        if (!stat.equals("20")) {
          mi.error("Statut Article ${itno} est invalide")
          return
        }
      } else {
        mi.error("Article ${itno} n'existe pas")
        return
      }
    } else {
      mi.error("Code Article est obligatoire")
      return
    }
    if (mi.in.get("FDAT") == null) {
      mi.error("Date de Validité est obligatoire")
      return
    } else {
      fdat = mi.in.get("FDAT")
      if (!utility.call("DateUtil", "isDateValid", fdat, "yyyyMMdd")) {
        mi.error("Format Date de Validité incorrect " + fdat)
        return
      }
    }

    LocalDateTime timeOfCreation = LocalDateTime.now()
    DBAction ext025Query = database.table("EXT025").index("00").build()
    DBContainer ext025Request = ext025Query.getContainer()
    ext025Request.set("EXCONO", currentCompany)
    ext025Request.set("EXCUNO", cuno)
    ext025Request.set("EXITNO", itno)
    ext025Request.setInt("EXFDAT", fdat as Integer)
    if (!ext025Query.read(ext025Request)) {
      ext025Request.setInt("EXRGDT", timeOfCreation.format(DateTimeFormatter.ofPattern("yyyyMMdd")) as Integer)
      ext025Request.setInt("EXRGTM", timeOfCreation.format(DateTimeFormatter.ofPattern("HHmmss")) as Integer)
      ext025Request.setInt("EXLMDT", timeOfCreation.format(DateTimeFormatter.ofPattern("yyyyMMdd")) as Integer)
      ext025Request.setInt("EXCHNO", 1)
      ext025Request.set("EXCHID", program.getUser())
      ext025Query.insert(ext025Request)
    } else {
      mi.error("L'enregistrement existe déjà")
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
