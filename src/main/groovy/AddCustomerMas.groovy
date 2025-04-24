/**
 * README
 * This extension is used by EventHub
 *
 * Name : EXT014MI.AddCustomerMas
 * Description : Add Customer Massification
 * Date         Changed By   Description
 * 20240903     PBEAUDOUIN   LOG14 - Shipment
 */
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

public class AddCustomerMas extends ExtendM3Transaction {
  private final MIAPI mi
  private final DatabaseAPI database
  private final LoggerAPI logger
  private final ProgramAPI program
  private final UtilityAPI utility
  private Integer currentCompany

  public AddCustomerMas(MIAPI mi, DatabaseAPI database, LoggerAPI logger, ProgramAPI program, UtilityAPI utility) {
    this.mi = mi
    this.database = database
    this.logger = logger
    this.program = program
    this.utility = utility
  }

  public void main() {
    String cuno = ""
    String whlo = ""
    int fvdt = 0
    int lvdt = 0
    Integer chno = 1

    currentCompany = (Integer) program.getLDAZD().CONO

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

    if (mi.in.get("WHLO") != null) {
      DBAction mitwhlQuery = database.table("MITWHL").index("00").build()
      DBContainer mitwhlRequest = mitwhlQuery.getContainer()
      mitwhlRequest.set("MWCONO", currentCompany)
      mitwhlRequest.set("MWWHLO", mi.in.get("WHLO"))
      if (!mitwhlQuery.read(mitwhlRequest)) {
        mi.error("Code Dépôt " + mi.in.get("WHLO") + " n'existe pas")
        return
      }
      whlo = mi.in.get("WHLO")
    } else {
      mi.error("Code Dépôt est obligatoire")
      return
    }


    if (mi.in.get("FVDT") == null) {
      mi.error("Date de Début est obligatoire")
      return
    } else {
      fvdt = mi.in.get("FVDT") as int
      boolean checkDate = (Boolean) utility.call("DateUtil", "isDateValid", "" + fvdt, "yyyyMMdd")
      logger.debug("#PB FVDT Correct is " + checkDate)
      if (!checkDate){
        mi.error("Format Date de Début incorrect")
        return
      }

    }

    if (mi.in.get("LVDT") != null) {
      lvdt = mi.in.get("LVDT") as int
      boolean checkDate = (Boolean) utility.call("DateUtil", "isDateValid", "" + lvdt, "yyyyMMdd")
      if (!checkDate) {
        mi.error("Format Date de Fin incorrect")
        return
      } else if (lvdt<= fvdt) {
        mi.error("La Date de Fin doit être supérieur à la Date de Début")
        return
      }

    } else {

      lvdt = 0
    }

    LocalDateTime timeOfCreation = LocalDateTime.now()
    DBAction ext014Query = database.table("EXT014").index("00").build()
    DBContainer  ext014Request =  ext014Query.getContainer()
    ext014Request.set("EXCONO", currentCompany)
    ext014Request.set("EXCUNO", cuno)
    ext014Request.set("EXWHLO", whlo)
    if (! ext014Query.read( ext014Request)) {
      ext014Request.setInt("EXFVDT", fvdt as Integer)
      ext014Request.setInt("EXLVDT", lvdt as Integer)
      ext014Request.setInt("EXRGDT", timeOfCreation.format(DateTimeFormatter.ofPattern("yyyyMMdd")) as Integer)
      ext014Request.setInt("EXRGTM", timeOfCreation.format(DateTimeFormatter.ofPattern("HHmmss")) as Integer)
      ext014Request.setInt("EXLMDT", timeOfCreation.format(DateTimeFormatter.ofPattern("yyyyMMdd")) as Integer)
      ext014Request.setInt("EXCHNO", chno)
      ext014Request.set("EXCHID", program.getUser())
      ext014Query.insert(ext014Request)
    } else {
      mi.error("L'enregistrement existe déjà")
    }
  }
}
