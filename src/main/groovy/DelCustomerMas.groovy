/**
 * README
 * This extension is used by EventHub
 *
 * Name : EXT014MI.DelCustomerMas
 * Description : Delete Customer Massification
 * Date         Changed By   Description
 * 20240903     PBEAUDOUIN   LOG14 - Shipment
 */
public class DelCustomerMas extends ExtendM3Transaction {
  private final MIAPI mi
  private final DatabaseAPI database
  private final LoggerAPI logger
  private final ProgramAPI program
  private final UtilityAPI utility

  private int currentCompany

  public DelCustomerMas(MIAPI mi, DatabaseAPI database, LoggerAPI logger, ProgramAPI program, UtilityAPI utility) {
    this.mi = mi
    this.database = database
    this.logger = logger
    this.program = program
    this.utility = utility
  }

  public void main() {
    currentCompany = (int) program.getLDAZD().CONO

    String cuno = ""
    String whlo = ""
    int fvdt = 0

    if (mi.in.get("CUNO") == null) {
      mi.error("Code Client est obligatoire")
      return
    } else {
      cuno = (String) mi.in.get("CUNO")
    }

    if (mi.in.get("WHLO") == null) {
      mi.error("Code Dépôt est obligatoire")
      return
    } else {
      whlo = (String) mi.in.get("WHLO")
    }

    if (mi.in.get("FVDT") == null) {
      mi.error("Date de Début est obligatoire")
      return
    } else {
      fvdt = mi.in.get("FVDT") as int
      boolean checkDate = (Boolean) utility.call("DateUtil", "isDateValid", "" + fvdt, "yyyyMMdd")
      if (!checkDate){
        mi.error("Format Date de Début incorrect")
        return
      }

    }

    //Check if record exists
    DBAction ext014Query = database.table("EXT014")
      .index("00")
      .selection(
        "EXCONO",
        "EXCUNO",
        "EXWHLO",
        "EXFVDT"
      )
      .build()

    DBContainer ext014Request = ext014Query.getContainer()
    ext014Request.set("EXCONO", currentCompany)
    ext014Request.set("EXCUNO", cuno)
    ext014Request.set("EXWHLO", whlo)
    ext014Request.set("EXFVDT", fvdt)

    //Record exists
    if (!ext014Query.read(ext014Request)) {
      mi.error("L'enregistrement n'existe pas")
      return
    }

    Closure<?> ext014Updater = { LockedResult ext014LockedResult ->
      ext014LockedResult.delete()
    }

    ext014Query.readLock(ext014Request, ext014Updater)

  }
}
