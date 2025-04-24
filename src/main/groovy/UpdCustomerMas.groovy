/**
 * README
 * This extension is used by EventHub
 *
 * Name : EXT014MI.UpdCustomerMas
 * Description : Update Customer Massification
 * Date         Changed By   Description
 * 20240903     PBEAUDOUIN   LOG14 - Shipment
 */
public class UpdCustomerMas extends ExtendM3Transaction {
  private final MIAPI mi
  private final DatabaseAPI database
  private final LoggerAPI logger
  private final ProgramAPI program
  private final UtilityAPI utility

  private int currentCompany


  public UpdCustomerMas(MIAPI mi, DatabaseAPI database, LoggerAPI logger, ProgramAPI program, UtilityAPI utility) {
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
    int lvdt = 0
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

    //Check if record exists
    DBAction ext014Query = database.table("EXT014")
      .index("00")
      .selection(
        "EXCONO",
        "EXCUNO",
        "EXWHLO",
        "EXFVDT",
      )
      .build()

    DBContainer ext014Request = ext014Query.getContainer()
    ext014Request.set("EXCONO", currentCompany)
    ext014Request.set("EXCUNO", cuno)
    ext014Request.set("EXWHLO", whlo)
    ext014Request.set("EXFVDT", fvdt)
    if (mi.in.get("LVDT") != null)
      ext014Request.setInt("EXLVDT", lvdt as Integer)
    //Record exists
    if (!ext014Query.read(ext014Request)) {
      mi.error("L'enregistrement n'existe pas")
      return
    }

    Closure<?> ext014Updater = { LockedResult ext014LockedResult ->
      ext014LockedResult.set("EXLMDT", utility.call("DateUtil", "currentDateY8AsInt"))
      ext014LockedResult.set("EXCHNO", ((Integer) ext014LockedResult.get("EXCHNO") + 1))
      ext014LockedResult.set("EXCHID", program.getUser())
      ext014LockedResult.set("EXLVDT", lvdt)
      ext014LockedResult.update()
    }

    ext014Query.readLock(ext014Request, ext014Updater)
  }
}
