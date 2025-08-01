/**
 * README
 * This extension is used by EventHub
 *
 * Name : EXT014MI.GetCustomerMas
 * Description : Get Customer Massification
 * Date         Changed By   Description
 * 20240903     PBEAUDOUIN   LOG14 - Shipment
 */
public class GetCustomerMas extends ExtendM3Transaction {
  private final MIAPI mi
  private final DatabaseAPI database
  private final LoggerAPI logger
  private final ProgramAPI program
  private final UtilityAPI utility

  private int currentCompany

  public GetCustomerMas(MIAPI mi, DatabaseAPI database, LoggerAPI logger, ProgramAPI program, UtilityAPI utility) {
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

    if (mi.in.get("CONO") != null) {
      currentCompany = mi.in.get("CONO")
    } else {
      currentCompany = (Integer) program.getLDAZD().CONO
    }

    if (mi.in.get("CUNO") != null) {
      cuno = mi.in.get("CUNO")
    } else {
      mi.error("Code Client obligatoire")
      return
    }

    if (mi.in.get("WHLO") != null) {
      whlo = mi.in.get("WHLO")
    } else {
      mi.error("Dépôt t obligatoire")
      return
    }

    if (mi.in.get("FVDT") != null) {

      fvdt = mi.in.get("FVDT")
      boolean checkDate = (Boolean) utility.call("DateUtil", "isDateValid", "" + fvdt, "yyyyMMdd")
      if (!checkDate){
        mi.error("Format Date de Début incorrect")
        return
      }

    }  else {
      mi.error("Date de début obligatoire")
      return
    }

    //Run Select
    DBAction ext014Query = database.table("EXT014").index("00").selection("EXCONO", "EXCUNO", "EXWHLO", "EXFVDT", "EXLVDT").build()
    DBContainer ext014Request = ext014Query.getContainer()
    ext014Request.setInt("EXCONO", currentCompany)
    ext014Request.set("EXCUNO", cuno)
    ext014Request.set("EXWHLO", whlo)
    ext014Request.setInt("EXFVDT", fvdt)
    if (!ext014Query.read(ext014Request)) {
      mi.error("L'enregistrement n'existe pas")
    } else {
      String cono = ext014Request.get("EXCONO")
      cuno = ext014Request.get("EXCUNO")
      whlo = ext014Request.get("EXWHLO")
      fvdt = ext014Request.get("EXFVDT")
      String lvdt = ext014Request.get("EXLVDT")
      String cunm = ""
      String whnm = ""

      //Retrieve Customer Name
      DBAction ocusmaQuery = database.table("OCUSMA")
        .index("00")
        .selection(
          "OKCUNM"
        )
        .build()

      DBContainer ocusmaRequest = ocusmaQuery.getContainer()
      ocusmaRequest.set("OKCONO", currentCompany)
      ocusmaRequest.set("OKCUNO", cuno)

      //Record exists
      if (!ocusmaQuery.read(ocusmaRequest)) {
        mi.error("L'enregistrement n'existe pas")
        return
      } else {
        cunm = ocusmaRequest.get("OKCUNM")
      }

// Retrieve Warehouse description

      //Retrieve Customer Name
      DBAction mitwhlQuery = database.table("MITWHL")
        .index("00")
        .selection(
          "MWWHNM"
        )
        .build()

      DBContainer mitwhlRequest = mitwhlQuery.getContainer()
      mitwhlRequest.set("MWCONO", currentCompany)
      mitwhlRequest.set("MWWHLO", whlo)

      //Record exists
      if (!mitwhlQuery.read(mitwhlRequest)) {
        mi.error("L'enregistrement n'existe pas dans MITWH")
        return
      } else {
        whnm = mitwhlRequest.get("MWWHNM")
      }

      mi.outData.put("CONO", cono)
      mi.outData.put("CUNO", cuno)
      mi.outData.put("CUNM", cunm)
      mi.outData.put("WHLO", whlo)
      mi.outData.put("WHNM", whnm)
      mi.outData.put("FVDT", fvdt as String)
      mi.outData.put("LVDT", lvdt)

      mi.write()
    }
  }
}
