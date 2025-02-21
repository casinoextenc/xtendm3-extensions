/**
 * README
 * This extension is used by EventHub
 *
 * Name : EXT014MI.LstCustomerMas
 * Description : List Customer Massification
 * Date         Changed By   Description
 * 20240903     PBEAUDOUIN   LOG14 - Shipment
 */
public class LstCustomerMas extends ExtendM3Transaction {
  private final MIAPI mi
  private final DatabaseAPI database
  private final LoggerAPI logger
  private final ProgramAPI program
  private final UtilityAPI utility

  private int currentCompany
  private Integer nbMaxRecord = 10000

  public LstCustomerMas(MIAPI mi, DatabaseAPI database, LoggerAPI logger, ProgramAPI program, UtilityAPI utility) {
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

    if (mi.in.get("CONO") != null) {
      currentCompany = (Integer) mi.in.get("CONO")
    }

    if (mi.in.get("CUNO") != null) {
      cuno = mi.in.get("CUNO")
    }
    if (mi.in.get("WHLO") != null) {
      whlo = mi.in.get("WHLO")
    }

    if (mi.in.get("FVDT") != null) {

      fvdt = mi.in.get("FVDT")
      boolean checkDate = (Boolean) utility.call("DateUtil", "isDateValid", "" + fvdt, "yyyyMMdd")
      if (!checkDate){
        mi.error("Format Date de Début incorrect")
        return
      }
    }

    if (mi.in.get("LVDT") != null) {
      lvdt = mi.in.get("LVDT")
      boolean checkDate = (Boolean) utility.call("DateUtil", "isDateValid", "" + lvdt, "yyyyMMdd")
      if (!checkDate) {
        mi.error("Format Date de Fin incorrect")
        return
      } else if (lvdt<= fvdt) {
        mi.error("La Date de Fin doit être supérieur à la Date de Début")
        return
      }
    }

    //Create Expression
    ExpressionFactory ext014Expression = database.getExpressionFactory("EXT014")
    ext014Expression = ext014Expression.eq("EXCONO", currentCompany.toString())

    if (cuno != "") {
      ext014Expression = ext014Expression.and(ext014Expression.eq("EXCUNO", cuno))
    }
    if (whlo != "") {
      ext014Expression = ext014Expression.and(ext014Expression.eq("EXWHLO", whlo))
    }
    if (fvdt > 0) {
      ext014Expression = ext014Expression.and(ext014Expression.ge("EXFVDT", String.valueOf(fvdt)))
    }
    if (lvdt >0 ) {
      ext014Expression = ext014Expression.and(ext014Expression.le("EXLVDT", String.valueOf(lvdt)))
    }

    //Run Select
    DBAction ext014Query = database.table("EXT014").index("00").matching(ext014Expression).selection("EXCONO", "EXCUNO", "EXWHLO", "EXFVDT", "EXLVDT").build()
    DBContainer ext014Request = ext014Query.getContainer()
    ext014Request.setInt("EXCONO", currentCompany)
    if (!ext014Query.readAll(ext014Request, 1, nbMaxRecord, ext014Reader)) {
      mi.error("L'enregistrement n'existe pas")
      return
    }
  }
  // Retrieve EXT014
  Closure<?> ext014Reader = { DBContainer ext014Result ->
    String cono = ext014Result.get("EXCONO")
    String cuno = ext014Result.get("EXCUNO")
    String whlo = ext014Result.get("EXWHLO")
    String fvdt = ext014Result.get("EXFVDT")
    String lvdt = ext014Result.get("EXLVDT")
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
      mi.error("L'enregistrement n'existe pas")
      return
    } else {
      whnm = mitwhlRequest.get("MWWHNM")
    }

    mi.outData.put("CONO", cono)
    mi.outData.put("CUNO", cuno)
    mi.outData.put("CUNM", cunm)
    mi.outData.put("WHLO", whlo)
    mi.outData.put("WHNM", whnm)
    mi.outData.put("FVDT", fvdt)
    mi.outData.put("LVDT", lvdt)

    mi.write()
  }
}
