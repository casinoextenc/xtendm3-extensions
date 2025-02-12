/**
 * Name : EXT012MI.AddOrUpdGDA
 * Description :
 * APP02 Planning GDA This API method to add or update records in specific table
 * Date         Changed By    Description
 * 20221122     FLEBARS       Creation
 * 20230801     FLEBARS       Suppression du controle de statut client
 * 20240911     FLEBARS       Revue code pour validation
 */

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

public class AddOrUpdGDA extends ExtendM3Transaction {
  private final MIAPI mi
  private final DatabaseAPI database
  private final LoggerAPI logger
  private final ProgramAPI program
  private final UtilityAPI utility

  private int currentCompany
  private String errorMessage

  public AddOrUpdGDA(MIAPI mi, DatabaseAPI database, LoggerAPI logger, ProgramAPI program, UtilityAPI utility) {
    this.mi = mi
    this.database = database
    this.logger = logger
    this.program = program
    this.utility=utility
  }

  /**
   * Get mi inputs
   * Check input values
   * Check if record not exists in
   * Serialize in
   */
  public void main() {
    currentCompany = (int)program.getLDAZD().CONO

    //Get mi inputs
    String asgd = (String)(mi.in.get("ASGD") != null ? mi.in.get("ASGD") : "")
    String cuno = (String)(mi.in.get("CUNO") != null ? mi.in.get("CUNO") : "")
    String suno = (String)(mi.in.get("SUNO") != null ? mi.in.get("SUNO") : "")
    int drgd = (Integer)(mi.in.get("DRGD") != null ? mi.in.get("DRGD") : 0)
    int dlgd = (Integer)(mi.in.get("DLGD") != null ? mi.in.get("DLGD") : 0)
    int hrgd = (Integer)(mi.in.get("HRGD") != null ? mi.in.get("HRGD") : 0)


    //Check inputs
    if (!checkCustomer(cuno)){
      mi.error(errorMessage)
      return
    }

    if (suno.length() > 0) {
      if (!checkSupplier(suno)){
        mi.error(errorMessage)
        return
      }
    }

    if (drgd != 0){
      boolean checkDate = (Boolean)utility.call("DateUtil", "isDateValid", "" + drgd, "yyyyMMdd")
      if (!checkDate){
        mi.error("Date de ramassage ${drgd} est invalide")
        return
      }
    }

    if (hrgd != 0){
      String formattedHrgd = String.format("%06d", hrgd)
      boolean checkTime = (Boolean)utility.call("DateUtil", "isDateValid", "20000101" + formattedHrgd, "yyyyMMddHHmmss")
      if (!checkTime){
        mi.error("Heure de ramassage ${hrgd} est invalide")
        return
      }
    }
    if (dlgd != 0){
      boolean checkDate = (Boolean)utility.call("DateUtil", "isDateValid", "" + dlgd, "yyyyMMdd")
      if (!checkDate){
        mi.error("Date de livraison ${dlgd} est invalide")
        return
      }
    }

    //Check if record exists
    DBAction ext012Query = database.table("EXT012")
      .index("00")
      .selection(
        "EXCONO",
        "EXASGD",
        "EXCUNO",
        "EXSUNO",
        "EXDRGD",
        "EXHRGD",
        "EXDLGD",
        "EXRGDT",
        "EXRGTM",
        "EXLMDT",
        "EXCHNO",
        "EXCHID"
      )
      .build()

    DBContainer ext012Request = ext012Query.getContainer()
    ext012Request.set("EXCONO", currentCompany)
    ext012Request.set("EXCUNO", cuno)
    ext012Request.set("EXSUNO", suno)
    ext012Request.set("EXASGD", asgd)
    ext012Request.set("EXDRGD", drgd)
    ext012Request.set("EXHRGD", hrgd)
    ext012Request.set("EXDLGD", dlgd)

    //Record exists
    if (ext012Query.read(ext012Request)) {
      Closure<?> ext012Updater = { LockedResult ext012LockedResult ->
        ext012LockedResult.set("EXASGD", asgd)
        ext012LockedResult.set("EXCUNO", cuno)
        ext012LockedResult.set("EXSUNO", suno)
        ext012LockedResult.set("EXDRGD", drgd)
        ext012LockedResult.set("EXHRGD", hrgd)
        ext012LockedResult.set("EXDLGD", dlgd)
        ext012LockedResult.set("EXLMDT", utility.call("DateUtil", "currentDateY8AsInt"))
        ext012LockedResult.setInt("EXCHNO", ((Integer)ext012LockedResult.get("EXCHNO") + 1))
        ext012LockedResult.set("EXCHID", program.getUser())
        ext012LockedResult.update()
      }
      ext012Query.readLock(ext012Request, ext012Updater)
    } else {
      ext012Request.set("EXCONO", currentCompany)
      ext012Request.set("EXASGD", asgd)
      ext012Request.set("EXCUNO", cuno)
      ext012Request.set("EXSUNO", suno)
      ext012Request.set("EXDRGD", drgd)
      ext012Request.set("EXHRGD", hrgd)
      ext012Request.set("EXDLGD", dlgd)
      ext012Request.set("EXRGDT", utility.call("DateUtil", "currentDateY8AsInt"))
      ext012Request.set("EXRGTM", utility.call("DateUtil", "currentTimeAsInt"))
      ext012Request.set("EXLMDT", utility.call("DateUtil", "currentDateY8AsInt"))
      ext012Request.set("EXCHNO", 1)
      ext012Request.set("EXCHID", program.getUser())
      ext012Query.insert(ext012Request)
    }
  }

  /**
   * Read customer information from DB OCUSMA
   * Customer must exists
   * Stat must be 20
   * Cutp must be 0
   *
   * @parameter Customer
   * @return true if ok false otherwise
   * */
  private boolean checkCustomer(String cuno){
    DBAction ocusmaQuery = database.table("OCUSMA").index("00").selection(
      "OKCONO", "OKCUNO", "OKSTAT", "OKCUTP").build()

    DBContainer ocusmaRequest = ocusmaQuery.getContainer()
    ocusmaRequest.set("OKCONO", currentCompany)
    ocusmaRequest.set("OKCUNO", cuno)
    if (ocusmaQuery.read(ocusmaRequest)) {
      String stat = (String)ocusmaRequest.get("OKSTAT")
      int cutp = (Integer)ocusmaRequest.get("OKCUTP")
      /*if (!stat.equals("20")){
        errorMessage = "Statut Client ${stat} est invalide"
        return false
      }*/
      if (cutp != 0){
        errorMessage = "Type Client ${cutp} est invalide"
        return false
      }
    } else {
      errorMessage = "Client ${cuno} n'existe pas"
      return false
    }
    return true
  }

  /**
   * Read Supplier information from DB CIDMAS
   * Supplier must exists
   * Status must be 20
   * Supplier group must equal input sucl
   *
   * @parameter Supplier
   * @parameter Supplier Group
   * @return true if ok false otherwise
   * */
  private boolean checkSupplier(String suno){
    DBAction cidmasQuery = database.table("CIDMAS").index("00").selection(
      "IDCONO", "IDSUNO", "IDSTAT").build()
    DBContainer cidmasRequest = cidmasQuery.getContainer()

    DBAction cidvenQuery = database.table("CIDVEN").index("00").selection(
      "IICONO", "IISUNO", "IISUCL").build()
    DBContainer cidvenRequest = cidvenQuery.getContainer()

    cidmasRequest.set("IDCONO", currentCompany)
    cidmasRequest.set("IDSUNO", suno)
    if (cidmasQuery.read(cidmasRequest)) {
      String stat = (String)cidmasRequest.get("IDSTAT")
      if (!stat.equals("20")){
        errorMessage = "Statut fournisseur ${stat} est invalide"
        return false
      }
    } else {
      errorMessage = "Fournisseur ${suno} n'existe pas"
      return false
    }
    return true
  }
}
