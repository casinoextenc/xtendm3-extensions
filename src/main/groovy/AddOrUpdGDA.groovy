/**
 * Name : EXT012MI.AddOrUpdGDA
 * 
 * Description : 
 * This API method to add records in specific table  Planning GDA
 * 
 * 
 * Date         Changed By    Description
 * 20221122     FLEBARS       Creation
 * 20230801     FLEBARS       Suppression du controle de statut client
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
      String formatted_hrgd = String.format("%06d", hrgd)
      boolean checkTime = (Boolean)utility.call("DateUtil", "isDateValid", "20000101" + formatted_hrgd, "yyyyMMddHHmmss")
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
    DBAction queryEXT012 = database.table("EXT012")
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

    DBContainer containerEXT012 = queryEXT012.getContainer()
    containerEXT012.set("EXCONO", currentCompany)
    containerEXT012.set("EXCUNO", cuno)
    containerEXT012.set("EXSUNO", suno)
    containerEXT012.set("EXASGD", asgd)
    containerEXT012.set("EXDRGD", drgd)
    containerEXT012.set("EXHRGD", hrgd)
    containerEXT012.set("EXDLGD", dlgd)

    //Record exists
    if (queryEXT012.read(containerEXT012)) {
      Closure<?> updateEXT012 = { LockedResult lockedResultEXT012 ->
        lockedResultEXT012.set("EXASGD", asgd)
        lockedResultEXT012.set("EXCUNO", cuno)
        lockedResultEXT012.set("EXSUNO", suno)
        lockedResultEXT012.set("EXDRGD", drgd)
        lockedResultEXT012.set("EXHRGD", hrgd)
        lockedResultEXT012.set("EXDLGD", dlgd)
        lockedResultEXT012.set("EXLMDT", utility.call("DateUtil", "currentDateY8AsInt"))
        lockedResultEXT012.setInt("EXCHNO", ((Integer)lockedResultEXT012.get("EXCHNO") + 1))
        lockedResultEXT012.set("EXCHID", program.getUser())
        lockedResultEXT012.update()
      }
      queryEXT012.readLock(containerEXT012, updateEXT012)
    } else {
      containerEXT012.set("EXCONO", currentCompany)
      containerEXT012.set("EXASGD", asgd)
      containerEXT012.set("EXCUNO", cuno)
      containerEXT012.set("EXSUNO", suno)
      containerEXT012.set("EXDRGD", drgd)
      containerEXT012.set("EXHRGD", hrgd)
      containerEXT012.set("EXDLGD", dlgd)
      containerEXT012.set("EXRGDT", utility.call("DateUtil", "currentDateY8AsInt"))
      containerEXT012.set("EXRGTM", utility.call("DateUtil", "currentTimeAsInt"))
      containerEXT012.set("EXLMDT", utility.call("DateUtil", "currentDateY8AsInt"))
      containerEXT012.set("EXCHNO", 1)
      containerEXT012.set("EXCHID", program.getUser())
      queryEXT012.insert(containerEXT012)
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
    DBAction queryOCUSMA = database.table("OCUSMA").index("00").selection(
        "OKCONO", "OKCUNO", "OKSTAT", "OKCUTP").build()

    DBContainer containerOCUSMA = queryOCUSMA.getContainer()
    containerOCUSMA.set("OKCONO", currentCompany)
    containerOCUSMA.set("OKCUNO", cuno)
    if (queryOCUSMA.read(containerOCUSMA)) {
      String stat = (String)containerOCUSMA.get("OKSTAT")
      int cutp = (Integer)containerOCUSMA.get("OKCUTP")
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
    DBAction queryCIDMAS = database.table("CIDMAS").index("00").selection(
        "IDCONO", "IDSUNO", "IDSTAT").build()
    DBContainer containerCIDMAS = queryCIDMAS.getContainer()

    DBAction queryCIDVEN = database.table("CIDVEN").index("00").selection(
        "IICONO", "IISUNO", "IISUCL").build()
    DBContainer containerCIDVEN = queryCIDVEN.getContainer()

    containerCIDMAS.set("IDCONO", currentCompany)
    containerCIDMAS.set("IDSUNO", suno)
    if (queryCIDMAS.read(containerCIDMAS)) {
      String stat = (String)containerCIDMAS.get("IDSTAT")
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