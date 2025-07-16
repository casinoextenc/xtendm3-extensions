/**
 * Name : EXT010MI.UpdRefAsso
 * COMX01 Gestion des assortiments clients
 * Description :
 * This API method to update records in specific table EXT010 Customer Assortment
 * This API works in mode Add or Upd if does'nt exists
 * Date         Changed By    Description
 * 20221122     FLEBARS       COMX01 - Creation
 * 20240228     FLEBARS       Gestion statuts 20-50
 * 20240620     FLEBARS       COMX01 - Controle code pour validation Infor
 * 20250114     YJANNIN       COMX01 - Historisation
 */

import java.time.LocalDateTime
import java.time.ZoneOffset

public class UpdRefAsso extends ExtendM3Transaction {
  private final MIAPI mi
  private final DatabaseAPI database
  private final LoggerAPI logger
  private final ProgramAPI program
  private final UtilityAPI utility
  private final MICallerAPI miCaller
  private int currentCompany
  private String errorMessage
  private String txtMessage
  private String fuds
  private double price
  private String retourPrice

  public UpdRefAsso(MIAPI mi, DatabaseAPI database, LoggerAPI logger, ProgramAPI program, MICallerAPI miCaller, UtilityAPI utility) {
    this.mi = mi
    this.database = database
    this.logger = logger
    this.program = program
    this.utility = utility
    this.miCaller = miCaller
  }

  /**
   * Get mi inputs
   * Check input values
   * Check if record not exists in EXT010
   * Serialize in EXT010
   */
  public void main() {
    currentCompany = (int) program.getLDAZD().CONO

    //Get mi inputs
    String asgd = (String) (mi.in.get("ASGD") != null ? mi.in.get("ASGD") : "")
    String cuno = (String) (mi.in.get("CUNO") != null ? mi.in.get("CUNO") : "")
    String itno = (String) (mi.in.get("ITNO") != null ? mi.in.get("ITNO") : "")
    double sapr = (Double) (mi.in.get("SAPR") != null ? mi.in.get("SAPR") : 0)
    int tvdt = (Integer) (mi.in.get("TVDT") != null ? mi.in.get("TVDT") : 0)
    String sule = (String) (mi.in.get("SULE") != null ? mi.in.get("SULE") : "")
    String suld = (String) (mi.in.get("SULD") != null ? mi.in.get("SULD") : "")
    int cdat = (Integer) (mi.in.get("CDAT") != null ? mi.in.get("CDAT") : 0)
    int rscl = (Integer) (mi.in.get("RSCL") != null ? mi.in.get("RSCL") : 0)
    int cmde = (Integer) (mi.in.get("CMDE") != null ? mi.in.get("CMDE") : 0)
    int fvdt = (Integer) (mi.in.get("FVDT") != null ? mi.in.get("FVDT") : 0)
    int lvdt = (Integer) (mi.in.get("LVDT") != null ? mi.in.get("LVDT") : 0)

    txtMessage =""
    //Check inputs
    if (!checkCustomer(cuno)) {
      mi.error(errorMessage)
      return
    }
    if (!checkItem(itno)) {
      mi.error(errorMessage)
      return
    }

    if (txtMessage == "" && sule.length() > 0 && suld.length() > 0) {
      txtMessage = "il faut renseigner soit le fournisseur entrepot soit le fournisseur direct pas les deux"
      logger.debug("#PB "+txtMessage)
    }
    if (txtMessage == "" && sule.length() == 0 && suld.length() == 0) {
      txtMessage = "il faut renseigner au moins le fournisseur entrepot ou le fournisseur direct"
      logger.debug("#PB "+txtMessage)
    }
    if (txtMessage == "") {
      if (sule.length() > 0) {
        if (!checkSupplier(sule, "200")) {
          txtMessage = errorMessage
          logger.debug("#PB "+txtMessage)
        } else if (!checkSunoItno(sule, itno)) {
          txtMessage = "Alerte problème filière d'approvisionnement PPS040 SULE : "+sule
          logger.debug("#PB "+txtMessage)
        }
      } else if (suld.length() > 0) {
        if (!checkSupplier(suld, "100")) {
          txtMessage = errorMessage
          logger.debug("#PB "+txtMessage)
        } else if (!checkSunoItno(suld, itno)) {
          txtMessage = "Alerte problème filière d'approvisionnement PPS040 SULD : "+suld
          logger.debug("#PB "+txtMessage)
        }
      }
    }


    retourPrice = ""
    if (txtMessage == "") {
      price = 0
      if (sule.length() > 0) {
        executePPS106MIGetPrice(itno, sule)
      } else {
        executePPS106MIGetPrice(itno, suld)
      }
      if (retourPrice != "") {
        txtMessage = retourPrice
        logger.debug("#PB "+txtMessage)
      } else if (price == 0.0100) {
        txtMessage = "Alerte problème filière d'approvisionnement PPS100"
        logger.debug("#PB "+txtMessage)
      }
    }


    if (cmde != 1 && cmde != 2) {
      mi.error("L'indicateur de commandabilité doit être égal à 1 ou 2")
      return

    }
    if (cdat != 0) {
      boolean checkDate = (Boolean) utility.call("DateUtil", "isDateValid", "" + cdat, "yyyyMMdd")
      if (!checkDate) {
        mi.error("Date début tarif ${cdat} est invalide")
        return
      }
    }
    if (fvdt != 0) {
      boolean checkDate = (Boolean) utility.call("DateUtil", "isDateValid", "" + fvdt, "yyyyMMdd")
      if (!checkDate) {
        mi.error("Date début tarif ${fvdt} est invalide")
        return
      }
    }
    if (lvdt != 0) {
      boolean checkDate = (Boolean) utility.call("DateUtil", "isDateValid", "" + lvdt, "yyyyMMdd")
      if (!checkDate) {
        mi.error("Date début tarif ${lvdt} est invalide")
        return
      }
    }
    if (fvdt != 0 && lvdt != 0 && lvdt < fvdt) {
      mi.error("Date de fin ${lvdt} doit être inférieure à date de début ${fvdt}")
      return

    }
    if (tvdt != 0) {
      boolean checkDate = (Boolean) utility.call("DateUtil", "isDateValid", "" + tvdt, "yyyyMMdd")
      if (!checkDate) {
        mi.error("Date début tarif ${tvdt} est invalide")
        return
      }
    }

    //Check if record exists
    DBAction ext010Query = database.table("EXT010")
      .index("00")
      .selection(
        "EXCONO",
        "EXASGD",
        "EXCUNO",
        "EXITNO",
        "EXCDAT",
        "EXSIG6",
        "EXSAPR",
        "EXSULE",
        "EXSULD",
        "EXFUDS",
        "EXRSCL",
        "EXCMDE",
        "EXFVDT",
        "EXLVDT",
        "EXTVDT",
        "EXRGDT",
        "EXRGTM",
        "EXLMDT",
        "EXCHNO",
        "EXCHID"
      )
      .build()

    DBContainer ext010Request = ext010Query.getContainer()
    ext010Request.set("EXCONO", currentCompany)
    ext010Request.set("EXASGD", asgd)
    ext010Request.set("EXCUNO", cuno)
    ext010Request.set("EXITNO", itno)
    ext010Request.set("EXCDAT", cdat)

    // if Record not exists then create
    if (!ext010Query.read(ext010Request)) {
      ext010Request.set("EXSIG6", itno.substring(0, 6))
      ext010Request.set("EXSAPR", sapr)
      ext010Request.set("EXSULE", sule)
      ext010Request.set("EXSULD", suld)
      ext010Request.set("EXFUDS", fuds)
      ext010Request.set("EXRSCL", rscl)
      ext010Request.set("EXCMDE", cmde)
      ext010Request.set("EXTVDT", tvdt)
      ext010Request.set("EXFVDT", fvdt)
      ext010Request.set("EXLVDT", lvdt)
      ext010Request.set("EXRGDT", utility.call("DateUtil", "currentDateY8AsInt"))
      ext010Request.set("EXRGTM", utility.call("DateUtil", "currentTimeAsInt"))
      ext010Request.set("EXLMDT", utility.call("DateUtil", "currentDateY8AsInt"))
      ext010Request.set("EXCHNO", 1)
      ext010Request.set("EXCHID", program.getUser())
      createEXT011Record(ext010Request, "C")
      ext010Query.insert(ext010Request)
      return
    }

    double saprDb = ext010Request.get("EXSAPR") as double
    int rsclDb = ext010Request.get("EXRSCL") as int
    int cmdeDb = ext010Request.get("EXCMDE") as int
    int tvdtDb = ext010Request.get("EXTVDT") as int
    int fvdtDb = ext010Request.get("EXFVDT") as int
    int lvdtDb = ext010Request.get("EXLVDT") as int

    //Check inputs for update
    boolean flagUpdate = false
    flagUpdate = flagUpdate || (sapr != saprDb)
    flagUpdate = flagUpdate || !mi.in.get("SULE").equals(ext010Request.getString("EXSULE").trim())
    flagUpdate = flagUpdate || !mi.in.get("SULD").equals(ext010Request.getString("EXSULD").trim())
    flagUpdate = flagUpdate || (rscl != rsclDb)
    flagUpdate = flagUpdate || (cmde != cmdeDb)
    flagUpdate = flagUpdate || (tvdt != tvdtDb)
    flagUpdate = flagUpdate || (fvdt != fvdtDb)
    flagUpdate = flagUpdate || (lvdt != lvdtDb)


    // else if Record  exists then Update
    Closure<?> ext010Updater = { LockedResult ext010LockedResult ->
      if (mi.in.get("SAPR") != null)
        ext010LockedResult.set("EXSAPR", sapr)
      if (mi.in.get("SULE") != null)
        ext010LockedResult.set("EXSULE", sule)
      if (mi.in.get("SULD") != null)
        ext010LockedResult.set("EXSULD", suld)
      if (mi.in.get("RSCL") != null)
        ext010LockedResult.set("EXRSCL", rscl)
      if (mi.in.get("CMDE") != null)
        ext010LockedResult.set("EXCMDE", cmde)
      if (mi.in.get("TVDT") != null)
        ext010LockedResult.set("EXTVDT", tvdt)
      if (mi.in.get("FVDT") != null)
        ext010LockedResult.set("EXFVDT", fvdt)
      if (mi.in.get("LVDT") != null)
        ext010LockedResult.set("EXLVDT", lvdt)

      ext010LockedResult.set("EXLMDT", utility.call("DateUtil", "currentDateY8AsInt"))
      ext010LockedResult.set("EXCHNO", ((Integer) ext010LockedResult.get("EXCHNO") + 1))
      ext010LockedResult.set("EXCHID", program.getUser())
      createEXT011Record(ext010LockedResult, "U")
      ext010LockedResult.update()
    }
    if (flagUpdate) ext010Query.readLock(ext010Request, ext010Updater)
    if (txtMessage != "") {
      mi.error(txtMessage)
    }
  }

  /**
   * Create EXT011 record
   * @parameter DBContainer,flag
   * */
  private void createEXT011Record(DBContainer ext010Request, String flag) {
    DBAction ext011Query = database.table("EXT011")
      .index("00")
      .selection(
        "EXCONO",
        "EXASGD",
        "EXCUNO",
        "EXITNO",
        "EXCDAT",
        "EXRGDT",
        "EXRGTM",
        "EXCHNO",
        "EXCHID"
      )
      .build()

    DBContainer ext011Request = ext011Query.getContainer()
    ext011Request.set("EXCONO", currentCompany)
    ext011Request.set("EXASGD", ext010Request.get("EXASGD"))
    ext011Request.set("EXCUNO", ext010Request.get("EXCUNO"))
    ext011Request.set("EXITNO", ext010Request.get("EXITNO"))
    ext011Request.set("EXSIG6", ext010Request.get("EXSIG6"))
    ext011Request.set("EXSAPR", ext010Request.get("EXSAPR"))
    ext011Request.set("EXSULE", ext010Request.get("EXSULE"))
    ext011Request.set("EXSULD", ext010Request.get("EXSULD"))
    ext011Request.set("EXFUDS", ext010Request.get("EXFUDS"))
    ext011Request.set("EXCDAT", ext010Request.get("EXCDAT"))
    ext011Request.set("EXRSCL", ext010Request.get("EXRSCL"))
    ext011Request.set("EXCMDE", ext010Request.get("EXCMDE"))
    ext011Request.set("EXFVDT", ext010Request.get("EXFVDT"))
    ext011Request.set("EXLVDT", ext010Request.get("EXLVDT"))
    ext011Request.set("EXTVDT", ext010Request.get("EXTVDT"))
    ext011Request.set("EXRGDT", ext010Request.get("EXRGDT"))
    ext011Request.set("EXRGTM", ext010Request.get("EXRGTM"))
    ext011Request.set("EXCHNO", ext010Request.get("EXCHNO"))
    ext011Request.set("EXCHID", ext010Request.get("EXCHID"))
    ext011Request.set("EXFLAG", flag)
    ext011Query.insert(ext011Request)
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
  private boolean checkCustomer(String cuno) {
    DBAction ocusmaQuery = database.table("OCUSMA").index("00").selection(
      "OKCONO", "OKCUNO", "OKSTAT", "OKCUTP").build()

    DBContainer ocusmaRequest = ocusmaQuery.getContainer()
    ocusmaRequest.set("OKCONO", currentCompany)
    ocusmaRequest.set("OKCUNO", cuno)
    if (ocusmaQuery.read(ocusmaRequest)) {
      String stat = (String) ocusmaRequest.get("OKSTAT")
      int cutp = (Integer) ocusmaRequest.get("OKCUTP")

      if (cutp != 0) {
        errorMessage = "Type Client ${cuno} est invalide"
        return false
      }
    } else {
      errorMessage = "Client ${cuno} n'existe pas"
      return false
    }
    return true
  }

  /**
   * Read item information from DB MITMAS
   * Item must exists
   * Status must be 20
   *
   * @parameter Item
   * @return true if ok false otherwise
   * */
  private boolean checkItem(String itno) {
    DBAction mitmasQuery = database.table("MITMAS").index("00").selection(
      "MMCONO", "MMITNO", "MMSTAT", "MMFUDS").build()

    DBContainer mitmasContianer = mitmasQuery.getContainer()
    mitmasContianer.set("MMCONO", currentCompany)
    mitmasContianer.set("MMITNO", itno)
    if (mitmasQuery.read(mitmasContianer)) {
      String stat = (String) mitmasContianer.get("MMSTAT")
      fuds = (String) mitmasContianer.get("MMFUDS")
      if (!(stat.compareTo("20") >= 0 && stat.compareTo("90") < 0)) {
        errorMessage = "Statut Article ${itno} est invalide"
        return false
      }
    } else {
      errorMessage = "Article ${itno} n'existe pas"
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
  private boolean checkSupplier(String suno, String sucl) {
    DBAction cidmasQuery = database.table("CIDMAS").index("00").selection(
      "IDCONO", "IDSUNO", "IDSTAT").build()
    DBContainer cidmasRequest = cidmasQuery.getContainer()

    DBAction cidvenQuery = database.table("CIDVEN").index("00").selection(
      "IICONO", "IISUNO", "IISUCL").build()
    DBContainer cidvenRequest = cidvenQuery.getContainer()

    cidmasRequest.set("IDCONO", currentCompany)
    cidmasRequest.set("IDSUNO", suno)
    if (cidmasQuery.read(cidmasRequest)) {
      String stat = (String) cidmasRequest.get("IDSTAT")
      if (!stat.equals("20")) {
        errorMessage = "Statut fournisseur ${suno} est invalide"
        return false
      }
    } else {
      errorMessage = "Fournisseur ${suno} n'existe pas"
      return false
    }

    String dbSucl = ""

    cidvenRequest.set("IICONO", currentCompany)
    cidvenRequest.set("IISUNO", suno)
    if (cidvenQuery.read(cidvenRequest)) {
      dbSucl = (String) cidvenRequest.get("IISUCL")
    }
    if (!dbSucl.equals(sucl)) {
      errorMessage = "Groupe fournisseur ${suno} est invalide"
      return false
    }
    return true
  }

  /**
   * Read information from DB EXT010
   * Double check
   *
   * @parameter Supplier
   * @parameter Item
   * @return true if ok false otherwise
   * */
  private boolean checkSunoItno(String suno, String itno) {
    boolean found = false
    DBAction mitvenQuery = database.table("MITVEN")
      .index("10")
      .selection("IFSITE")
      .build()
    DBContainer containerMitven = mitvenQuery.getContainer()
    containerMitven.set("IFCONO", currentCompany)
    containerMitven.set("IFSUNO", suno)
    containerMitven.set("IFITNO", itno)
    Closure<?> outMitven = { DBContainer mitvenResult ->
      found = true
    }
    if (!mitvenQuery.readAll(containerMitven, 3, 1, outMitven)) {
    }
    if (found) {
      return true
    }
    return false
  }

  /**
   * Execute PPS106MI.GetPrice
   *
   * @parameter Item, supplier
   * @return price
   * */
  private executePPS106MIGetPrice(String ITNO, String SUNO) {
    Map<String, String> parameters = ["ITNO": ITNO, "SUNO": SUNO, "ORQA": "1"]
    Closure<?> handler = { Map<String, String> response ->
      if (response.error != null) {
        retourPrice = response.error
      } else if (response.PUPR != null) {
        price = response.PUPR as double
        logger.debug("#PB Price = "+String.valueOf(price))
      }
    }
    miCaller.call("PPS106MI", "GetPrice", parameters, handler)
  }
}
