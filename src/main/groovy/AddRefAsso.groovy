/**
 * Name : EXT010MI.AddRefAsso
 * description: Add assortment record in EXT010
 * COMX01 Gestion des assortiments clients
 * Date         Changed By    Description
 * 20221122     FLEBARS       COMX01 - Creation
 * 20240228     FLEBARS       Gestion statuts 20-50
 * 20240620     FLEBARS       COMX01 - Controle code pour validation Infor
 * 20250114     YJANNIN       COMX01 - Historisation
 * 20250408     PBEAUDOUIN    COMX01 - Check to approval
 */

import java.time.LocalDateTime
import java.time.ZoneOffset

public class AddRefAsso extends ExtendM3Transaction {
  private final MIAPI mi
  private final DatabaseAPI database
  private final LoggerAPI logger
  private final ProgramAPI program
  private final UtilityAPI utility
  private final MICallerAPI miCaller

  private int currentCompany
  private String errorMessage
  private String fuds
  private double price
  private String retourPrice


  public AddRefAsso(MIAPI mi, DatabaseAPI database, LoggerAPI logger, ProgramAPI program, MICallerAPI miCaller, UtilityAPI utility) {
    this.mi = mi
    this.database = database
    this.logger = logger
    this.program = program
    this.miCaller = miCaller
    this.utility = utility
  }

  /**
   * Get mi inputs
   * Check input values
   * Check if record not exists in EXT010
   * Serialize in EXT010
   */
  public void main() {
    currentCompany = (int) program.getLDAZD().CONO

    String txtMessage = ""
    String fieldMessage = ""
    String codeMessage = ""

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


    //Check inputs
    if (!checkCustomer(cuno)) {
      mi.error(errorMessage)
      return
    }
    if (!checkItem(itno)) {
      mi.error(errorMessage)
      return
    }
    if (sule.length() > 0 && suld.length() > 0) {
      mi.error("il faut renseigner soit le fournisseur entrepot soit le fournisseur direct")
      return
    }
    // sule is a supplier with CIDMAS.SUCL = 200
    if (sule.length() > 0) {
      if (!checkSupplier(sule, "200")) {
        mi.error(errorMessage)
        return
      }
    }
    // suld is a supplier with CIDMAS.SUCL = 100
    if (suld.length() > 0) {
      if (!checkSupplier(suld, "100")) {
        mi.error(errorMessage)
        return
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
    } else {
      cdat = (Integer) utility.call("DateUtil", "currentDateY8AsInt")
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

    // Check double
    if (!checkDouble(cuno, itno, fvdt as String, lvdt as String)) {
      txtMessage = "Alerte, il existe un doublon pour client ${cuno}, article ${itno}, date de début ${fvdt} et date de fin ${lvdt}"
    }

    if (txtMessage == "") {
      if (sule.length() == 0 && suld.length() == 0) {
        txtMessage = "il faut renseigner Un fournisseur, Depot ou Entrepot pas les deux"
      }
    }
    if (txtMessage == "") {
      if (sule.length() > 0 && suld.length() > 0) {
        txtMessage = "il faut renseigner soit le fournisseur entrepot soit le fournisseur direct pas les deux"

      }
    }
    if (txtMessage == "") {
      if (sule.length() > 0) {
        if (!checkSupplier(sule, "200")) {
          txtMessage = errorMessage
        } else if (!checkSunoItno(sule, itno)) {
          txtMessage = "Alerte, problème filière d'approvisionnement PPS040 SULD"
        }

      } else if (!checkSupplier(suld, "100")) {
        txtMessage = errorMessage
      } else if (!checkSunoItno(suld, itno)) {
        txtMessage = "Alerte, problème filière d'approvisionnement PPS040 SULE"
      }

    }
    retourPrice = ""
    if (txtMessage == "") {
      price = 0
      if (sule.length() > 0) {
        retourPrice = executePPS106MIGetPrice(itno, sule)
      } else {
        retourPrice = executePPS106MIGetPrice(itno, suld)
      }
      if (retourPrice != "") {
        txtMessage = retourPrice
      } else if (price == 0) {
        txtMessage = "Alerte, problème filière d'approvisionnement PPS100"
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

    //Record exists
    if (ext010Query.read(ext010Request)) {
      String chid = (String) ext010Request.get("EXCHID")
      mi.error("L'enregistrement a été crée par l'utilisateur ${chid}")
      return
    }

    ext010Request.set("EXCONO", currentCompany)
    ext010Request.set("EXASGD", asgd)
    ext010Request.set("EXCUNO", cuno)
    ext010Request.set("EXITNO", itno)
    ext010Request.set("EXCDAT", cdat)
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

    if (txtMessage != "") {
      mi.error(txtMessage, fieldMessage, codeMessage)
    }
  }

  /**
   * Create EXT011 record
   * @parameter DBContainer, flag
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
      if (!stat.equals("20")) {
        errorMessage = "Statut Client ${cuno} est invalide"
        return false
      }
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

    DBContainer mitmasRequest = mitmasQuery.getContainer()
    mitmasRequest.set("MMCONO", currentCompany)
    mitmasRequest.set("MMITNO", itno)
    if (mitmasQuery.read(mitmasRequest)) {
      String stat = (String) mitmasRequest.get("MMSTAT")
      fuds = (String) mitmasRequest.get("MMFUDS")
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

    String dbsucl = ""

    cidvenRequest.set("IICONO", currentCompany)
    cidvenRequest.set("IISUNO", suno)
    if (cidvenQuery.read(cidvenRequest)) {
      dbsucl = (String) cidvenRequest.get("IISUCL")
    }
    if (!dbsucl.equals(sucl)) {
      errorMessage = "Groupe fournisseur ${suno} est invalide"
      return false
    }

    return true
  }

  /**
   * Read information from DB EXT010
   * Double check
   *
   * @parameter Customer, Item, From date, To date
   * @return true if ok false otherwise
   * */
  private boolean checkDouble(String cuno, String itno, String fvdt, String lvdt) {
    boolean errorIndicator = false
    DBAction ext010Query = database.table("EXT010")
      .index("02")
      .selection("EXFVDT", "EXLVDT")
      .build()
    DBContainer ext010Request = ext010Query.getContainer()
    ext010Request.set("EXCONO", currentCompany)
    ext010Request.set("EXCUNO", cuno)
    ext010Request.set("EXITNO", itno)

    int ifvdt = fvdt as Integer
    int ilvdt = lvdt as Integer

    Closure<?> outEXT0101 = { DBContainer EXT0101result ->
      int tfvdt = EXT0101result.get("EXFVDT") as Integer
      int tlvdt = EXT0101result.get("EXLVDT") as Integer
      if (!checkDates(ifvdt, ilvdt, tfvdt, tlvdt)) {
        errorIndicator = true
        Closure<?> ext010Updater = { LockedResult ext010LockedRecord ->
          ext010LockedRecord.set("EXCMDE", 99)
          ext010LockedRecord.update()
        }
        ext010Query.readLock(EXT0101result, ext010Updater)
      }
    }
    ext010Query.readAll(ext010Request, 3, 10000, outEXT0101)
    return !errorIndicator
  }
  /**
   * Check overlap dates
   * @param fvdt1
   * @param lvdt1
   * @param fvdt2
   * @param lvdt2
   * @return
   */
  private boolean checkDates(int fvdt1, int lvdt1, int fvdt2, int lvdt2) {
    if (fvdt1 == fvdt2 && lvdt1 <= lvdt2) {
      return true
    }
    if (fvdt1 == fvdt2 && lvdt1 == lvdt2)
      return true

    if (lvdt1 < fvdt1 || lvdt2 < fvdt2)
      return false

    if (lvdt1 < fvdt2 || lvdt1 < fvdt2)
      return false

    return true
  }

  /**
   * Read information from DB EXT010
   * chexk MITVEN Exist by suno, itno
   *
   * @parameter SUNO suppliEr, ITNO Item
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
   * @parameter Item ITNO, supplier SUNO
   * @return price PUPR
   * */
  private executePPS106MIGetPrice(String ITNO, String SUNO) {
    Map<String, String> parameters = ["ITNO": ITNO, "SUNO": SUNO, "ORQA": "1"]
    Closure<?> handler = { Map<String, String> response ->
      if (response.error != null) {
        return response.error
      }
      if (response.PUPR != null) {
        price = response.PUPR as double
      }
    }
    miCaller.call("PPS106MI", "GetPrice", parameters, handler)
  }
}
