/**
 * Name : EXT010MI.AddRefAsso
 * name: AddRefAsso
 * program: EXT010MI
 * description: Add assortment record in EXT010
 *
 *
 * Date         Changed By    Description
 * 20221122     FLEBARS       COMX01 - Creation
 * 20240228     FLEBARS       Gestion statuts 20-50
 */
public class AddRefAsso extends ExtendM3Transaction {
  private final MIAPI mi
  private final DatabaseAPI database
  private final LoggerAPI logger
  private final ProgramAPI program
  private final UtilityAPI utility

  private int currentCompany
  private String errorMessage
  private String fuds


  public AddRefAsso(MIAPI mi, DatabaseAPI database, LoggerAPI logger, ProgramAPI program, UtilityAPI utility) {
    this.mi = mi
    this.database = database
    this.logger = logger
    this.program = program
    this.utility=utility
  }

  /**
   * Get mi inputs
   * Check input values
   * Check if record not exists in EXT010
   * Serialize in EXT010
   */
  public void main() {
    currentCompany = (int)program.getLDAZD().CONO

    //Get mi inputs
    String asgd = (String)(mi.in.get("ASGD") != null ? mi.in.get("ASGD") : "")
    String cuno = (String)(mi.in.get("CUNO") != null ? mi.in.get("CUNO") : "")
    String itno = (String)(mi.in.get("ITNO") != null ? mi.in.get("ITNO") : "")
    double sapr = (Double)(mi.in.get("SAPR") != null ? mi.in.get("SAPR") : 0)
    int tvdt = (Integer)(mi.in.get("TVDT") != null ? mi.in.get("TVDT") : 0)
    String sule = (String)(mi.in.get("SULE") != null ? mi.in.get("SULE") : "")
    String suld = (String)(mi.in.get("SULD") != null ? mi.in.get("SULD") : "")
    int cdat = (Integer)(mi.in.get("CDAT") != null ? mi.in.get("CDAT") : 0)
    int rscl = (Integer)(mi.in.get("RSCL") != null ? mi.in.get("RSCL") : 0)
    int cmde = (Integer)(mi.in.get("CMDE") != null ? mi.in.get("CMDE") : 0)
    int fvdt = (Integer)(mi.in.get("FVDT") != null ? mi.in.get("FVDT") : 0)
    int lvdt = (Integer)(mi.in.get("LVDT") != null ? mi.in.get("LVDT") : 0)


    //Check inputs
    if (!checkCustomer(cuno)){
      mi.error(errorMessage)
      return
    }
    if (!checkItem(itno)){
      mi.error(errorMessage)
      return
    }
    if (sule.length() > 0 && suld.length() >0){
      mi.error("il faut renseigner soit le fournisseur entrepot soit le fournisseur direct")
      return
    }
    if (sule.length() > 0) {
      if (!checkSupplier(sule, "200")){
        mi.error(errorMessage)
        return
      }
    }
    if (suld.length() > 0) {
      if (!checkSupplier(suld, "100")){
        mi.error(errorMessage)
        return
      }
    }
    if (cmde != 1 && cmde != 2){
      mi.error("L'indicateur de commandabilité doit être égal à 1 ou 2")
      return
    }
    if (cdat != 0){
      boolean checkDate = (Boolean)utility.call("DateUtil", "isDateValid", "" + cdat, "yyyyMMdd")
      if (!checkDate){
        mi.error("Date début tarif ${cdat} est invalide")
        return
      }
    } else {
      cdat = (Integer)utility.call("DateUtil", "currentDateY8AsInt")
    }
    if (fvdt != 0){
      boolean checkDate = (Boolean)utility.call("DateUtil", "isDateValid", "" + fvdt, "yyyyMMdd")
      if (!checkDate){
        mi.error("Date début tarif ${fvdt} est invalide")
        return
      }
    }
    if (lvdt != 0){
      boolean checkDate = (Boolean)utility.call("DateUtil", "isDateValid", "" + lvdt, "yyyyMMdd")
      if (!checkDate){
        mi.error("Date début tarif ${lvdt} est invalide")
        return
      }
    }
    if (fvdt != 0 && lvdt != 0 && lvdt < fvdt){
      mi.error("Date de fin ${lvdt} doit être inférieure à date de début ${fvdt}")
      return
    }
    if (tvdt != 0){
      boolean checkDate = (Boolean)utility.call("DateUtil", "isDateValid", "" + tvdt, "yyyyMMdd")
      if (!checkDate){
        mi.error("Date début tarif ${tvdt} est invalide")
        return
      }
    }

    //Check if record exists
    DBAction queryEXT010 = database.table("EXT010")
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

    DBContainer containerEXT010 = queryEXT010.getContainer()
    containerEXT010.set("EXCONO", currentCompany)
    containerEXT010.set("EXASGD", asgd)
    containerEXT010.set("EXCUNO", cuno)
    containerEXT010.set("EXITNO", itno)
    containerEXT010.set("EXCDAT", cdat)

    //Record exists
    if (queryEXT010.read(containerEXT010)) {
      String chid = (String)containerEXT010.get("EXCHID")
      mi.error("L'enregistrement a été crée par l'utilisateur ${chid}")
      return
    }

    containerEXT010.set("EXCONO", currentCompany)
    containerEXT010.set("EXASGD", asgd)
    containerEXT010.set("EXCUNO", cuno)
    containerEXT010.set("EXITNO", itno)
    containerEXT010.set("EXCDAT", cdat)
    containerEXT010.set("EXSIG6", itno.substring(0, 6))
    containerEXT010.set("EXSAPR", sapr)
    containerEXT010.set("EXSULE", sule)
    containerEXT010.set("EXSULD", suld)
    containerEXT010.set("EXFUDS", fuds)
    containerEXT010.set("EXRSCL", rscl)
    containerEXT010.set("EXCMDE", cmde)
    containerEXT010.set("EXTVDT", tvdt)
    containerEXT010.set("EXFVDT", fvdt)
    containerEXT010.set("EXLVDT", lvdt)
    containerEXT010.set("EXRGDT", utility.call("DateUtil", "currentDateY8AsInt"))
    containerEXT010.set("EXRGTM", utility.call("DateUtil", "currentTimeAsInt"))
    containerEXT010.set("EXLMDT", utility.call("DateUtil", "currentDateY8AsInt"))
    containerEXT010.set("EXCHNO", 1)
    containerEXT010.set("EXCHID", program.getUser())
    queryEXT010.insert(containerEXT010)
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
      if (!stat.equals("20")){
        errorMessage = "Statut Client ${cuno} est invalide"
        return false
      }
      if (cutp != 0){
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
  private boolean checkItem(String itno){
    DBAction queryMITMAS = database.table("MITMAS").index("00").selection(
      "MMCONO", "MMITNO", "MMSTAT", "MMFUDS").build()

    DBContainer containerMITMAS = queryMITMAS.getContainer()
    containerMITMAS.set("MMCONO", currentCompany)
    containerMITMAS.set("MMITNO", itno)
    if (queryMITMAS.read(containerMITMAS)) {
      String stat = (String)containerMITMAS.get("MMSTAT")
      fuds = (String)containerMITMAS.get("MMFUDS")
      if (!(stat.compareTo("20") >= 0 && stat.compareTo("90") < 0)){//A°20240228
        //if (!stat.equals("20") && !stat.equals("50") ){//A°2024022
        // if (!stat.equals("20")){//D°20240228
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
  private boolean checkSupplier(String suno, String sucl){
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
        errorMessage = "Statut fournisseur ${suno} est invalide"
        return false
      }
    } else {
      errorMessage = "Fournisseur ${suno} n'existe pas"
      return false
    }

    String dbsucl = ""

    containerCIDVEN.set("IICONO", currentCompany)
    containerCIDVEN.set("IISUNO", suno)
    if (queryCIDVEN.read(containerCIDVEN)) {
      dbsucl = (String)containerCIDVEN.get("IISUCL")
    }
    if (!dbsucl.equals(sucl)){
      errorMessage = "Groupe fournisseur ${suno} est invalide"
      return false
    }

    return true
  }
}
