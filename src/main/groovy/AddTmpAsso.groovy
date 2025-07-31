/**
 * Name : EXT110MI.AddTempAsso
 * description: Add assortment record in workfile EXT110
 * Only controls needed :
 * Record must exists in EXT105 for CONO, CUNO, TRDT, FILE
 * Record must not exists in EXT110
 *
 * Date         Changed By    Version   Description
 * 20250730     FLEBARS       1.0       Creation
 */
public class AddTmpAsso extends ExtendM3Transaction {
  private final MIAPI mi
  private final DatabaseAPI database
  private final LoggerAPI logger
  private final ProgramAPI program
  private final UtilityAPI utility
  private final MICallerAPI miCaller

  private int currentCompany


  public AddTmpAsso(MIAPI mi, DatabaseAPI database, LoggerAPI logger, ProgramAPI program, MICallerAPI miCaller, UtilityAPI utility) {
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
   * Check if record not exists in EXT110
   * Serialize in EXT110
   */
  public void main() {
    currentCompany = (int) program.getLDAZD().CONO

    //Get mi inputs
    String trdt = (String) (mi.in.get("TRDT") != null ? mi.in.get("TRDT") : "")
    String file = (String) (mi.in.get("FILE") != null ? mi.in.get("FILE") : "")
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

    //Check if record exists in EXT105
    DBAction ext105Query = database.table("EXT105").index("00").build()
    DBContainer ext105Request = ext105Query.getContainer()
    ext105Request.set("EXCONO", currentCompany)
    ext105Request.set("EXCUNO", cuno)
    ext105Request.setInt("EXTRDT", trdt as Integer)
    ext105Request.set("EXFILE", file)
    if (!ext105Query.read(ext105Request)) {
      mi.error("L'enregistrement EXT105 n'existe pas client:${cuno} date:${trdt} file:${file}")
      return
    }

    //Check if record exists
    DBAction ext110Query = database.table("EXT110")
      .index("00")
      .selection(
        "EXCONO",
        "EXTRDT",
        "EXFILE",
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

    DBContainer ext110Request = ext110Query.getContainer()
    ext110Request.set("EXCONO", currentCompany)
    ext110Request.set("EXTRDT", trdt)
    ext110Request.set("EXFILE", file)
    ext110Request.set("EXASGD", asgd)
    ext110Request.set("EXCUNO", cuno)
    ext110Request.set("EXITNO", itno)
    ext110Request.set("EXCDAT", cdat)

    //Record exists
    if (ext110Query.read(ext110Request)) {
      String chid = (String) ext110Request.get("EXCHID")
      mi.error("L'enregistrement a été crée par l'utilisateur ${chid}")
      return
    }

    ext110Request.set("EXCONO", currentCompany)
    ext110Request.set("EXTRDT", trdt)
    ext110Request.set("EXFILE", file)
    ext110Request.set("EXASGD", asgd)
    ext110Request.set("EXCUNO", cuno)
    ext110Request.set("EXITNO", itno)
    ext110Request.set("EXCDAT", cdat)
    ext110Request.set("EXSIG6", itno.substring(0, 6))
    ext110Request.set("EXSAPR", sapr)
    ext110Request.set("EXSULE", sule)
    ext110Request.set("EXSULD", suld)
    ext110Request.set("EXFUDS", "")
    ext110Request.set("EXRSCL", rscl)
    ext110Request.set("EXCMDE", cmde)
    ext110Request.set("EXTVDT", tvdt)
    ext110Request.set("EXFVDT", fvdt)
    ext110Request.set("EXLVDT", lvdt)
    ext110Request.set("EXRGDT", utility.call("DateUtil", "currentDateY8AsInt"))
    ext110Request.set("EXRGTM", utility.call("DateUtil", "currentTimeAsInt"))
    ext110Request.set("EXLMDT", utility.call("DateUtil", "currentDateY8AsInt"))
    ext110Request.set("EXCHNO", 1)
    ext110Request.set("EXCHID", program.getUser())
    ext110Query.insert(ext110Request)
  }
}
