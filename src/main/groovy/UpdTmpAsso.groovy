import java.time.format.DateTimeFormatter

/**
 * Name : EXT110MI.UpdTempAsso
 * description: Update assortment record in workfile EXT110
 * Only controls needed :
 * Record must exists in EXT105 for CONO, CUNO, TRDT, FILE
 * Record must not exists in EXT110
 *
 * Date         Changed By    Version   Description
 * 20250730     FLEBARS       1.0       Creation
 */
public class UpdTmpAsso extends ExtendM3Transaction {
  private final MIAPI mi
  private final DatabaseAPI database
  private final LoggerAPI logger
  private final ProgramAPI program
  private final UtilityAPI utility
  private final MICallerAPI miCaller

  private int currentCompany


  public UpdTmpAsso(MIAPI mi, DatabaseAPI database, LoggerAPI logger, ProgramAPI program, MICallerAPI miCaller, UtilityAPI utility) {
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
    int cdat = (Integer) (mi.in.get("CDAT") != null ? mi.in.get("CDAT") : 0)
    String stat = (String) (mi.in.get("STAT") != null ? mi.in.get("STAT") : "")
    String txer = (String) (mi.in.get("TXER") != null ? mi.in.get("TXER") : "")

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
    ext110Request.set("EXTRDT", trdt as Integer)
    ext110Request.set("EXFILE", file)
    ext110Request.set("EXASGD", asgd)
    ext110Request.set("EXCUNO", cuno)
    ext110Request.set("EXITNO", itno)
    ext110Request.set("EXCDAT", cdat)

    //Record exists
    if (!ext110Query.readLock(ext110Request, { LockedResult ext110Lockedresult ->
      if (!stat.isEmpty())
        ext110Lockedresult.set("EXSTAT", stat)
      if (!txer.isEmpty())
        ext110Lockedresult.set("EXTXER", txer)
      ext110Lockedresult.set("EXLMDT", utility.call("DateUtil", "currentDateY8AsInt"))
      ext110Lockedresult.setInt("EXCHNO", (ext110Lockedresult.get("EXCHNO") as Integer) + 1) // Keep existing CHNO
      ext110Lockedresult.set("EXCHID", program.getUser())
      ext110Lockedresult.update()
    })) {
      String chid = (String) ext110Request.get("EXCHID")
      mi.error("L'enregistrement n'existe pas")
      return
    }
  }
}
