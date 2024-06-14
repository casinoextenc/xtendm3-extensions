/**
 * README
 * This extension is used by Mashup
 * QUAX01 Gestion du référentiel qualité
 * Name : EXT035MI.UpdDocumentCode
 * Description : Add records to the EXT035 table.
 * Date         Changed By   Description
 * 20230201     SEAR         QUAX01 - Constraints matrix
 * 20240605     FLEBARS      QUAX01 - Controle code pour validation Infor
 */
public class UpdDocumentCode extends ExtendM3Transaction {
  private final MIAPI mi
  private final LoggerAPI logger
  private final ProgramAPI program
  private final DatabaseAPI database
  private final UtilityAPI utility

  private int currentCompany
  private String ads1 = ""

  public UpdDocumentCode(MIAPI mi, DatabaseAPI database, ProgramAPI program, UtilityAPI utility, LoggerAPI logger) {
    this.mi = mi
    this.database = database
    this.program = program
    this.utility = utility
    this.logger = logger
  }

  public void main() {
    if (mi.in.get("CONO") == null) {
      currentCompany = (Integer) program.getLDAZD().CONO
    } else {
      currentCompany = mi.in.get("CONO")
    }

    //Check if record exists in Constraint Code Table (EXT034)
    if (mi.in.get("ZCOD") != null) {
      DBAction ext034Query = database.table("EXT034").index("00").build()
      DBContainer ext034Request = ext034Query.getContainer()
      ext034Request.set("EXCONO", currentCompany)
      ext034Request.set("EXZCOD", mi.in.get("ZCOD"))
      if (!ext034Query.read(ext034Request)) {
        mi.error("Code contrainte " + mi.in.get("ZCOD") + " n'existe pas")
        return
      }
    }

    //Check if record exists in country Code Table (CSYTAB)
    if (mi.in.get("CSCD") != null) {
      DBAction csytabQuery = database.table("CSYTAB").index("00").build()
      DBContainer csytabRequest = csytabQuery.getContainer()
      csytabRequest.set("CTCONO", currentCompany)
      csytabRequest.set("CTSTCO", "CSCD")
      csytabRequest.set("CTSTKY", mi.in.get("CSCD"))
      if (!csytabQuery.read(csytabRequest)) {
        mi.error("Code pays " + mi.in.get("CSCD") + " n'existe pas")
        return
      }
    }

    //Check if record Cutomer in Customer Table (OCUSMA)
    if (mi.in.get("CUNO") != null) {
      DBAction ocusmaQuery = database.table("OCUSMA").index("00").build()
      DBContainer ocusmaRequest = ocusmaQuery.getContainer()
      ocusmaRequest.set("OKCONO", currentCompany)
      ocusmaRequest.set("OKCUNO", mi.in.get("CUNO"))
      if (!ocusmaQuery.read(ocusmaRequest)) {
        mi.error("Code client " + mi.in.get("CUNO") + " n'existe pas")
        return
      }
    }

    //Check if record exists in Document Code Table (MPDDOC)
    if (mi.in.get("DOID") != null) {
      DBAction mpddocQuery = database.table("MPDDOC").index("00").selection("DOADS1").build()
      DBContainer mpddocRequest = mpddocQuery.getContainer()
      mpddocRequest.set("DOCONO", currentCompany)
      mpddocRequest.set("DODOID", mi.in.get("DOID"))
      if (!mpddocQuery.read(mpddocRequest)) {
        mi.error("Code Document " + mi.in.get("DOID") + " n'existe pas")
        return
      }
      ads1 = (String) mpddocRequest.get("DOADS1")
    }

    //document type from input
    if (mi.in.get("ADS1") != null) {
      ads1 = mi.in.get("ADS1")
    }

    //Check if record exists
    DBAction ext035Query = database.table("EXT035")
      .index("00")
      .selection(
        "EXCONO",
        "EXZCOD",
        "EXCSCD",
        "EXCUNO",
        "EXDOID",
        "EXADS1",
        "EXRGDT",
        "EXRGTM",
        "EXLMDT",
        "EXCHNO",
        "EXCHID"
      )
      .build()

    DBContainer ext035Request = ext035Query.getContainer()
    ext035Request.set("EXCONO", currentCompany)
    ext035Request.set("EXZCOD", mi.in.get("ZCOD"))
    ext035Request.set("EXCSCD", mi.in.get("CSCD"))
    ext035Request.set("EXCUNO", mi.in.get("CUNO"))
    ext035Request.set("EXDOID", mi.in.get("DOID"))

    Closure<?> ext035Updater = { LockedResult ext035LockedResultEXT035 ->
      if (mi.in.get("CSCD") != null)
        ext035LockedResultEXT035.set("EXCSCD", mi.in.get("CSCD"))
      if (mi.in.get("CUNO") != null)
        ext035LockedResultEXT035.set("EXCUNO", mi.in.get("CUNO"))
      if (mi.in.get("DOID") != null)
        ext035LockedResultEXT035.set("EXDOID", mi.in.get("DOID"))
      if (mi.in.get("ADS1") != null)
        ext035LockedResultEXT035.set("EXADS1", ads1)
      ext035LockedResultEXT035.set("EXLMDT", utility.call("DateUtil", "currentDateY8AsInt"))
      ext035LockedResultEXT035.set("EXCHNO", ((Integer) ext035LockedResultEXT035.get("EXCHNO") + 1))
      ext035LockedResultEXT035.set("EXCHID", program.getUser())
      ext035LockedResultEXT035.update()
    }

    if (!ext035Query.readLock(ext035Request, ext035Updater)) {
      mi.error("L'enregistrement n'existe pas")
      return
    }
  }
}
